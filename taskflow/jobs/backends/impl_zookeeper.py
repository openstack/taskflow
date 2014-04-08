# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import contextlib
import functools
import logging

from kazoo import exceptions as k_exceptions
from kazoo.protocol import paths as k_paths
from kazoo.recipe import watchers
import six

from taskflow import exceptions as excp
from taskflow.jobs import job as base_job
from taskflow.jobs import jobboard
from taskflow.openstack.common import excutils
from taskflow.openstack.common import jsonutils
from taskflow.openstack.common import uuidutils
from taskflow.persistence import logbook
from taskflow import states
from taskflow.utils import kazoo_utils
from taskflow.utils import lock_utils
from taskflow.utils import misc

LOG = logging.getLogger(__name__)

UNCLAIMED_JOB_STATES = (
    states.UNCLAIMED,
)
ALL_JOB_STATES = (
    states.UNCLAIMED,
    states.COMPLETE,
    states.CLAIMED,
)

# Transaction support was added in 3.4.0
MIN_ZK_VERSION = (3, 4, 0)


def _check_who(who):
    if not isinstance(who, six.string_types):
        raise TypeError("Job applicant must be a string type")
    if len(who) == 0:
        raise ValueError("Job applicant must be non-empty")


class ZookeeperJob(base_job.Job):
    def __init__(self, name, board, client, backend, path,
                 uuid=None, details=None, book=None, book_data=None):
        super(ZookeeperJob, self).__init__(name, uuid=uuid, details=details)
        self._board = board
        self._book = book
        if not book_data:
            book_data = {}
        self._book_data = book_data
        self._client = client
        self._backend = backend
        if all((self._book, self._book_data)):
            raise ValueError("Only one of 'book_data' or 'book'"
                             " can be provided")
        self._path = path
        self._lock_path = "%s.lock" % (path)

    @property
    def lock_path(self):
        return self._lock_path

    @property
    def path(self):
        return self._path

    @property
    def board(self):
        return self._board

    def _load_book(self, book_uuid, book_name):
        # No backend to attempt to fetch from :-(
        if self._backend is None:
            return logbook.LogBook(name=book_name, uuid=book_uuid)
        # TODO(harlowja): we are currently limited by assuming that the job
        # posted has the same backend as this loader (to start this seems to
        # be a ok assumption, and can be adjusted in the future if we determine
        # there is a use-case for multi-backend loaders, aka a registry of
        # loaders).
        with contextlib.closing(self._backend.get_connection()) as conn:
            return conn.get_logbook(book_uuid)

    @property
    def state(self):
        owner = self.board.find_owner(self)
        job_data = {}
        try:
            raw_data, _data_stat = self._client.get(self.path)
            job_data = misc.decode_json(raw_data)
        except k_exceptions.NoNodeError:
            pass
        except k_exceptions.SessionExpiredError as e:
            raise excp.JobFailure("Can not fetch the state of %s,"
                                  " session expired" % (self.uuid), e)
        except self._client.handler.timeout_exception as e:
            raise excp.JobFailure("Can not fetch the state of %s,"
                                  " connection timed out" % (self.uuid), e)
        except k_exceptions.KazooException as e:
            raise excp.JobFailure("Can not fetch the state of %s, internal"
                                  " error" % (self.uuid), e)
        if not job_data:
            # No data this job has been completed (the owner that we might have
            # fetched will not be able to be fetched again, since the job node
            # is a parent node of the owner/lock node).
            return states.COMPLETE
        if not owner:
            # No owner, but data, still work to be done.
            return states.UNCLAIMED
        return states.CLAIMED

    def __cmp__(self, other):
        return cmp(self.path, other.path)

    def __hash__(self):
        return hash(self.path)

    @property
    def book(self):
        if self._book is None:
            loaded_book = None
            try:
                book_uuid = self._book_data['uuid']
                book_name = self._book_data['name']
                loaded_book = self._load_book(book_uuid, book_name)
            except (KeyError, TypeError):
                pass
            self._book = loaded_book
        return self._book


class ZookeeperJobBoard(jobboard.JobBoard):
    def __init__(self, name, conf, client=None, persistence=None):
        super(ZookeeperJobBoard, self).__init__(name, conf)
        if client is not None:
            self._client = client
            self._owned = False
        else:
            self._client = kazoo_utils.make_client(self._conf)
            self._owned = True
        path = str(conf.get("path", "/taskflow/jobs"))
        if not path:
            raise ValueError("Empty zookeeper path is disallowed")
        if not k_paths.isabs(path):
            raise ValueError("Zookeeper path must be absolute")
        self._path = path
        # The backend to load the full logbooks from, since whats sent over
        # the zookeeper data connection is only the logbook uuid and name, and
        # not currently the full logbook (later when a zookeeper backend
        # appears we can likely optimize for that backend usage by directly
        # reading from the path where the data is stored, if we want).
        self._persistence = persistence
        # Misc. internal details
        self._known_jobs = {}
        self._job_mutate = self._client.handler.rlock_object()
        self._open_close_lock = self._client.handler.rlock_object()
        self._client.add_listener(self._state_change_listener)
        self._bad_paths = frozenset([path])
        self._job_watcher = None
        # Since we use sequenced ids this will be the path that the sequences
        # are prefixed with, for example, job0000000001, job0000000002, ...
        self._job_base = k_paths.join(path, "job")

    @property
    def path(self):
        return self._path

    @property
    def job_count(self):
        with self._job_mutate:
            return len(self._known_jobs)

    def _force_refresh(self, delayed=False):
        try:
            children = self._client.get_children(self.path)
        except self._client.handler.timeout_exception as e:
            raise excp.JobFailure("Refreshing failure, connection timed out",
                                  e)
        except k_exceptions.SessionExpiredError as e:
            raise excp.JobFailure("Refreshing failure, session expired", e)
        except k_exceptions.NoNodeError:
            pass
        except k_exceptions.KazooException as e:
            raise excp.JobFailure("Refreshing failure, internal error", e)
        else:
            self._on_job_posting(children, delayed=delayed)

    def iterjobs(self, only_unclaimed=False, ensure_fresh=False):
        if ensure_fresh:
            self._force_refresh()
        ok_states = ALL_JOB_STATES
        if only_unclaimed:
            ok_states = UNCLAIMED_JOB_STATES
        with self._job_mutate:
            known_jobs = list(six.itervalues(self._known_jobs))
        for job in sorted(known_jobs):
            try:
                if job.state in ok_states:
                    yield job
            except excp.JobFailure:
                LOG.warn("Failed determining the state of job: %s (%s)",
                         job.uuid, job.path, exc_info=True)
            except excp.NotFound:
                # Someone destroyed it while we are iterating.
                with self._job_mutate:
                    self._remove_job(job.path)

    def _remove_job(self, path):
        LOG.debug("Removing job that was at path: %s", path)
        self._known_jobs.pop(path, None)

    def _process_child(self, path, request):
        """Receives the result of a child data fetch request."""
        try:
            raw_data, _stat = request.get()
            job_data = misc.decode_json(raw_data)
            with self._job_mutate:
                if path not in self._known_jobs:
                    job = ZookeeperJob(job_data['name'], self,
                                       self._client, self._persistence, path,
                                       uuid=job_data['uuid'],
                                       book_data=job_data.get("book"),
                                       details=job_data.get("details", {}))
                    self._known_jobs[path] = job
        except (ValueError, TypeError, KeyError):
            LOG.warn("Incorrectly formatted job data found at path: %s",
                     path, exc_info=True)
        except self._client.handler.timeout_exception:
            LOG.warn("Connection timed out fetching job data from path: %s",
                     path, exc_info=True)
        except k_exceptions.SessionExpiredError:
            LOG.warn("Session expired fetching job data from path: %s", path,
                     exc_info=True)
        except k_exceptions.NoNodeError:
            LOG.debug("No job node found at path: %s, it must have"
                      " disappeared or was removed", path)
        except k_exceptions.KazooException:
            LOG.warn("Internal error fetching job data from path: %s",
                     path, exc_info=True)

    def _on_job_posting(self, children, delayed=True):
        LOG.debug("Got children %s under path %s", children, self.path)
        child_paths = [k_paths.join(self.path, c) for c in children]

        # Remove jobs that we know about but which are no longer children
        with self._job_mutate:
            removals = set()
            for path, _job in six.iteritems(self._known_jobs):
                if path not in child_paths:
                    removals.add(path)
            for path in removals:
                self._remove_job(path)

        # Ensure that we have a job record for each new job that has appeared
        for path in child_paths:
            if path in self._bad_paths:
                continue
            with self._job_mutate:
                if path not in self._known_jobs:
                    # Fire off the request to populate this job asynchronously.
                    #
                    # This method is *usually* called from a asynchronous
                    # handler so it's better to exit from this quickly to
                    # allow other asynchronous handlers to be executed.
                    request = self._client.get_async(path)
                    child_proc = functools.partial(self._process_child, path)
                    if delayed:
                        request.rawlink(child_proc)
                    else:
                        child_proc(request)

    def post(self, name, book, details=None):

        def format_posting(job_uuid):
            posting = {
                'uuid': job_uuid,
                'name': name,
            }
            if details:
                posting['details'] = details
            else:
                posting['details'] = {}
            if book is not None:
                posting['book'] = {
                    'name': book.name,
                    'uuid': book.uuid,
                }
            return posting

        # NOTE(harlowja): Jobs are not ephemeral, they will persist until they
        # are consumed (this may change later, but seems safer to do this until
        # further notice).
        job_uuid = uuidutils.generate_uuid()
        with self._wrap(job_uuid, None,
                        "Posting failure: %s", ensure_known=False):
            job_posting = format_posting(job_uuid)
            job_posting = misc.binary_encode(jsonutils.dumps(job_posting))
            job_path = self._client.create(self._job_base,
                                           value=job_posting,
                                           sequence=True,
                                           ephemeral=False)
            job = ZookeeperJob(name, self, self._client,
                               self._persistence, job_path,
                               book=book, details=details,
                               uuid=job_uuid)
            with self._job_mutate:
                self._known_jobs[job_path] = job
            return job

    def claim(self, job, who):
        _check_who(who)
        with self._wrap(job.uuid, job.path, "Claiming failure: %s"):
            # NOTE(harlowja): post as json which will allow for future changes
            # more easily than a raw string/text.
            value = jsonutils.dumps({
                'owner': who,
            })
            try:
                self._client.create(job.lock_path,
                                    value=misc.binary_encode(value),
                                    ephemeral=True)
            except k_exceptions.NodeExistsException:
                # Try to see if we can find who the owner really is...
                try:
                    owner = self.find_owner(job)
                except Exception:
                    owner = None
                if owner:
                    msg = "Job %s already claimed by '%s'" % (job.uuid, owner)
                else:
                    msg = "Job %s already claimed" % (job.uuid)
                raise excp.UnclaimableJob(msg)

    @contextlib.contextmanager
    def _wrap(self, job_uuid, job_path,
              fail_msg_tpl="Failure: %s", ensure_known=True):
        if job_path:
            fail_msg_tpl += " (%s)" % (job_path)
        if ensure_known:
            if not job_path:
                raise ValueError("Unable to check if %r is a known path"
                                 % (job_path))
            with self._job_mutate:
                if job_path not in self._known_jobs:
                    fail_msg_tpl += ", unknown job"
                    raise excp.NotFound(fail_msg_tpl % (job_uuid))
        try:
            yield
        except self._client.handler.timeout_exception as e:
            fail_msg_tpl += ", connection timed out"
            raise excp.JobFailure(fail_msg_tpl % (job_uuid), e)
        except k_exceptions.SessionExpiredError as e:
            fail_msg_tpl += ", session expired"
            raise excp.JobFailure(fail_msg_tpl % (job_uuid), e)
        except k_exceptions.NoNodeError:
            fail_msg_tpl += ", unknown job"
            raise excp.NotFound(fail_msg_tpl % (job_uuid))
        except k_exceptions.KazooException as e:
            fail_msg_tpl += ", internal error"
            raise excp.JobFailure(fail_msg_tpl % (job_uuid), e)

    def find_owner(self, job):
        with self._wrap(job.uuid, job.path, "Owner query failure: %s"):
            try:
                self._client.sync(job.lock_path)
                raw_data, _lock_stat = self._client.get(job.lock_path)
                data = misc.decode_json(raw_data)
                owner = data.get("owner")
            except k_exceptions.NoNodeError:
                owner = None
            return owner

    def _get_owner_and_data(self, job):
        lock_data, lock_stat = self._client.get(job.lock_path)
        job_data, job_stat = self._client.get(job.path)
        return (misc.decode_json(lock_data), lock_stat,
                misc.decode_json(job_data), job_stat)

    def consume(self, job, who):
        _check_who(who)
        with self._wrap(job.uuid, job.path, "Consumption failure: %s"):
            try:
                owner_data = self._get_owner_and_data(job)
                lock_data, lock_stat, data, data_stat = owner_data
            except k_exceptions.NoNodeError:
                raise excp.JobFailure("Can not consume a job %s"
                                      " which we can not determine"
                                      " the owner of" % (job.uuid))
            if lock_data.get("owner") != who:
                raise excp.JobFailure("Can not consume a job %s"
                                      " which is not owned by %s"
                                      % (job.uuid, who))
            with self._client.transaction() as txn:
                txn.delete(job.lock_path, version=lock_stat.version)
                txn.delete(job.path, version=data_stat.version)
            with self._job_mutate:
                self._remove_job(job.path)

    def abandon(self, job, who):
        _check_who(who)
        with self._wrap(job.uuid, job.path, "Abandonment failure: %s"):
            try:
                owner_data = self._get_owner_and_data(job)
                lock_data, lock_stat, data, data_stat = owner_data
            except k_exceptions.NoNodeError:
                raise excp.JobFailure("Can not abandon a job %s"
                                      " which we can not determine"
                                      " the owner of" % (job.uuid))
            if lock_data.get("owner") != who:
                raise excp.JobFailure("Can not abandon a job %s"
                                      " which is not owned by %s"
                                      % (job.uuid, who))
            with self._client.transaction() as txn:
                txn.delete(job.lock_path, version=lock_stat.version)

    def _state_change_listener(self, state):
        LOG.debug("Kazoo client has changed to state: %s", state)

    def _clear(self):
        with self._job_mutate:
            self._known_jobs = {}
            self._job_watcher = None

    @property
    def connected(self):
        return self._client.connected

    @lock_utils.locked(lock='_open_close_lock')
    def close(self):
        if self._owned:
            LOG.debug("Stopping client")
            kazoo_utils.finalize_client(self._client)
        self._clear()
        LOG.debug("Stopped & cleared local state")

    @lock_utils.locked(lock='_open_close_lock')
    def connect(self, timeout=10.0):

        def try_clean():
            # Attempt to do the needed cleanup if post-connection setup does
            # not succeed (maybe the connection is lost right after it is
            # obtained).
            try:
                self.close()
            except k_exceptions.KazooException:
                LOG.exception("Failed cleaning-up after post-connection"
                              " initialization failed")

        try:
            if timeout is not None:
                timeout = float(timeout)
            self._client.start(timeout=timeout)
        except (self._client.handler.timeout_exception,
                k_exceptions.KazooException) as e:
            raise excp.JobFailure("Failed to connect to zookeeper", e)
        try:
            kazoo_utils.check_compatible(self._client, MIN_ZK_VERSION)
            self._client.ensure_path(self.path)
            self._job_watcher = watchers.ChildrenWatch(
                self._client,
                self.path,
                func=self._on_job_posting,
                allow_session_lost=False)
        except excp.IncompatibleVersion:
            with excutils.save_and_reraise_exception():
                try_clean()
        except (self._client.handler.timeout_exception,
                k_exceptions.KazooException) as e:
            try_clean()
            raise excp.JobFailure("Failed to do post-connection"
                                  " initialization", e)
