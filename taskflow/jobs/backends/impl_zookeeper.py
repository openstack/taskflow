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

import collections
import contextlib
import functools
import threading

from concurrent import futures
from kazoo import exceptions as k_exceptions
from kazoo.protocol import paths as k_paths
from kazoo.recipe import watchers
from oslo_serialization import jsonutils
from oslo_utils import excutils
from oslo_utils import uuidutils
import six

from taskflow import exceptions as excp
from taskflow.jobs import base
from taskflow import logging
from taskflow import states
from taskflow.types import timing as tt
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
LOCK_POSTFIX = ".lock"
JOB_PREFIX = 'job'


def _check_who(who):
    if not isinstance(who, six.string_types):
        raise TypeError("Job applicant must be a string type")
    if len(who) == 0:
        raise ValueError("Job applicant must be non-empty")


class ZookeeperJob(base.Job):
    """A zookeeper job."""

    def __init__(self, name, board, client, backend, path,
                 uuid=None, details=None, book=None, book_data=None,
                 created_on=None):
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
        self._path = k_paths.normpath(path)
        self._lock_path = path + LOCK_POSTFIX
        self._created_on = created_on
        self._node_not_found = False
        basename = k_paths.basename(self._path)
        self._root = self._path[0:-len(basename)]
        self._sequence = int(basename[len(JOB_PREFIX):])

    @property
    def lock_path(self):
        return self._lock_path

    @property
    def path(self):
        return self._path

    @property
    def sequence(self):
        """Sequence number of the current job."""
        return self._sequence

    @property
    def root(self):
        """The parent path of the job in zookeeper."""
        return self._root

    def _get_node_attr(self, path, attr_name, trans_func=None):
        try:
            _data, node_stat = self._client.get(path)
            attr = getattr(node_stat, attr_name)
            if trans_func is not None:
                return trans_func(attr)
            else:
                return attr
        except k_exceptions.NoNodeError as e:
            raise excp.NotFound("Can not fetch the %r attribute"
                                " of job %s (%s), path %s not found"
                                % (attr_name, self.uuid, self.path, path), e)
        except self._client.handler.timeout_exception as e:
            raise excp.JobFailure("Can not fetch the %r attribute"
                                  " of job %s (%s), operation timed out"
                                  % (attr_name, self.uuid, self.path), e)
        except k_exceptions.SessionExpiredError as e:
            raise excp.JobFailure("Can not fetch the %r attribute"
                                  " of job %s (%s), session expired"
                                  % (attr_name, self.uuid, self.path), e)
        except (AttributeError, k_exceptions.KazooException) as e:
            raise excp.JobFailure("Can not fetch the %r attribute"
                                  " of job %s (%s), internal error" %
                                  (attr_name, self.uuid, self.path), e)

    @property
    def last_modified(self):
        modified_on = None
        try:
            if not self._node_not_found:
                modified_on = self._get_node_attr(
                    self.path, 'mtime',
                    trans_func=misc.millis_to_datetime)
        except excp.NotFound:
            self._node_not_found = True
        return modified_on

    @property
    def created_on(self):
        # This one we can cache (since it won't change after creation).
        if self._node_not_found:
            return None
        if self._created_on is None:
            try:
                self._created_on = self._get_node_attr(
                    self.path, 'ctime',
                    trans_func=misc.millis_to_datetime)
            except excp.NotFound:
                self._node_not_found = True
        return self._created_on

    @property
    def board(self):
        return self._board

    def _load_book(self):
        book_uuid = self.book_uuid
        if self._backend is not None and book_uuid is not None:
            # TODO(harlowja): we are currently limited by assuming that the
            # job posted has the same backend as this loader (to start this
            # seems to be a ok assumption, and can be adjusted in the future
            # if we determine there is a use-case for multi-backend loaders,
            # aka a registry of loaders).
            with contextlib.closing(self._backend.get_connection()) as conn:
                return conn.get_logbook(book_uuid)
        # No backend to fetch from or no uuid specified
        return None

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
                                  " operation timed out" % (self.uuid), e)
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

    def __lt__(self, other):
        if self.root == other.root:
            return self.sequence < other.sequence
        else:
            return self.root < other.root

    def __hash__(self):
        return hash(self.path)

    @property
    def book(self):
        if self._book is None:
            self._book = self._load_book()
        return self._book

    @property
    def book_uuid(self):
        if self._book:
            return self._book.uuid
        if self._book_data:
            return self._book_data.get('uuid')
        else:
            return None

    @property
    def book_name(self):
        if self._book:
            return self._book.name
        if self._book_data:
            return self._book_data.get('name')
        else:
            return None


class ZookeeperJobBoardIterator(six.Iterator):
    """Iterator over a zookeeper jobboard that iterates over potential jobs.

    It supports the following attributes/constructor arguments:

    * ``ensure_fresh``: boolean that requests that during every fetch of a new
      set of jobs this will cause the iterator to force the backend to
      refresh (ensuring that the jobboard has the most recent job listings).
    * ``only_unclaimed``: boolean that indicates whether to only iterate
      over unclaimed jobs.
    """

    def __init__(self, board, only_unclaimed=False, ensure_fresh=False):
        self._board = board
        self._jobs = collections.deque()
        self._fetched = False
        self.ensure_fresh = ensure_fresh
        self.only_unclaimed = only_unclaimed

    @property
    def board(self):
        return self._board

    def __iter__(self):
        return self

    def _next_job(self):
        if self.only_unclaimed:
            allowed_states = UNCLAIMED_JOB_STATES
        else:
            allowed_states = ALL_JOB_STATES
        job = None
        while self._jobs and job is None:
            maybe_job = self._jobs.popleft()
            try:
                if maybe_job.state in allowed_states:
                    job = maybe_job
            except excp.JobFailure:
                LOG.warn("Failed determining the state of job: %s (%s)",
                         maybe_job.uuid, maybe_job.path, exc_info=True)
            except excp.NotFound:
                self._board._remove_job(maybe_job.path)
        return job

    def __next__(self):
        if not self._jobs:
            if not self._fetched:
                jobs = self._board._fetch_jobs(ensure_fresh=self.ensure_fresh)
                self._jobs.extend(jobs)
                self._fetched = True
        job = self._next_job()
        if job is None:
            raise StopIteration
        else:
            return job


class ZookeeperJobBoard(base.NotifyingJobBoard):
    """A jobboard backend by zookeeper.

    Powered by the `kazoo <http://kazoo.readthedocs.org/>`_ library.

    This jobboard creates *sequenced* persistent znodes in a directory in
    zookeeper (that directory defaults ``/taskflow/jobs``) and uses zookeeper
    watches to notify other jobboards that the job which was posted using the
    :meth:`.post` method (this creates a znode with contents/details in json)
    The users of those jobboard(s) (potentially on disjoint sets of machines)
    can then iterate over the available jobs and decide if they want to attempt
    to claim one of the jobs they have iterated over. If so they will then
    attempt to contact zookeeper and will attempt to create a ephemeral znode
    using the name of the persistent znode + ".lock" as a postfix. If the
    entity trying to use the jobboard to :meth:`.claim` the job is able to
    create a ephemeral znode with that name then it will be allowed (and
    expected) to perform whatever *work* the contents of that job that it
    locked described. Once finished the ephemeral znode and persistent znode
    may be deleted (if successfully completed) in a single transcation or if
    not successfull (or the entity that claimed the znode dies) the ephemeral
    znode will be released (either manually by using :meth:`.abandon` or
    automatically by zookeeper the ephemeral is deemed to be lost).
    """

    def __init__(self, name, conf,
                 client=None, persistence=None, emit_notifications=True):
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
        self._job_cond = threading.Condition()
        self._open_close_lock = threading.RLock()
        self._client.add_listener(self._state_change_listener)
        self._bad_paths = frozenset([path])
        self._job_watcher = None
        # Since we use sequenced ids this will be the path that the sequences
        # are prefixed with, for example, job0000000001, job0000000002, ...
        self._job_base = k_paths.join(path, JOB_PREFIX)
        self._worker = None
        self._emit_notifications = bool(emit_notifications)

    def _emit(self, state, details):
        # Submit the work to the executor to avoid blocking the kazoo queue.
        try:
            self._worker.submit(self.notifier.notify, state, details)
        except (AttributeError, RuntimeError):
            # Notification thread is shutdown or non-existent, either case we
            # just want to skip submitting a notification...
            pass

    @property
    def path(self):
        return self._path

    @property
    def job_count(self):
        return len(self._known_jobs)

    def _fetch_jobs(self, ensure_fresh=False):
        if ensure_fresh:
            self._force_refresh()
        with self._job_cond:
            return sorted(six.itervalues(self._known_jobs))

    def _force_refresh(self):
        try:
            children = self._client.get_children(self.path)
        except self._client.handler.timeout_exception as e:
            raise excp.JobFailure("Refreshing failure, operation timed out",
                                  e)
        except k_exceptions.SessionExpiredError as e:
            raise excp.JobFailure("Refreshing failure, session expired", e)
        except k_exceptions.NoNodeError:
            pass
        except k_exceptions.KazooException as e:
            raise excp.JobFailure("Refreshing failure, internal error", e)
        else:
            self._on_job_posting(children, delayed=False)

    def iterjobs(self, only_unclaimed=False, ensure_fresh=False):
        return ZookeeperJobBoardIterator(self,
                                         only_unclaimed=only_unclaimed,
                                         ensure_fresh=ensure_fresh)

    def _remove_job(self, path):
        if path not in self._known_jobs:
            return
        with self._job_cond:
            job = self._known_jobs.pop(path, None)
        if job is not None:
            LOG.debug("Removed job that was at path '%s'", path)
            self._emit(base.REMOVAL, details={'job': job})

    def _process_child(self, path, request):
        """Receives the result of a child data fetch request."""
        job = None
        try:
            raw_data, node_stat = request.get()
            job_data = misc.decode_json(raw_data)
            created_on = misc.millis_to_datetime(node_stat.ctime)
        except (ValueError, TypeError, KeyError):
            LOG.warn("Incorrectly formatted job data found at path: %s",
                     path, exc_info=True)
        except self._client.handler.timeout_exception:
            LOG.warn("Operation timed out fetching job data from path: %s",
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
        else:
            with self._job_cond:
                # Now we can offically check if someone already placed this
                # jobs information into the known job set (if it's already
                # existing then just leave it alone).
                if path not in self._known_jobs:
                    job = ZookeeperJob(job_data['name'], self,
                                       self._client, self._persistence, path,
                                       uuid=job_data['uuid'],
                                       book_data=job_data.get("book"),
                                       details=job_data.get("details", {}),
                                       created_on=created_on)
                    self._known_jobs[path] = job
                    self._job_cond.notify_all()
        if job is not None:
            self._emit(base.POSTED, details={'job': job})

    def _on_job_posting(self, children, delayed=True):
        LOG.debug("Got children %s under path %s", children, self.path)
        child_paths = []
        for c in children:
            if c.endswith(LOCK_POSTFIX) or not c.startswith(JOB_PREFIX):
                # Skip lock paths or non-job-paths (these are not valid jobs)
                continue
            child_paths.append(k_paths.join(self.path, c))

        # Figure out what we really should be investigating and what we
        # shouldn't (remove jobs that exist in our local version, but don't
        # exist in the children anymore) and accumulate all paths that we
        # need to trigger population of (without holding the job lock).
        investigate_paths = []
        pending_removals = []
        with self._job_cond:
            for path in six.iterkeys(self._known_jobs):
                if path not in child_paths:
                    pending_removals.append(path)
        for path in child_paths:
            if path in self._bad_paths:
                continue
            # This pre-check will *not* guarantee that we will not already
            # have the job (if it's being populated elsewhere) but it will
            # reduce the amount of duplicated requests in general; later when
            # the job information has been populated we will ensure that we
            # are not adding duplicates into the currently known jobs...
            if path in self._known_jobs:
                continue
            if path not in investigate_paths:
                investigate_paths.append(path)
        if pending_removals:
            with self._job_cond:
                for path in pending_removals:
                    self._remove_job(path)
        for path in investigate_paths:
            # Fire off the request to populate this job.
            #
            # This method is *usually* called from a asynchronous handler so
            # it's better to exit from this quickly to allow other asynchronous
            # handlers to be executed.
            request = self._client.get_async(path)
            if delayed:
                request.rawlink(functools.partial(self._process_child, path))
            else:
                self._process_child(path, request)

    def post(self, name, book=None, details=None):

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
            with self._job_cond:
                self._known_jobs[job_path] = job
                self._job_cond.notify_all()
            self._emit(base.POSTED, details={'job': job})
            return job

    def claim(self, job, who):
        def _unclaimable_try_find_owner(cause):
            try:
                owner = self.find_owner(job)
            except Exception:
                owner = None
            if owner:
                msg = "Job %s already claimed by '%s'" % (job.uuid, owner)
            else:
                msg = "Job %s already claimed" % (job.uuid)
            return excp.UnclaimableJob(msg, cause)

        _check_who(who)
        with self._wrap(job.uuid, job.path, "Claiming failure: %s"):
            # NOTE(harlowja): post as json which will allow for future changes
            # more easily than a raw string/text.
            value = jsonutils.dumps({
                'owner': who,
            })
            # Ensure the target job is still existent (at the right version).
            job_data, job_stat = self._client.get(job.path)
            txn = self._client.transaction()
            # This will abort (and not create the lock) if the job has been
            # removed (somehow...) or updated by someone else to a different
            # version...
            txn.check(job.path, version=job_stat.version)
            txn.create(job.lock_path, value=misc.binary_encode(value),
                       ephemeral=True)
            try:
                kazoo_utils.checked_commit(txn)
            except k_exceptions.NodeExistsError as e:
                raise _unclaimable_try_find_owner(e)
            except kazoo_utils.KazooTransactionException as e:
                if len(e.failures) < 2:
                    raise
                else:
                    if isinstance(e.failures[0], k_exceptions.NoNodeError):
                        raise excp.NotFound(
                            "Job %s not found to be claimed" % job.uuid,
                            e.failures[0])
                    if isinstance(e.failures[1], k_exceptions.NodeExistsError):
                        raise _unclaimable_try_find_owner(e.failures[1])
                    else:
                        raise excp.UnclaimableJob(
                            "Job %s claim failed due to transaction"
                            " not succeeding" % (job.uuid), e)

    @contextlib.contextmanager
    def _wrap(self, job_uuid, job_path,
              fail_msg_tpl="Failure: %s", ensure_known=True):
        if job_path:
            fail_msg_tpl += " (%s)" % (job_path)
        if ensure_known:
            if not job_path:
                raise ValueError("Unable to check if %r is a known path"
                                 % (job_path))
            if job_path not in self._known_jobs:
                fail_msg_tpl += ", unknown job"
                raise excp.NotFound(fail_msg_tpl % (job_uuid))
        try:
            yield
        except self._client.handler.timeout_exception as e:
            fail_msg_tpl += ", operation timed out"
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
            txn = self._client.transaction()
            txn.delete(job.lock_path, version=lock_stat.version)
            txn.delete(job.path, version=data_stat.version)
            kazoo_utils.checked_commit(txn)
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
            txn = self._client.transaction()
            txn.delete(job.lock_path, version=lock_stat.version)
            kazoo_utils.checked_commit(txn)

    def _state_change_listener(self, state):
        LOG.debug("Kazoo client has changed to state: %s", state)

    def wait(self, timeout=None):
        # Wait until timeout expires (or forever) for jobs to appear.
        watch = tt.StopWatch(duration=timeout)
        watch.start()
        with self._job_cond:
            while True:
                if not self._known_jobs:
                    if watch.expired():
                        raise excp.NotFound("Expired waiting for jobs to"
                                            " arrive; waited %s seconds"
                                            % watch.elapsed())
                    # This is done since the given timeout can not be provided
                    # to the condition variable, since we can not ensure that
                    # when we acquire the condition that there will actually
                    # be jobs (especially if we are spuriously awaken), so we
                    # must recalculate the amount of time we really have left.
                    self._job_cond.wait(watch.leftover(return_none=True))
                else:
                    it = ZookeeperJobBoardIterator(self)
                    it._jobs.extend(self._fetch_jobs())
                    it._fetched = True
                    return it

    @property
    def connected(self):
        return self._client.connected

    @lock_utils.locked(lock='_open_close_lock')
    def close(self):
        if self._owned:
            LOG.debug("Stopping client")
            kazoo_utils.finalize_client(self._client)
        if self._worker is not None:
            LOG.debug("Shutting down the notifier")
            self._worker.shutdown()
            self._worker = None
        with self._job_cond:
            self._known_jobs.clear()
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
            if self._conf.get('check_compatible', True):
                kazoo_utils.check_compatible(self._client, MIN_ZK_VERSION)
            if self._worker is None and self._emit_notifications:
                self._worker = futures.ThreadPoolExecutor(max_workers=1)
            self._client.ensure_path(self.path)
            if self._job_watcher is None:
                self._job_watcher = watchers.ChildrenWatch(
                    self._client,
                    self.path,
                    func=self._on_job_posting,
                    allow_session_lost=True)
        except excp.IncompatibleVersion:
            with excutils.save_and_reraise_exception():
                try_clean()
        except (self._client.handler.timeout_exception,
                k_exceptions.KazooException) as e:
            try_clean()
            raise excp.JobFailure("Failed to do post-connection"
                                  " initialization", e)
