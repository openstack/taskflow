#    Copyright (C) Red Hat
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

import threading
import typing

import etcd3gw
import fasteners
from oslo_serialization import jsonutils
from oslo_utils import timeutils
from oslo_utils import uuidutils

from taskflow import exceptions as exc
from taskflow.jobs import base
from taskflow import logging
from taskflow import states
from taskflow.utils import misc
if typing.TYPE_CHECKING:
    from taskflow.types import entity

LOG = logging.getLogger(__name__)


class EtcdJob(base.Job):
    """An Etcd job."""

    board: 'EtcdJobBoard'

    def __init__(self, board: 'EtcdJobBoard', name, client, key,
                 uuid=None, details=None, backend=None,
                 book=None, book_data=None,
                 priority=base.JobPriority.NORMAL,
                 sequence=None, created_on=None):
        super().__init__(board, name, uuid=uuid, details=details,
                         backend=backend, book=book, book_data=book_data)

        self._client = client
        self._key = key
        self._priority = priority
        self._sequence = sequence
        self._created_on = created_on
        self._root = board._root_path

        self._lease = None

    @property
    def key(self):
        return self._key

    @property
    def last_modified(self):
        try:
            raw_data = self.board.get_last_modified(self)
            data = jsonutils.loads(raw_data)
            ret = timeutils.parse_strtime(data["last_modified"])
            return ret
        except Exception:
            LOG.exception("Cannot read load_modified key.")
        return 0

    @property
    def created_on(self):
        return self._created_on

    @property
    def state(self):
        """Access the current state of this job."""
        owner, data = self.board.get_owner_and_data(self)
        if not data:
            if owner is not None:
                LOG.info(f"Owner key was found for job {self.uuid}, "
                         f"but the key {self.key} is missing")
            return states.COMPLETE
        if not owner:
            return states.UNCLAIMED
        return states.CLAIMED

    @property
    def sequence(self):
        return self._sequence

    @property
    def priority(self):
        return self._priority

    @property
    def lease(self):
        if not self._lease:
            owner_data = self.board.get_owner_data(self)
            if 'lease_id' not in owner_data:
                return None
            lease_id = owner_data['lease_id']
            self._lease = etcd3gw.Lease(id=lease_id,
                                        client=self._client)
        return self._lease

    def expires_in(self):
        """How many seconds until the claim expires."""
        if self.lease is None:
            return -1
        return self.lease.ttl()

    def extend_expiry(self, expiry):
        """Extends the owner key (aka the claim) expiry for this job.

        Returns ``True`` if the expiry request was performed
        otherwise ``False``.
        """
        if self.lease is None:
            return False
        ret = self.lease.refresh()
        return (ret > 0)

    @property
    def root(self):
        return self._root

    def __lt__(self, other):
        if not isinstance(other, EtcdJob):
            return NotImplemented
        if self.root == other.root:
            if self.priority == other.priority:
                return self.sequence < other.sequence
            else:
                ordered = base.JobPriority.reorder(
                    (self.priority, self), (other.priority, other))
                if ordered[0] is self:
                    return False
                return True
        else:
            # Different jobboards with different roots...
            return self.root < other.root

    def __eq__(self, other):
        if not isinstance(other, EtcdJob):
            return NotImplemented
        return ((self.root, self.sequence, self.priority) ==
                (other.root, other.sequence, other.priority))

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.key)


class EtcdJobBoard(base.JobBoard):
    """A jobboard backed by `etcd`_.

    This jobboard creates sequenced key/value pairs in etcd. Each key
    represents a job and its associated value contains the parameter of the
    job encoded in
    json.
    The users of the jobboard can iterate over the available job and decide if
    they want to attempt to claim one job by calling the :meth:`.claim` method.
    Claiming a job consists in atomically create a key based on the key of job
    and the ".lock" postfix. If the atomic creation of the key is successful
    the job belongs to the user. Any attempt to lock an already locked job
    will fail.
    When a job is complete, the user consumes the job by calling the
    :meth:`.consume` method, it deletes the job and the lock from etcd.
    Alternatively, a user can trash (:meth:`.trash`) or abandon
    (:meth:`.abandon`) if they want to delete the job or leave it for another
    user.
    Etcd doesn't provide a method for unlocking the jobs when a consumer dies.
    The Etcd jobboard provides timed expirations, based on a global ``ttl``
    configuration setting or the ``expiry`` parameter of the :meth:`.claim`
    method. When this time-to-live/expiry is reached, the job is automatically
    unlocked and another consumer can claim it. If it is expected that a task
    of a job takes more time than the defined time-to-live, the
    consumer can refresh the timer by calling the :meth:`EtcdJob.extend_expiry`
    function.

    .. _etcd: https://etcd.io/
    """
    ROOT_PATH = "/taskflow/jobs"

    TRASH_PATH = "/taskflow/.trash"

    DEFAULT_PATH = "jobboard"

    JOB_PREFIX = "job"

    SEQUENCE_KEY = "sequence"

    DATA_POSTFIX = ".data"

    LOCK_POSTFIX = ".lock"

    LAST_MODIFIED_POSTFIX = ".last_modified"

    ETCD_CONFIG_OPTIONS = (
        ("host", str),
        ("port", int),
        ("protocol", str),
        ("ca_cert", str),
        ("cert_key", str),
        ("cert_cert", str),
        ("timeout", float),
        ("api_path", str),
    )

    INIT_STATE = 'init'
    CONNECTED_STATE = 'connected'
    FETCH_STATE = 'fetched'

    _client: etcd3gw.Etcd3Client

    def __init__(self, name, conf, client=None, persistence=None):
        super().__init__(name, conf)

        self._client = client
        self._persistence = persistence
        self._state = self.INIT_STATE

        path_elems = [self.ROOT_PATH,
                      self._conf.get("path", self.DEFAULT_PATH)]
        self._root_path = self._create_path(*path_elems)

        self._job_cache = {}
        self._job_cond = threading.Condition()

        self._open_close_lock = threading.RLock()

        self._watcher_thd = None
        self._thread_cancel = None
        self._watcher = None
        self._watcher_cancel = None

    def _create_path(self, root, *args):
        return "/".join([root] + [a.strip("/") for a in args])

    def incr(self, key):
        """Atomically increment an integer, create it if it doesn't exist"""
        while True:
            value = self._client.get(key)
            if not value:
                res = self._client.create(key, 1)
                if res:
                    return 1
                # Another thread has just created the key after we failed to
                # read it, retry to get the new current value
                continue

            value = int(value[0])
            next_value = value + 1

            res = self._client.replace(key, value, next_value)
            if res:
                return next_value

    def get_one(self, key):
        if self._client is None:
            raise exc.JobFailure(f"Cannot read key {key}, client is closed")
        value = self._client.get(key)
        if not value:
            return None
        return value[0]

    def _fetch_jobs(self, only_unclaimed=False, ensure_fresh=False):
        # TODO(gthiemonge) only_unclaimed is ignored
        if ensure_fresh or self._state != self.FETCH_STATE:
            self._ensure_fresh()
        return sorted(self._job_cache.values())

    def _ensure_fresh(self):
        prefix = self._create_path(self._root_path, self.JOB_PREFIX)
        jobs = self._client.get_prefix(prefix)
        listed_jobs = {}
        for job in jobs:
            data, metadata = job
            key = misc.binary_decode(metadata['key'])
            if key.endswith(self.DATA_POSTFIX):
                key = key.rstrip(self.DATA_POSTFIX)
                listed_jobs[key] = data

        removed_jobs = []
        with self._job_cond:
            for key in self._job_cache.keys():
                if key not in listed_jobs:
                    removed_jobs.append(key)
        for key in removed_jobs:
            self._remove_job_from_cache(key)

        for key, data in listed_jobs.items():
            self._process_incoming_job(key, data)
        self._state = self.FETCH_STATE

    def _process_incoming_job(self, key, data):
        try:
            job_data = jsonutils.loads(data)
        except jsonutils.json.JSONDecodeError:
            msg = ("Incorrectly formatted job data found at "
                   f"key: {key}")
            LOG.warning(msg, exc_info=True)
            LOG.info("Deleting invalid job data at key: %s", key)
            self._client.delete(key)
            raise exc.JobFailure(msg)

        with self._job_cond:
            if key not in self._job_cache:
                job_priority = base.JobPriority.convert(job_data["priority"])
                new_job = EtcdJob(self,
                                  job_data["name"],
                                  self._client,
                                  key,
                                  uuid=job_data["uuid"],
                                  details=job_data.get("details", {}),
                                  backend=self._persistence,
                                  book_data=job_data.get("book"),
                                  priority=job_priority,
                                  sequence=job_data["sequence"])
                self._job_cache[key] = new_job
                self._job_cond.notify_all()

    def _remove_job_from_cache(self, key):
        """Remove job from cache."""
        with self._job_cond:
            if key in self._job_cache:
                self._job_cache.pop(key, None)

    def _board_removal_func(self, job):
        try:
            self._remove_job_from_cache(job.key)
            self._client.delete_prefix(job.key)
        except Exception:
            LOG.exception(f"Failed to delete prefix {job.key}")

    def iterjobs(self, only_unclaimed=False, ensure_fresh=False):
        """Returns an iterator of jobs that are currently on this board."""
        return base.JobBoardIterator(
            self, LOG, only_unclaimed=only_unclaimed,
            ensure_fresh=ensure_fresh,
            board_fetch_func=self._fetch_jobs,
            board_removal_func=self._board_removal_func)

    def wait(self, timeout=None):
        """Waits a given amount of time for **any** jobs to be posted."""
        # Wait until timeout expires (or forever) for jobs to appear.
        watch = timeutils.StopWatch(duration=timeout)
        watch.start()
        with self._job_cond:
            while True:
                if not self._job_cache:
                    if watch.expired():
                        raise exc.NotFound("Expired waiting for jobs to"
                                           " arrive; waited %s seconds"
                                           % watch.elapsed())
                    # This is done since the given timeout can not be provided
                    # to the condition variable, since we can not ensure that
                    # when we acquire the condition that there will actually
                    # be jobs (especially if we are spuriously awaken), so we
                    # must recalculate the amount of time we really have left.
                    self._job_cond.wait(watch.leftover(return_none=True))
                else:
                    curr_jobs = self._fetch_jobs()
                    fetch_func = lambda ensure_fresh: curr_jobs
                    removal_func = lambda a_job: self._remove_job_from_cache(
                        a_job.key)
                    return base.JobBoardIterator(
                        self, LOG, board_fetch_func=fetch_func,
                        board_removal_func=removal_func)

    @property
    def job_count(self):
        """Returns how many jobs are on this jobboard."""
        return len(self._job_cache)

    def get_owner_data(self, job: EtcdJob) -> typing.Optional[dict]:
        owner_key = job.key + self.LOCK_POSTFIX
        owner_data = self.get_one(owner_key)
        if not owner_data:
            return None
        return jsonutils.loads(owner_data)

    def find_owner(self, job: EtcdJob) -> typing.Optional[dict]:
        """Gets the owner of the job if one exists."""
        data = self.get_owner_data(job)
        if data:
            return data['owner']
        return None

    def get_data(self, job: EtcdJob) -> bytes:
        key = job.key + self.DATA_POSTFIX
        return self.get_one(key)

    def get_owner_and_data(self, job: EtcdJob) -> tuple[
            typing.Optional[str], typing.Optional[bytes]]:
        if self._client is None:
            raise exc.JobFailure("Cannot retrieve information, "
                                 "not connected")

        job_data = None
        job_owner = None

        for data, metadata in self._client.get_prefix(job.key + "."):
            key = misc.binary_decode(metadata["key"])
            if key.endswith(self.DATA_POSTFIX):
                # bytes?
                job_data = data
            elif key.endswith(self.LOCK_POSTFIX):
                data = jsonutils.loads(data)
                job_owner = data["owner"]

        return job_owner, job_data

    def set_last_modified(self, job: EtcdJob):
        key = job.key + self.LAST_MODIFIED_POSTFIX

        now = timeutils.utcnow()
        self._client.put(key, jsonutils.dumps({"last_modified": now}))

    def get_last_modified(self, job: EtcdJob):
        key = job.key + self.LAST_MODIFIED_POSTFIX

        return self.get_one(key)

    def post(self, name, book=None, details=None,
             priority=base.JobPriority.NORMAL) -> EtcdJob:
        """Atomically creates and posts a job to the jobboard."""
        job_priority = base.JobPriority.convert(priority)
        job_uuid = uuidutils.generate_uuid()
        job_posting = base.format_posting(job_uuid, name,
                                          created_on=timeutils.utcnow(),
                                          book=book, details=details,
                                          priority=job_priority)
        seq = self.incr(self._create_path(self._root_path, self.SEQUENCE_KEY))
        key = self._create_path(self._root_path, f"{self.JOB_PREFIX}{seq}")

        job_posting["sequence"] = seq
        raw_job_posting = jsonutils.dumps(job_posting)

        data_key = key + self.DATA_POSTFIX

        self._client.create(data_key, raw_job_posting)
        job = EtcdJob(self, name, self._client, key,
                      uuid=job_uuid,
                      details=details,
                      backend=self._persistence,
                      book=book,
                      book_data=job_posting.get('book'),
                      priority=job_priority,
                      sequence=seq)
        with self._job_cond:
            self._job_cache[key] = job
            self._job_cond.notify_all()
        return job

    @base.check_who
    def claim(self, job, who, expiry=None):
        """Atomically attempts to claim the provided job."""
        owner_key = job.key + self.LOCK_POSTFIX

        ttl = expiry or self._conf.get('ttl', None)

        if ttl:
            lease = self._client.lease(ttl=ttl)
        else:
            lease = None

        owner_dict = {
            "owner": who,
        }
        if lease:
            owner_dict["lease_id"] = lease.id

        owner_value = jsonutils.dumps(owner_dict)

        # Create a lock for the job, if the lock already exists, the job
        # is owned by another worker
        created = self._client.create(owner_key, owner_value, lease=lease)
        if not created:
            # Creation is denied, revoke the lease, we cannot claim the job.
            if lease:
                lease.revoke()

            owner = self.find_owner(job)
            if owner:
                message = f"Job {job.uuid} already claimed by '{owner}'"
            else:
                message = f"Job {job.uuid} already claimed"
            raise exc.UnclaimableJob(message)

        # Ensure that the job still exists, it may have been claimed and
        # consumed by another thread before we enter this function
        if not self.get_data(job):
            # Revoke the lease
            if lease:
                lease.revoke()
            else:
                self._client.delete(owner_key)
            raise exc.UnclaimableJob(f"Job {job.uuid} already deleted.")

        self.set_last_modified(job)

    @base.check_who
    def consume(self, job, who):
        """Permanently (and atomically) removes a job from the jobboard."""
        owner, data = self.get_owner_and_data(job)
        if data is None or owner is None:
            raise exc.NotFound(f"Cannot find job {job.uuid}")
        if owner != who:
            raise exc.JobFailure(f"Cannot consume a job {job.uuid}"
                                 f" which is not owned by {who}")

        self._client.delete_prefix(job.key + ".")
        self._remove_job_from_cache(job.key)

    @base.check_who
    def abandon(self, job, who):
        """Atomically attempts to abandon the provided job."""
        owner, data = self.get_owner_and_data(job)
        if data is None or owner is None:
            raise exc.NotFound(f"Cannot find job {job.uuid}")
        if owner != who:
            raise exc.JobFailure(f"Cannot abandon a job {job.uuid}"
                                 f" which is not owned by {who}")

        owner_key = job.key + self.LOCK_POSTFIX
        self._client.delete(owner_key)

    @base.check_who
    def trash(self, job, who):
        """Trash the provided job."""
        owner, data = self.get_owner_and_data(job)
        if data is None or owner is None:
            raise exc.NotFound(f"Cannot find job {job.uuid}")
        if owner != who:
            raise exc.JobFailure(f"Cannot trash a job {job.uuid} "
                                 f"which is not owned by {who}")

        trash_key = job.key.replace(self.ROOT_PATH, self.TRASH_PATH)
        self._client.create(trash_key, data)
        self._client.delete_prefix(job.key + ".")
        self._remove_job_from_cache(job.key)

    def register_entity(self, entity: 'entity.Entity'):
        """Register an entity to the jobboard('s backend), e.g: a conductor"""
        # TODO(gthiemonge) Doesn't seem to be useful with Etcd

    @property
    def connected(self):
        """Returns if this jobboard is connected."""
        return self._client is not None

    @fasteners.locked(lock='_open_close_lock')
    def connect(self):
        """Opens the connection to any backend system."""
        if self._client is None:
            etcd_conf = {}
            for config_opts in self.ETCD_CONFIG_OPTIONS:
                key, value_type = config_opts
                if key in self._conf:
                    etcd_conf[key] = value_type(self._conf[key])

            self._client = etcd3gw.Etcd3Client(**etcd_conf)
            self._state = self.CONNECTED_STATE

            watch_url = self._create_path(self._root_path, self.JOB_PREFIX)
            self._thread_cancel = threading.Event()
            try:
                (self._watcher,
                 self._watcher_cancel) = self._client.watch_prefix(watch_url)
            except etcd3gw.exceptions.ConnectionFailedError:
                exc.raise_with_cause(exc.JobFailure,
                                     "Failed to connect to Etcd")
            self._watcher_thd = threading.Thread(target=self._watcher_thread)
            self._watcher_thd.start()

    def _watcher_thread(self):
        while not self._thread_cancel.is_set():
            for event in self._watcher:
                if "kv" not in event:
                    continue

                key_value = event["kv"]
                key = misc.binary_decode(key_value["key"])

                if key.endswith(self.DATA_POSTFIX):
                    key = key.rstrip(self.DATA_POSTFIX)
                    if event.get("type") == "DELETE":
                        self._remove_job_from_cache(key)
                    else:
                        data = key_value["value"]
                        self._process_incoming_job(key, data)

    @fasteners.locked(lock='_open_close_lock')
    def close(self):
        """Close the connection to any backend system."""
        if self._client is not None:
            if self._watcher_cancel is not None:
                self._watcher_cancel()
            if self._thread_cancel is not None:
                self._thread_cancel.set()
            if self._watcher_thd is not None:
                self._watcher_thd.join()
            del self._client
            self._client = None
            self._state = self.INIT_STATE
