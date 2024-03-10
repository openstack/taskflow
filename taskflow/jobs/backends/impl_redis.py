# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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
import datetime
import functools
import re
import string
import threading
import time

import fasteners
import msgpack
from oslo_serialization import msgpackutils
from oslo_utils import excutils
from oslo_utils import strutils
from oslo_utils import timeutils
from oslo_utils import uuidutils
from redis import exceptions as redis_exceptions
from redis import sentinel

from taskflow import exceptions as exc
from taskflow.jobs import base
from taskflow import logging
from taskflow import states
from taskflow.utils import misc
from taskflow.utils import redis_utils as ru


LOG = logging.getLogger(__name__)


@contextlib.contextmanager
def _translate_failures():
    """Translates common redis exceptions into taskflow exceptions."""
    try:
        yield
    except redis_exceptions.ConnectionError:
        exc.raise_with_cause(exc.JobFailure, "Failed to connect to redis")
    except redis_exceptions.TimeoutError:
        exc.raise_with_cause(exc.JobFailure,
                             "Failed to communicate with redis, connection"
                             " timed out")
    except redis_exceptions.RedisError:
        exc.raise_with_cause(exc.JobFailure,
                             "Failed to communicate with redis,"
                             " internal error")


@functools.total_ordering
class RedisJob(base.Job):
    """A redis job."""

    def __init__(self, board, name, sequence, key,
                 uuid=None, details=None,
                 created_on=None, backend=None,
                 book=None, book_data=None,
                 priority=base.JobPriority.NORMAL):
        super(RedisJob, self).__init__(board, name,
                                       uuid=uuid, details=details,
                                       backend=backend,
                                       book=book, book_data=book_data)
        self._created_on = created_on
        self._client = board._client
        self._redis_version = board._redis_version
        self._sequence = sequence
        self._key = key
        self._last_modified_key = board.join(key + board.LAST_MODIFIED_POSTFIX)
        self._owner_key = board.join(key + board.OWNED_POSTFIX)
        self._priority = priority

    @property
    def key(self):
        """Key (in board listings/trash hash) the job data is stored under."""
        return self._key

    @property
    def priority(self):
        return self._priority

    @property
    def last_modified_key(self):
        """Key the job last modified data is stored under."""
        return self._last_modified_key

    @property
    def owner_key(self):
        """Key the job claim + data of the owner is stored under."""
        return self._owner_key

    @property
    def sequence(self):
        """Sequence number of the current job."""
        return self._sequence

    def expires_in(self):
        """How many seconds until the claim expires.

        Returns the number of seconds until the ownership entry expires or
        :attr:`~taskflow.utils.redis_utils.UnknownExpire.DOES_NOT_EXPIRE` or
        :attr:`~taskflow.utils.redis_utils.UnknownExpire.KEY_NOT_FOUND` if it
        does not expire or if the expiry can not be determined (perhaps the
        :attr:`.owner_key` expired at/before time of inquiry?).
        """
        with _translate_failures():
            return ru.get_expiry(self._client, self._owner_key,
                                 prior_version=self._redis_version)

    def extend_expiry(self, expiry):
        """Extends the owner key (aka the claim) expiry for this job.

        NOTE(harlowja): if the claim for this job did **not** previously
        have an expiry associated with it, calling this method will create
        one (and after that time elapses the claim on this job will cease
        to exist).

        Returns ``True`` if the expiry request was performed
        otherwise ``False``.
        """
        with _translate_failures():
            return ru.apply_expiry(self._client, self._owner_key, expiry,
                                   prior_version=self._redis_version)

    def __lt__(self, other):
        if not isinstance(other, RedisJob):
            return NotImplemented
        if self.board.listings_key == other.board.listings_key:
            if self.priority == other.priority:
                return self.sequence < other.sequence
            else:
                ordered = base.JobPriority.reorder(
                    (self.priority, self), (other.priority, other))
                if ordered[0] is self:
                    return False
                return True
        else:
            # Different jobboards with different listing keys...
            return self.board.listings_key < other.board.listings_key

    def __eq__(self, other):
        if not isinstance(other, RedisJob):
            return NotImplemented
        return ((self.board.listings_key, self.priority, self.sequence) ==
                (other.board.listings_key, other.priority, other.sequence))

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((self.board.listings_key, self.priority, self.sequence))

    @property
    def created_on(self):
        return self._created_on

    @property
    def last_modified(self):
        with _translate_failures():
            raw_last_modified = self._client.get(self._last_modified_key)
        last_modified = None
        if raw_last_modified:
            last_modified = self._board._loads(
                raw_last_modified, root_types=(datetime.datetime,))
            # NOTE(harlowja): just incase this is somehow busted (due to time
            # sync issues/other), give back the most recent one (since redis
            # does not maintain clock information; we could have this happen
            # due to now clients who mutate jobs also send the time in).
            last_modified = max(last_modified, self._created_on)
        return last_modified

    @property
    def state(self):
        listings_key = self._board.listings_key
        owner_key = self._owner_key
        listings_sub_key = self._key

        def _do_fetch(p):
            # NOTE(harlowja): state of a job in redis is not set into any
            # explicit 'state' field, but is maintained by what nodes exist in
            # redis instead (ie if a owner key exists, then we know a owner
            # is active, if no job data exists and no owner, then we know that
            # the job is unclaimed, and so-on)...
            p.multi()
            p.hexists(listings_key, listings_sub_key)
            p.exists(owner_key)
            job_exists, owner_exists = p.execute()
            if not job_exists:
                if owner_exists:
                    # This should **not** be possible due to lua code ordering
                    # but let's log an INFO statement if it does happen (so
                    # that it can be investigated)...
                    LOG.info("Unexpected owner key found at '%s' when job"
                             " key '%s[%s]' was not found", owner_key,
                             listings_key, listings_sub_key)
                return states.COMPLETE
            else:
                if owner_exists:
                    return states.CLAIMED
                else:
                    return states.UNCLAIMED

        with _translate_failures():
            return self._client.transaction(_do_fetch,
                                            listings_key, owner_key,
                                            value_from_callable=True)


class RedisJobBoard(base.JobBoard):
    """A jobboard backed by `redis`_.

    Powered by the `redis-py <http://redis-py.readthedocs.org/>`_ library.

    This jobboard creates job entries by listing jobs in a redis `hash`_. This
    hash contains jobs that can be actively worked on by (and examined/claimed
    by) some set of eligible consumers. Job posting is typically performed
    using the :meth:`.post` method (this creates a hash entry with job
    contents/details encoded in `msgpack`_). The users of these
    jobboard(s) (potentially on disjoint sets of machines) can then
    iterate over the available jobs and decide if they want to attempt to
    claim one of the jobs they have iterated over. If so they will then
    attempt to contact redis and they will attempt to create a key in
    redis (using a embedded lua script to perform this atomically) to claim a
    desired job. If the entity trying to use the jobboard to :meth:`.claim`
    the job is able to create that lock/owner key then it will be
    allowed (and expected) to perform whatever *work* the contents of that
    job described. Once the claiming entity is finished the lock/owner key
    and the `hash`_ entry will be deleted (if successfully completed) in a
    single request (also using a embedded lua script to perform this
    atomically). If the claiming entity is not successful (or the entity
    that claimed the job dies) the lock/owner key can be released
    automatically (by **optional** usage of a claim expiry) or by
    using :meth:`.abandon` to manually abandon the job so that it can be
    consumed/worked on by others.

    NOTE(harlowja): by default the :meth:`.claim` has no expiry (which
    means claims will be persistent, even under claiming entity failure). To
    ensure a expiry occurs pass a numeric value for the ``expiry`` keyword
    argument to the :meth:`.claim` method that defines how many seconds the
    claim should be retained for. When an expiry is used ensure that that
    claim is kept alive while it is being worked on by using
    the :py:meth:`~.RedisJob.extend_expiry` method periodically.

    .. _msgpack: https://msgpack.org/
    .. _redis: https://redis.io/
    .. _hash: https://redis.io/topics/data-types#hashes
    """

    CLIENT_CONF_TRANSFERS = tuple([
        # Host config...
        ('host', str),
        ('port', int),

        # See: http://redis.io/commands/auth
        ('username', str),
        ('password', str),

        # Data encoding/decoding + error handling
        ('encoding', str),
        ('encoding_errors', str),

        # Connection settings.
        ('socket_timeout', float),
        ('socket_connect_timeout', float),

        # This one negates the usage of host, port, socket connection
        # settings as it doesn't use the same kind of underlying socket...
        ('unix_socket_path', str),

        # Do u want ssl???
        ('ssl', strutils.bool_from_string),
        ('ssl_keyfile', str),
        ('ssl_certfile', str),
        ('ssl_cert_reqs', str),
        ('ssl_ca_certs', str),

        # See: http://www.rediscookbook.org/multiple_databases.html
        ('db', int),
    ])
    """
    Keys (and value type converters) that we allow to proxy from the jobboard
    configuration into the redis client (used to configure the redis client
    internals if no explicit client is provided via the ``client`` keyword
    argument).

    See: http://redis-py.readthedocs.org/en/latest/#redis.Redis

    See: https://github.com/andymccurdy/redis-py/blob/2.10.3/redis/client.py
    """

    #: Postfix (combined with job key) used to make a jobs owner key.
    OWNED_POSTFIX = b".owned"

    #: Postfix (combined with job key) used to make a jobs last modified key.
    LAST_MODIFIED_POSTFIX = b".last_modified"

    #: Default namespace for keys when none is provided.
    DEFAULT_NAMESPACE = b'taskflow'

    MIN_REDIS_VERSION = (2, 6)
    """
    Minimum redis version this backend requires.

    This version is required since we need the built-in server-side lua
    scripting support that is included in 2.6 and newer.
    """

    NAMESPACE_SEP = b':'
    """
    Separator that is used to combine a key with the namespace (to get
    the **actual** key that will be used).
    """

    KEY_PIECE_SEP = b'.'
    """
    Separator that is used to combine a bunch of key pieces together (to get
    the **actual** key that will be used).
    """

    #: Expected lua response status field when call is ok.
    SCRIPT_STATUS_OK = "ok"

    #: Expected lua response status field when call is **not** ok.
    SCRIPT_STATUS_ERROR = "error"

    #: Expected lua script error response when the owner is not as expected.
    SCRIPT_NOT_EXPECTED_OWNER = "Not expected owner!"

    #: Expected lua script error response when the owner is not findable.
    SCRIPT_UNKNOWN_OWNER = "Unknown owner!"

    #: Expected lua script error response when the job is not findable.
    SCRIPT_UNKNOWN_JOB = "Unknown job!"

    #: Expected lua script error response when the job is already claimed.
    SCRIPT_ALREADY_CLAIMED = "Job already claimed!"

    SCRIPT_TEMPLATES = {
        'consume': """
-- Extract *all* the variables (so we can easily know what they are)...
local owner_key = KEYS[1]
local listings_key = KEYS[2]
local last_modified_key = KEYS[3]

local expected_owner = ARGV[1]
local job_key = ARGV[2]
local result = {}
if redis.call("hexists", listings_key, job_key) == 1 then
    if redis.call("exists", owner_key) == 1 then
        local owner = redis.call("get", owner_key)
        if owner ~= expected_owner then
            result["status"] = "${error}"
            result["reason"] = "${not_expected_owner}"
            result["owner"] = owner
        else
            -- The order is important here, delete the owner first (and if
            -- that blows up, the job data will still exist so it can be
            -- worked on again, instead of the reverse)...
            redis.call("del", owner_key, last_modified_key)
            redis.call("hdel", listings_key, job_key)
            result["status"] = "${ok}"
        end
    else
        result["status"] = "${error}"
        result["reason"] = "${unknown_owner}"
    end
else
    result["status"] = "${error}"
    result["reason"] = "${unknown_job}"
end
return cmsgpack.pack(result)
""",
        'claim': """
local function apply_ttl(key, ms_expiry)
    if ms_expiry ~= nil then
        redis.call("pexpire", key, ms_expiry)
    end
end

-- Extract *all* the variables (so we can easily know what they are)...
local owner_key = KEYS[1]
local listings_key = KEYS[2]
local last_modified_key = KEYS[3]

local expected_owner = ARGV[1]
local job_key = ARGV[2]
local last_modified_blob = ARGV[3]

-- If this is non-numeric (which it may be) this becomes nil
local ms_expiry = nil
if ARGV[4] ~= "none" then
    ms_expiry = tonumber(ARGV[4])
end
local result = {}
if redis.call("hexists", listings_key, job_key) == 1 then
    if redis.call("exists", owner_key) == 1 then
        local owner = redis.call("get", owner_key)
        if owner == expected_owner then
            -- Owner is the same, leave it alone...
            redis.call("set", last_modified_key, last_modified_blob)
            apply_ttl(owner_key, ms_expiry)
        end
        result["status"] = "${error}"
        result["reason"] = "${already_claimed}"
        result["owner"] = owner
    else
        redis.call("set", owner_key, expected_owner)
        redis.call("set", last_modified_key, last_modified_blob)
        apply_ttl(owner_key, ms_expiry)
        result["status"] = "${ok}"
    end
else
    result["status"] = "${error}"
    result["reason"] = "${unknown_job}"
end
return cmsgpack.pack(result)
""",
        'abandon': """
-- Extract *all* the variables (so we can easily know what they are)...
local owner_key = KEYS[1]
local listings_key = KEYS[2]
local last_modified_key = KEYS[3]

local expected_owner = ARGV[1]
local job_key = ARGV[2]
local last_modified_blob = ARGV[3]
local result = {}
if redis.call("hexists", listings_key, job_key) == 1 then
    if redis.call("exists", owner_key) == 1 then
        local owner = redis.call("get", owner_key)
        if owner ~= expected_owner then
            result["status"] = "${error}"
            result["reason"] = "${not_expected_owner}"
            result["owner"] = owner
        else
            redis.call("del", owner_key)
            redis.call("set", last_modified_key, last_modified_blob)
            result["status"] = "${ok}"
        end
    else
        result["status"] = "${error}"
        result["reason"] = "${unknown_owner}"
    end
else
    result["status"] = "${error}"
    result["reason"] = "${unknown_job}"
end
return cmsgpack.pack(result)
""",
        'trash': """
-- Extract *all* the variables (so we can easily know what they are)...
local owner_key = KEYS[1]
local listings_key = KEYS[2]
local last_modified_key = KEYS[3]
local trash_listings_key = KEYS[4]

local expected_owner = ARGV[1]
local job_key = ARGV[2]
local last_modified_blob = ARGV[3]
local result = {}
if redis.call("hexists", listings_key, job_key) == 1 then
    local raw_posting = redis.call("hget", listings_key, job_key)
    if redis.call("exists", owner_key) == 1 then
        local owner = redis.call("get", owner_key)
        if owner ~= expected_owner then
            result["status"] = "${error}"
            result["reason"] = "${not_expected_owner}"
            result["owner"] = owner
        else
            -- This ordering is important (try to first move the value
            -- and only if that works do we try to do any deletions)...
            redis.call("hset", trash_listings_key, job_key, raw_posting)
            redis.call("set", last_modified_key, last_modified_blob)
            redis.call("del", owner_key)
            redis.call("hdel", listings_key, job_key)
            result["status"] = "${ok}"
        end
    else
        result["status"] = "${error}"
        result["reason"] = "${unknown_owner}"
    end
else
    result["status"] = "${error}"
    result["reason"] = "${unknown_job}"
end
return cmsgpack.pack(result)
""",
    }
    """`Lua`_ **template** scripts that will be used by various methods (they
    are turned into real scripts and loaded on call into the :func:`.connect`
    method).

    Some things to note:

    - The lua script is ran serially, so when this runs no other command will
      be mutating the backend (and redis also ensures that no other script
      will be running) so atomicity of these scripts are  guaranteed by redis.

    - Transactions were considered (and even mostly implemented) but
      ultimately rejected since redis does not support rollbacks and
      transactions can **not** be interdependent (later operations can **not**
      depend on the results of earlier operations). Both of these issues limit
      our ability to correctly report errors (with useful messages) and to
      maintain consistency under failure/contention (due to the inability to
      rollback). A third and final blow to using transactions was to
      correctly use them we would have to set a watch on a *very* contentious
      key (the listings key) which would under load cause clients to retry more
      often then would be desired (this also increases network load, CPU
      cycles used, transactions failures triggered and so on).

    - Partial transaction execution is possible due to pre/post ``EXEC``
      failures (and the lack of rollback makes this worse).

    So overall after thinking, it seemed like having little lua scripts
    was not that bad (even if it is somewhat convoluted) due to the above and
    public mentioned issues with transactions. In general using lua scripts
    for this purpose seems to be somewhat common practice and it solves the
    issues that came up when transactions were considered & implemented.

    Some links about redis (and redis + lua) that may be useful to look over:

    - `Atomicity of scripts`_
    - `Scripting and transactions`_
    - `Why redis does not support rollbacks`_
    - `Intro to lua for redis programmers`_
    - `Five key takeaways for developing with redis`_
    - `Everything you always wanted to know about redis`_ (slides)

    .. _Lua: http://www.lua.org/
    .. _Atomicity of scripts: http://redis.io/commands/eval#atomicity-of-\
                              scripts
    .. _Scripting and transactions: http://redis.io/topics/transactions#redis-\
                                    scripting-and-transactions
    .. _Why redis does not support rollbacks: http://redis.io/topics/transa\
                                              ctions#why-redis-does-not-suppo\
                                              rt-roll-backs
    .. _Intro to lua for redis programmers: http://www.redisgreen.net/blog/int\
                                            ro-to-lua-for-redis-programmers
    .. _Five key takeaways for developing with redis: https://redislabs.com/bl\
                                                      og/5-key-takeaways-fo\
                                                      r-developing-with-redis
    .. _Everything you always wanted to know about redis: http://www.slidesh
                                                          are.net/carlosabal\
                                                          de/everything-you-a\
                                                          lways-wanted-to-\
                                                          know-about-redis-b\
                                                          ut-were-afraid-to-ask
    """

    @classmethod
    def _parse_sentinel(cls, sentinel):
        # IPv6 (eg. [::1]:6379 )
        match = re.search(r'^\[(\S+)\]:(\d+)$', sentinel)
        if match:
            return (match[1], int(match[2]))
        # IPv4 or hostname (eg. 127.0.0.1:6379 or localhost:6379)
        match = re.search(r'^(\S+):(\d+)$', sentinel)
        if match:
            return (match[1], int(match[2]))
        raise ValueError('Malformed sentinel server format')

    @classmethod
    def _make_client(cls, conf):
        client_conf = {}
        for key, value_type_converter in cls.CLIENT_CONF_TRANSFERS:
            if key in conf:
                if value_type_converter is not None:
                    client_conf[key] = value_type_converter(conf[key])
                else:
                    client_conf[key] = conf[key]
        if conf.get('sentinel') is not None:
            sentinels = [(client_conf.pop('host'), client_conf.pop('port'))]
            for fallback in conf.get('sentinel_fallbacks', []):
                sentinels.append(cls._parse_sentinel(fallback))
            s = sentinel.Sentinel(sentinels,
                                  sentinel_kwargs=conf.get('sentinel_kwargs'),
                                  **client_conf)
            return s.master_for(conf['sentinel'])
        else:
            return ru.RedisClient(**client_conf)

    def __init__(self, name, conf,
                 client=None, persistence=None):
        super(RedisJobBoard, self).__init__(name, conf)
        self._closed = True
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = self._make_client(self._conf)
            # NOTE(harlowja): This client should not work until connected...
            self._client.close()
            self._owns_client = True
        self._namespace = self._conf.get('namespace', self.DEFAULT_NAMESPACE)
        self._open_close_lock = threading.RLock()
        # Redis server version connected to + scripts (populated on connect).
        self._redis_version = None
        self._scripts = {}
        # The backend to load the full logbooks from, since what is sent over
        # the data connection is only the logbook uuid and name, and not the
        # full logbook.
        self._persistence = persistence

    def join(self, key_piece, *more_key_pieces):
        """Create and return a namespaced key from many segments.

        NOTE(harlowja): all pieces that are text/unicode are converted into
        their binary equivalent (if they are already binary no conversion
        takes place) before being joined (as redis expects binary keys and not
        unicode/text ones).
        """
        namespace_pieces = []
        if self._namespace is not None:
            namespace_pieces = [self._namespace, self.NAMESPACE_SEP]
        else:
            namespace_pieces = []
        key_pieces = [key_piece]
        if more_key_pieces:
            key_pieces.extend(more_key_pieces)
        for i in range(0, len(namespace_pieces)):
            namespace_pieces[i] = misc.binary_encode(namespace_pieces[i])
        for i in range(0, len(key_pieces)):
            key_pieces[i] = misc.binary_encode(key_pieces[i])
        namespace = b"".join(namespace_pieces)
        key = self.KEY_PIECE_SEP.join(key_pieces)
        return namespace + key

    @property
    def namespace(self):
        """The namespace all keys will be prefixed with (or none)."""
        return self._namespace

    @misc.cachedproperty
    def trash_key(self):
        """Key where a hash will be stored with trashed jobs in it."""
        return self.join(b"trash")

    @misc.cachedproperty
    def sequence_key(self):
        """Key where a integer will be stored (used to sequence jobs)."""
        return self.join(b"sequence")

    @misc.cachedproperty
    def listings_key(self):
        """Key where a hash will be stored with active jobs in it."""
        return self.join(b"listings")

    @property
    def job_count(self):
        with _translate_failures():
            return self._client.hlen(self.listings_key)

    @property
    def connected(self):
        return not self._closed

    @fasteners.locked(lock='_open_close_lock')
    def connect(self):
        self.close()
        if self._owns_client:
            self._client = self._make_client(self._conf)
        with _translate_failures():
            # The client maintains a connection pool, so do a ping and
            # if that works then assume the connection works, which may or
            # may not be continuously maintained (if the server dies
            # at a later time, we will become aware of that when the next
            # op occurs).
            self._client.ping()
            is_new_enough, redis_version = ru.is_server_new_enough(
                self._client, self.MIN_REDIS_VERSION)
            if not is_new_enough:
                wanted_version = ".".join([str(p)
                                           for p in self.MIN_REDIS_VERSION])
                if redis_version:
                    raise exc.JobFailure("Redis version %s or greater is"
                                         " required (version %s is to"
                                         " old)" % (wanted_version,
                                                    redis_version))
                else:
                    raise exc.JobFailure("Redis version %s or greater is"
                                         " required" % (wanted_version))
            else:
                self._redis_version = redis_version
                script_params = {
                    # Status field values.
                    'ok': self.SCRIPT_STATUS_OK,
                    'error': self.SCRIPT_STATUS_ERROR,

                    # Known error reasons (when status field is error).
                    'not_expected_owner': self.SCRIPT_NOT_EXPECTED_OWNER,
                    'unknown_owner': self.SCRIPT_UNKNOWN_OWNER,
                    'unknown_job': self.SCRIPT_UNKNOWN_JOB,
                    'already_claimed': self.SCRIPT_ALREADY_CLAIMED,
                }
                prepared_scripts = {}
                for n, raw_script_tpl in self.SCRIPT_TEMPLATES.items():
                    script_tpl = string.Template(raw_script_tpl)
                    script_blob = script_tpl.substitute(**script_params)
                    script = self._client.register_script(script_blob)
                    prepared_scripts[n] = script
                self._scripts.update(prepared_scripts)
                self._closed = False

    @fasteners.locked(lock='_open_close_lock')
    def close(self):
        if self._owns_client:
            self._client.close()
        self._scripts.clear()
        self._redis_version = None
        self._closed = True

    @staticmethod
    def _dumps(obj):
        try:
            return msgpackutils.dumps(obj)
        except (msgpack.PackException, ValueError):
            # TODO(harlowja): remove direct msgpack exception access when
            # oslo.utils provides easy access to the underlying msgpack
            # pack/unpack exceptions..
            exc.raise_with_cause(exc.JobFailure,
                                 "Failed to serialize object to"
                                 " msgpack blob")

    @staticmethod
    def _loads(blob, root_types=(dict,)):
        try:
            return misc.decode_msgpack(blob, root_types=root_types)
        except (msgpack.UnpackException, ValueError):
            # TODO(harlowja): remove direct msgpack exception access when
            # oslo.utils provides easy access to the underlying msgpack
            # pack/unpack exceptions..
            exc.raise_with_cause(exc.JobFailure,
                                 "Failed to deserialize object from"
                                 " msgpack blob (of length %s)" % len(blob))

    _decode_owner = staticmethod(misc.binary_decode)

    _encode_owner = staticmethod(misc.binary_encode)

    def find_owner(self, job):
        owner_key = self.join(job.key + self.OWNED_POSTFIX)
        with _translate_failures():
            raw_owner = self._client.get(owner_key)
            return self._decode_owner(raw_owner)

    def post(self, name, book=None, details=None,
             priority=base.JobPriority.NORMAL):
        job_uuid = uuidutils.generate_uuid()
        job_priority = base.JobPriority.convert(priority)
        posting = base.format_posting(job_uuid, name,
                                      created_on=timeutils.utcnow(),
                                      book=book, details=details,
                                      priority=job_priority)
        with _translate_failures():
            sequence = self._client.incr(self.sequence_key)
            posting.update({
                'sequence': sequence,
            })
        with _translate_failures():
            raw_posting = self._dumps(posting)
            raw_job_uuid = job_uuid.encode('latin-1')
            was_posted = bool(self._client.hsetnx(self.listings_key,
                                                  raw_job_uuid, raw_posting))
            if not was_posted:
                raise exc.JobFailure("New job located at '%s[%s]' could not"
                                     " be posted" % (self.listings_key,
                                                     raw_job_uuid))
            else:
                return RedisJob(self, name, sequence, raw_job_uuid,
                                uuid=job_uuid, details=details,
                                created_on=posting['created_on'],
                                book=book, book_data=posting.get('book'),
                                backend=self._persistence,
                                priority=job_priority)

    def wait(self, timeout=None, initial_delay=0.005,
             max_delay=1.0, sleep_func=time.sleep):
        if initial_delay > max_delay:
            raise ValueError("Initial delay %s must be less than or equal"
                             " to the provided max delay %s"
                             % (initial_delay, max_delay))
        # This does a spin-loop that backs off by doubling the delay
        # up to the provided max-delay. In the future we could try having
        # a secondary client connected into redis pubsub and use that
        # instead, but for now this is simpler.
        w = timeutils.StopWatch(duration=timeout)
        w.start()
        delay = initial_delay
        while True:
            jc = self.job_count
            if jc > 0:
                curr_jobs = self._fetch_jobs()
                if curr_jobs:
                    return base.JobBoardIterator(
                        self, LOG,
                        board_fetch_func=lambda ensure_fresh: curr_jobs)
            if w.expired():
                raise exc.NotFound("Expired waiting for jobs to"
                                   " arrive; waited %s seconds"
                                   % w.elapsed())
            else:
                remaining = w.leftover(return_none=True)
                if remaining is not None:
                    delay = min(delay * 2, remaining, max_delay)
                else:
                    delay = min(delay * 2, max_delay)
                sleep_func(delay)

    def _fetch_jobs(self):
        with _translate_failures():
            raw_postings = self._client.hgetall(self.listings_key)
        postings = []
        for raw_job_key, raw_posting in raw_postings.items():
            try:
                job_data = self._loads(raw_posting)
                try:
                    job_priority = job_data['priority']
                    job_priority = base.JobPriority.convert(job_priority)
                except KeyError:
                    job_priority = base.JobPriority.NORMAL
                job_created_on = job_data['created_on']
                job_uuid = job_data['uuid']
                job_name = job_data['name']
                job_sequence_id = job_data['sequence']
                job_details = job_data.get('details', {})
            except (ValueError, TypeError, KeyError, exc.JobFailure):
                with excutils.save_and_reraise_exception():
                    LOG.warning("Incorrectly formatted job data found at"
                                " key: %s[%s]", self.listings_key,
                                raw_job_key, exc_info=True)
                    LOG.info("Deleting invalid job data at key: %s[%s]",
                             self.listings_key, raw_job_key)
                    self._client.hdel(self.listings_key, raw_job_key)
            else:
                postings.append(RedisJob(self, job_name, job_sequence_id,
                                         raw_job_key, uuid=job_uuid,
                                         details=job_details,
                                         created_on=job_created_on,
                                         book_data=job_data.get('book'),
                                         backend=self._persistence,
                                         priority=job_priority))
        return sorted(postings, reverse=True)

    def iterjobs(self, only_unclaimed=False, ensure_fresh=False):
        return base.JobBoardIterator(
            self, LOG, only_unclaimed=only_unclaimed,
            ensure_fresh=ensure_fresh,
            board_fetch_func=lambda ensure_fresh: self._fetch_jobs())

    def register_entity(self, entity):
        # Will implement a redis jobboard conductor register later
        pass

    @base.check_who
    def consume(self, job, who):
        script = self._get_script('consume')
        with _translate_failures():
            raw_who = self._encode_owner(who)
            raw_result = script(keys=[job.owner_key, self.listings_key,
                                      job.last_modified_key],
                                args=[raw_who, job.key])
            result = self._loads(raw_result)
        status = result['status']
        if status != self.SCRIPT_STATUS_OK:
            reason = result.get('reason')
            if reason == self.SCRIPT_UNKNOWN_JOB:
                raise exc.NotFound("Job %s not found to be"
                                   " consumed" % (job.uuid))
            elif reason == self.SCRIPT_UNKNOWN_OWNER:
                raise exc.NotFound("Can not consume job %s"
                                   " which we can not determine"
                                   " the owner of" % (job.uuid))
            elif reason == self.SCRIPT_NOT_EXPECTED_OWNER:
                raw_owner = result.get('owner')
                if raw_owner:
                    owner = self._decode_owner(raw_owner)
                    raise exc.JobFailure("Can not consume job %s"
                                         " which is not owned by %s (it is"
                                         " actively owned by %s)"
                                         % (job.uuid, who, owner))
                else:
                    raise exc.JobFailure("Can not consume job %s"
                                         " which is not owned by %s"
                                         % (job.uuid, who))
            else:
                raise exc.JobFailure("Failure to consume job %s,"
                                     " unknown internal error (reason=%s)"
                                     % (job.uuid, reason))

    @base.check_who
    def claim(self, job, who, expiry=None):
        if expiry is None:
            # On the lua side none doesn't translate to nil so we have
            # do to this string conversion to make sure that we can tell
            # the difference.
            ms_expiry = "none"
        else:
            ms_expiry = int(expiry * 1000.0)
            if ms_expiry <= 0:
                raise ValueError("Provided expiry (when converted to"
                                 " milliseconds) must be greater"
                                 " than zero instead of %s" % (expiry))
        script = self._get_script('claim')
        with _translate_failures():
            raw_who = self._encode_owner(who)
            raw_result = script(keys=[job.owner_key, self.listings_key,
                                      job.last_modified_key],
                                args=[raw_who, job.key,
                                      # NOTE(harlowja): we need to send this
                                      # in as a blob (even if it's not
                                      # set/used), since the format can not
                                      # currently be created in lua...
                                      self._dumps(timeutils.utcnow()),
                                      ms_expiry])
            result = self._loads(raw_result)
        status = result['status']
        if status != self.SCRIPT_STATUS_OK:
            reason = result.get('reason')
            if reason == self.SCRIPT_UNKNOWN_JOB:
                raise exc.NotFound("Job %s not found to be"
                                   " claimed" % (job.uuid))
            elif reason == self.SCRIPT_ALREADY_CLAIMED:
                raw_owner = result.get('owner')
                if raw_owner:
                    owner = self._decode_owner(raw_owner)
                    raise exc.UnclaimableJob("Job %s already"
                                             " claimed by %s"
                                             % (job.uuid, owner))
                else:
                    raise exc.UnclaimableJob("Job %s already"
                                             " claimed" % (job.uuid))
            else:
                raise exc.JobFailure("Failure to claim job %s,"
                                     " unknown internal error (reason=%s)"
                                     % (job.uuid, reason))

    @base.check_who
    def abandon(self, job, who):
        script = self._get_script('abandon')
        with _translate_failures():
            raw_who = self._encode_owner(who)
            raw_result = script(keys=[job.owner_key, self.listings_key,
                                      job.last_modified_key],
                                args=[raw_who, job.key,
                                      self._dumps(timeutils.utcnow())])
            result = self._loads(raw_result)
        status = result.get('status')
        if status != self.SCRIPT_STATUS_OK:
            reason = result.get('reason')
            if reason == self.SCRIPT_UNKNOWN_JOB:
                raise exc.NotFound("Job %s not found to be"
                                   " abandoned" % (job.uuid))
            elif reason == self.SCRIPT_UNKNOWN_OWNER:
                raise exc.NotFound("Can not abandon job %s"
                                   " which we can not determine"
                                   " the owner of" % (job.uuid))
            elif reason == self.SCRIPT_NOT_EXPECTED_OWNER:
                raw_owner = result.get('owner')
                if raw_owner:
                    owner = self._decode_owner(raw_owner)
                    raise exc.JobFailure("Can not abandon job %s"
                                         " which is not owned by %s (it is"
                                         " actively owned by %s)"
                                         % (job.uuid, who, owner))
                else:
                    raise exc.JobFailure("Can not abandon job %s"
                                         " which is not owned by %s"
                                         % (job.uuid, who))
            else:
                raise exc.JobFailure("Failure to abandon job %s,"
                                     " unknown internal"
                                     " error (status=%s, reason=%s)"
                                     % (job.uuid, status, reason))

    def _get_script(self, name):
        try:
            return self._scripts[name]
        except KeyError:
            exc.raise_with_cause(exc.NotFound,
                                 "Can not access %s script (has this"
                                 " board been connected?)" % name)

    @base.check_who
    def trash(self, job, who):
        script = self._get_script('trash')
        with _translate_failures():
            raw_who = self._encode_owner(who)
            raw_result = script(keys=[job.owner_key, self.listings_key,
                                      job.last_modified_key, self.trash_key],
                                args=[raw_who, job.key,
                                      self._dumps(timeutils.utcnow())])
            result = self._loads(raw_result)
        status = result['status']
        if status != self.SCRIPT_STATUS_OK:
            reason = result.get('reason')
            if reason == self.SCRIPT_UNKNOWN_JOB:
                raise exc.NotFound("Job %s not found to be"
                                   " trashed" % (job.uuid))
            elif reason == self.SCRIPT_UNKNOWN_OWNER:
                raise exc.NotFound("Can not trash job %s"
                                   " which we can not determine"
                                   " the owner of" % (job.uuid))
            elif reason == self.SCRIPT_NOT_EXPECTED_OWNER:
                raw_owner = result.get('owner')
                if raw_owner:
                    owner = self._decode_owner(raw_owner)
                    raise exc.JobFailure("Can not trash job %s"
                                         " which is not owned by %s (it is"
                                         " actively owned by %s)"
                                         % (job.uuid, who, owner))
                else:
                    raise exc.JobFailure("Can not trash job %s"
                                         " which is not owned by %s"
                                         % (job.uuid, who))
            else:
                raise exc.JobFailure("Failure to trash job %s,"
                                     " unknown internal error (reason=%s)"
                                     % (job.uuid, reason))
