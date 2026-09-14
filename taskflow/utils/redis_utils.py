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

import enum
import functools

from oslo_utils import versionutils
import redis
from redis import exceptions as redis_exceptions


def _raise_on_closed(meth):

    @functools.wraps(meth)
    def wrapper(self, *args, **kwargs):
        if self.closed:
            raise redis_exceptions.ConnectionError(
                "Connection has been closed"
            )
        return meth(self, *args, **kwargs)

    return wrapper


class RedisClient(redis.Redis):
    """A redis client that can be closed (and raises on-usage after closed).

    TODO(harlowja): if https://github.com/andymccurdy/redis-py/issues/613 ever
    gets resolved or merged or other then we can likely remove this.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closed = False

    def close(self):
        self.closed = True
        self.connection_pool.disconnect()

    execute_command = _raise_on_closed(redis.Redis.execute_command)
    transaction = _raise_on_closed(redis.Redis.transaction)
    pubsub = _raise_on_closed(redis.Redis.pubsub)


class UnknownExpire(enum.IntEnum):
    """Non-expiry (not ttls) results return from :func:`.get_expiry`.

    See: http://redis.io/commands/ttl or http://redis.io/commands/pttl
    """

    #: The command returns ``-1`` if the key exists but has no associated
    #: expire.
    DOES_NOT_EXPIRE = -1

    #: The command returns ``-2`` if the key does not exist.
    KEY_NOT_FOUND = -2


DOES_NOT_EXPIRE = UnknownExpire.DOES_NOT_EXPIRE
KEY_NOT_FOUND = UnknownExpire.KEY_NOT_FOUND


def get_expiry(client, key, prior_version=None):
    """Gets an expiry for a key (using **best** determined ttl method)."""
    result = client.pttl(key)
    try:
        return UnknownExpire(result)
    except ValueError:
        return result / 1000.0


def apply_expiry(client, key, expiry, prior_version=None):
    """Applies an expiry to a key (using **best** determined expiry method)."""
    ms_expiry = expiry * 1000.0
    ms_expiry = max(0, int(ms_expiry))
    result = client.pexpire(key, ms_expiry)
    return bool(result)


def is_server_new_enough(
    client, min_version, default=False, prior_version=None
):
    """Checks if a client is attached to a new enough redis server."""
    if not prior_version:
        try:
            server_info = client.info()
        except redis_exceptions.ResponseError:
            server_info = {}
        version_text = server_info.get('redis_version', '')
    else:
        version_text = prior_version

    version_pieces = tuple()
    if version_text:
        version_pieces = versionutils.convert_version_to_tuple(version_text)
    if not version_pieces:
        return (default, version_text)
    return (version_pieces >= min_version, version_text)
