# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

from oslo_utils import reflection
import six


class ExpiringCache(object):
    """Represents a thread-safe time-based expiring cache.

    NOTE(harlowja): the values in this cache must have a expired attribute that
    can be used to determine if the key and associated value has expired or if
    it has not.
    """

    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()

    def __setitem__(self, key, value):
        """Set a value in the cache."""
        with self._lock:
            self._data[key] = value

    def __len__(self):
        """Returns how many items are in this cache."""
        return len(self._data)

    def get(self, key, default=None):
        """Retrieve a value from the cache (returns default if not found)."""
        return self._data.get(key, default)

    def __getitem__(self, key):
        """Retrieve a value from the cache."""
        return self._data[key]

    def __delitem__(self, key):
        """Delete a key & value from the cache."""
        with self._lock:
            del self._data[key]

    def clear(self, on_cleared_callback=None):
        """Removes all keys & values from the cache."""
        cleared_items = []
        with self._lock:
            if on_cleared_callback is not None:
                cleared_items.extend(six.iteritems(self._data))
            self._data.clear()
        if on_cleared_callback is not None:
            arg_c = len(reflection.get_callable_args(on_cleared_callback))
            for (k, v) in cleared_items:
                if arg_c == 2:
                    on_cleared_callback(k, v)
                else:
                    on_cleared_callback(v)

    def cleanup(self, on_expired_callback=None):
        """Delete out-dated keys & values from the cache."""
        with self._lock:
            expired_values = [(k, v) for k, v in six.iteritems(self._data)
                              if v.expired]
            for (k, _v) in expired_values:
                del self._data[k]
        if on_expired_callback is not None:
            arg_c = len(reflection.get_callable_args(on_expired_callback))
            for (k, v) in expired_values:
                if arg_c == 2:
                    on_expired_callback(k, v)
                else:
                    on_expired_callback(v)
