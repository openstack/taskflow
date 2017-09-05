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

import six


class Timeout(object):
    """An object which represents a timeout.

    This object has the ability to be interrupted before the actual timeout
    is reached.
    """
    def __init__(self, value, event_factory=threading.Event):
        if value < 0:
            raise ValueError("Timeout value must be greater or"
                             " equal to zero and not '%s'" % (value))
        self._value = value
        self._event = event_factory()

    @property
    def value(self):
        """Immutable value of the internally used timeout."""
        return self._value

    def interrupt(self):
        """Forcefully set the timeout (releases any waiters)."""
        self._event.set()

    def is_stopped(self):
        """Returns if the timeout has been interrupted."""
        return self._event.is_set()

    def wait(self):
        """Block current thread (up to timeout) and wait until interrupted."""
        self._event.wait(self._value)

    def reset(self):
        """Reset so that interruption (and waiting) can happen again."""
        self._event.clear()


def convert_to_timeout(value=None, default_value=None,
                       event_factory=threading.Event):
    """Converts a given value to a timeout instance (and returns it).

    Does nothing if the value provided is already a timeout instance.
    """
    if value is None:
        value = default_value
    if isinstance(value, (int, float) + six.string_types):
        return Timeout(float(value), event_factory=event_factory)
    elif isinstance(value, Timeout):
        return value
    else:
        raise ValueError("Invalid timeout literal '%s'" % (value))
