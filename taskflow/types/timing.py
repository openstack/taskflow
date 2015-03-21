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

from oslo_utils import timeutils

from taskflow.utils import threading_utils

#: Moved to oslo.utils (just reference them from there until a later time).
Split = timeutils.Split

#: Moved to oslo.utils (just reference them from there until a later time).
StopWatch = timeutils.StopWatch


class Timeout(object):
    """An object which represents a timeout.

    This object has the ability to be interrupted before the actual timeout
    is reached.
    """
    def __init__(self, timeout):
        if timeout < 0:
            raise ValueError("Timeout must be >= 0 and not %s" % (timeout))
        self._timeout = timeout
        self._event = threading_utils.Event()

    def interrupt(self):
        self._event.set()

    def is_stopped(self):
        return self._event.is_set()

    def wait(self):
        self._event.wait(self._timeout)

    def reset(self):
        self._event.clear()
