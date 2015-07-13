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

from oslo_utils import timeutils


class Latch(object):
    """A class that ensures N-arrivals occur before unblocking.

    TODO(harlowja): replace with http://bugs.python.org/issue8777 when we no
    longer have to support python 2.6 or 2.7 and we can only support 3.2 or
    later.
    """

    def __init__(self, count):
        count = int(count)
        if count <= 0:
            raise ValueError("Count must be greater than zero")
        self._count = count
        self._cond = threading.Condition()

    @property
    def needed(self):
        """Returns how many decrements are needed before latch is released."""
        return max(0, self._count)

    def countdown(self):
        """Decrements the internal counter due to an arrival."""
        with self._cond:
            self._count -= 1
            if self._count <= 0:
                self._cond.notify_all()

    def wait(self, timeout=None):
        """Waits until the latch is released.

        :param timeout: wait until the timeout expires
        :type timeout: number
        :returns: true if the latch has been released before the
                  timeout expires otherwise false
        :rtype: boolean
        """
        watch = timeutils.StopWatch(duration=timeout)
        watch.start()
        with self._cond:
            while self._count > 0:
                if watch.expired():
                    return False
                else:
                    self._cond.wait(watch.leftover(return_none=True))
            return True
