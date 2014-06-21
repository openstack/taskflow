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

from taskflow.utils import misc


class Timeout(object):
    """An object which represents a timeout.

    This object has the ability to be interrupted before the actual timeout
    is reached.
    """
    def __init__(self, timeout):
        if timeout < 0:
            raise ValueError("Timeout must be >= 0 and not %s" % (timeout))
        self._timeout = timeout
        self._event = threading.Event()

    def interrupt(self):
        self._event.set()

    def is_stopped(self):
        return self._event.is_set()

    def wait(self):
        self._event.wait(self._timeout)

    def reset(self):
        self._event.clear()


class StopWatch(object):
    """A simple timer/stopwatch helper class.

    Inspired by: apache-commons-lang java stopwatch.

    Not thread-safe.
    """
    _STARTED = 'STARTED'
    _STOPPED = 'STOPPED'

    def __init__(self, duration=None):
        self._duration = duration
        self._started_at = None
        self._stopped_at = None
        self._state = None

    def start(self):
        if self._state == self._STARTED:
            return self
        self._started_at = misc.wallclock()
        self._stopped_at = None
        self._state = self._STARTED
        return self

    def elapsed(self):
        if self._state == self._STOPPED:
            return float(self._stopped_at - self._started_at)
        elif self._state == self._STARTED:
            return float(misc.wallclock() - self._started_at)
        else:
            raise RuntimeError("Can not get the elapsed time of an invalid"
                               " stopwatch")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        try:
            self.stop()
        except RuntimeError:
            pass
        # NOTE(harlowja): don't silence the exception.
        return False

    def leftover(self):
        if self._duration is None:
            raise RuntimeError("Can not get the leftover time of a watch that"
                               " has no duration")
        if self._state != self._STARTED:
            raise RuntimeError("Can not get the leftover time of a stopwatch"
                               " that has not been started")
        end_time = self._started_at + self._duration
        return max(0.0, end_time - misc.wallclock())

    def expired(self):
        if self._duration is None:
            return False
        if self.elapsed() > self._duration:
            return True
        return False

    def resume(self):
        if self._state == self._STOPPED:
            self._state = self._STARTED
            return self
        else:
            raise RuntimeError("Can not resume a stopwatch that has not been"
                               " stopped")

    def stop(self):
        if self._state == self._STOPPED:
            return self
        if self._state != self._STARTED:
            raise RuntimeError("Can not stop a stopwatch that has not been"
                               " started")
        self._stopped_at = misc.wallclock()
        self._state = self._STOPPED
        return self
