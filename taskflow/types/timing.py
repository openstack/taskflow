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

from oslo_utils import reflection

from taskflow.utils import misc
from taskflow.utils import threading_utils

# Find a monotonic providing time (or fallback to using time.time()
# which isn't *always* accurate but will suffice).
_now = misc.find_monotonic(allow_time_time=True)


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


class Split(object):
    """A *immutable* stopwatch split.

    See: http://en.wikipedia.org/wiki/Stopwatch for what this is/represents.
    """

    __slots__ = ['_elapsed', '_length']

    def __init__(self, elapsed, length):
        self._elapsed = elapsed
        self._length = length

    @property
    def elapsed(self):
        """Duration from stopwatch start."""
        return self._elapsed

    @property
    def length(self):
        """Seconds from last split (or the elapsed time if no prior split)."""
        return self._length

    def __repr__(self):
        r = reflection.get_class_name(self, fully_qualified=False)
        r += "(elapsed=%s, length=%s)" % (self._elapsed, self._length)
        return r


class StopWatch(object):
    """A simple timer/stopwatch helper class.

    Inspired by: apache-commons-lang java stopwatch.

    Not thread-safe (when a single watch is mutated by multiple threads at
    the same time). Thread-safe when used by a single thread (not shared) or
    when operations are performed in a thread-safe manner on these objects by
    wrapping those operations with locks.
    """
    _STARTED = 'STARTED'
    _STOPPED = 'STOPPED'

    """
    Class variables that should only be used for testing purposes only...
    """
    _now_offset = None
    _now_override = None

    def __init__(self, duration=None):
        if duration is not None:
            if duration < 0:
                raise ValueError("Duration must be >= 0 and not %s" % duration)
            self._duration = duration
        else:
            self._duration = None
        self._started_at = None
        self._stopped_at = None
        self._state = None
        self._splits = []

    def start(self):
        """Starts the watch (if not already started).

        NOTE(harlowja): resets any splits previously captured (if any).
        """
        if self._state == self._STARTED:
            return self
        self._started_at = self._now()
        self._stopped_at = None
        self._state = self._STARTED
        self._splits = []
        return self

    @property
    def splits(self):
        """Accessor to all/any splits that have been captured."""
        return tuple(self._splits)

    def split(self):
        """Captures a split/elapsed since start time (and doesn't stop)."""
        if self._state == self._STARTED:
            elapsed = self.elapsed()
            if self._splits:
                length = self._delta_seconds(self._splits[-1].elapsed, elapsed)
            else:
                length = elapsed
            self._splits.append(Split(elapsed, length))
            return self._splits[-1]
        else:
            raise RuntimeError("Can not create a split time of a stopwatch"
                               " if it has not been started")

    def restart(self):
        """Restarts the watch from a started/stopped state."""
        if self._state == self._STARTED:
            self.stop()
        self.start()
        return self

    @classmethod
    def clear_overrides(cls):
        """Clears all overrides/offsets.

        **Only to be used for testing (affects all watch instances).**
        """
        cls._now_override = None
        cls._now_offset = None

    @classmethod
    def set_offset_override(cls, offset):
        """Sets a offset that is applied to each time fetch.

        **Only to be used for testing (affects all watch instances).**
        """
        cls._now_offset = offset

    @classmethod
    def advance_time_seconds(cls, offset):
        """Advances/sets a offset that is applied to each time fetch.

        NOTE(harlowja): if a previous offset exists (not ``None``) then this
        offset will be added onto the existing one (if you want to reset
        the offset completely use the :meth:`.set_offset_override`
        method instead).

        **Only to be used for testing (affects all watch instances).**
        """
        if cls._now_offset is None:
            cls.set_offset_override(offset)
        else:
            cls.set_offset_override(cls._now_offset + offset)

    @classmethod
    def set_now_override(cls, now=None):
        """Sets time override to use (if none, then current time is fetched).

        NOTE(harlowja): if a list/tuple is provided then the first element of
        the list will be used (and removed) each time a time fetch occurs (once
        it becomes empty the override/s will no longer be applied). If a
        numeric value is provided then it will be used (and never removed
        until the override(s) are cleared via the :meth:`.clear_overrides`
        method).

        **Only to be used for testing (affects all watch instances).**
        """
        if isinstance(now, (list, tuple)):
            cls._now_override = list(now)
        else:
            if now is None:
                now = _now()
            cls._now_override = now

    @staticmethod
    def _delta_seconds(earlier, later):
        return max(0.0, later - earlier)

    @classmethod
    def _now(cls):
        if cls._now_override is not None:
            if isinstance(cls._now_override, list):
                try:
                    now = cls._now_override.pop(0)
                except IndexError:
                    now = _now()
            else:
                now = cls._now_override
        else:
            now = _now()
        if cls._now_offset is not None:
            now = now + cls._now_offset
        return now

    def elapsed(self, maximum=None):
        """Returns how many seconds have elapsed."""
        if self._state not in (self._STOPPED, self._STARTED):
            raise RuntimeError("Can not get the elapsed time of a stopwatch"
                               " if it has not been started/stopped")
        if self._state == self._STOPPED:
            elapsed = self._delta_seconds(self._started_at, self._stopped_at)
        else:
            elapsed = self._delta_seconds(self._started_at, self._now())
        if maximum is not None and elapsed > maximum:
            elapsed = max(0.0, maximum)
        return elapsed

    def __enter__(self):
        """Starts the watch."""
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        """Stops the watch (ignoring errors if stop fails)."""
        try:
            self.stop()
        except RuntimeError:
            pass

    def leftover(self, return_none=False):
        """Returns how many seconds are left until the watch expires.

        :param return_none: when ``True`` instead of raising a ``RuntimeError``
                            when no duration has been set this call will
                            return ``None`` instead
        :type return_none: boolean
        :returns: how many seconds left until the watch expires
        :rtype: number
        """
        if self._state != self._STARTED:
            raise RuntimeError("Can not get the leftover time of a stopwatch"
                               " that has not been started")
        if self._duration is None:
            if not return_none:
                raise RuntimeError("Can not get the leftover time of a watch"
                                   " that has no duration")
            else:
                return None
        return max(0.0, self._duration - self.elapsed())

    def expired(self):
        """Returns if the watch has expired (ie, duration provided elapsed).

        :returns: if the watch has expired
        :rtype: boolean
        """
        if self._state is None:
            raise RuntimeError("Can not check if a stopwatch has expired"
                               " if it has not been started/stopped")
        if self._duration is None:
            return False
        if self.elapsed() > self._duration:
            return True
        return False

    def resume(self):
        """Resumes the watch from a stopped state."""
        if self._state == self._STOPPED:
            self._state = self._STARTED
            return self
        else:
            raise RuntimeError("Can not resume a stopwatch that has not been"
                               " stopped")

    def stop(self):
        """Stops the watch."""
        if self._state == self._STOPPED:
            return self
        if self._state != self._STARTED:
            raise RuntimeError("Can not stop a stopwatch that has not been"
                               " started")
        self._stopped_at = self._now()
        self._state = self._STOPPED
        return self
