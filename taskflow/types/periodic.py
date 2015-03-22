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

import heapq
import inspect

from debtcollector import removals
from oslo_utils import reflection
import six

from taskflow import logging
from taskflow.utils import misc
from taskflow.utils import threading_utils as tu

LOG = logging.getLogger(__name__)

# Find a monotonic providing time (or fallback to using time.time()
# which isn't *always* accurate but will suffice).
_now = misc.find_monotonic(allow_time_time=True)


def _check_attrs(obj):
    """Checks that a periodic function/method has all the expected attributes.

    This will return the expected attributes that were **not** found.
    """
    missing_attrs = []
    for a in ('_periodic', '_periodic_spacing', '_periodic_run_immediately'):
        if not hasattr(obj, a):
            missing_attrs.append(a)
    return missing_attrs


def periodic(spacing, run_immediately=True):
    """Tags a method/function as wanting/able to execute periodically.

    :param run_immediately: option to specify whether to run
                            immediately or not
    :type run_immediately: boolean
    """

    if spacing <= 0:
        raise ValueError("Periodicity/spacing must be greater than"
                         " zero instead of %s" % spacing)

    def wrapper(f):
        f._periodic = True
        f._periodic_spacing = spacing
        f._periodic_run_immediately = run_immediately

        @six.wraps(f)
        def decorator(*args, **kwargs):
            return f(*args, **kwargs)

        return decorator

    return wrapper


class _Schedule(object):
    """Internal heap-based structure that maintains the schedule/ordering."""

    def __init__(self):
        self._ordering = []

    def push(self, next_run, index):
        heapq.heappush(self._ordering, (next_run, index))

    def push_next(self, cb, index, now=None):
        if now is None:
            now = _now()
        self.push(now + cb._periodic_spacing, index)

    def __len__(self):
        return len(self._ordering)

    def pop(self):
        return heapq.heappop(self._ordering)


def _build(callables):
    schedule = _Schedule()
    now = None
    immediates = []
    # Reverse order is used since these are later popped off (and to
    # ensure the popping order is first -> last we need to append them
    # in the opposite ordering last -> first).
    for i, cb in misc.reverse_enumerate(callables):
        if cb._periodic_run_immediately:
            immediates.append(i)
        else:
            if now is None:
                now = _now()
            schedule.push_next(cb, i, now=now)
    return immediates, schedule


def _safe_call(cb, kind):
    try:
        cb()
    except Exception:
        LOG.warn("Failed to call %s '%r'", kind, cb, exc_info=True)


class PeriodicWorker(object):
    """Calls a collection of callables periodically (sleeping as needed...).

    NOTE(harlowja): typically the :py:meth:`.start` method is executed in a
    background thread so that the periodic callables are executed in
    the background/asynchronously (using the defined periods to determine
    when each is called).
    """

    @classmethod
    def create(cls, objects, exclude_hidden=True):
        """Automatically creates a worker by analyzing object(s) methods.

        Only picks up methods that have been tagged/decorated with
        the :py:func:`.periodic` decorator (does not match against private
        or protected methods unless explicitly requested to).
        """
        callables = []
        for obj in objects:
            for (name, member) in inspect.getmembers(obj):
                if name.startswith("_") and exclude_hidden:
                    continue
                if reflection.is_bound_method(member):
                    missing_attrs = _check_attrs(member)
                    if not missing_attrs:
                        callables.append(member)
        return cls(callables)

    @removals.removed_kwarg('tombstone', version="0.8", removal_version="?")
    def __init__(self, callables, tombstone=None):
        if tombstone is None:
            self._tombstone = tu.Event()
        else:
            self._tombstone = tombstone
        self._callables = []
        for i, cb in enumerate(callables, 1):
            if not six.callable(cb):
                raise ValueError("Periodic callback %s must be callable" % i)
            missing_attrs = _check_attrs(cb)
            if missing_attrs:
                raise ValueError("Periodic callback %s missing required"
                                 " attributes %s" % (i, missing_attrs))
            if cb._periodic:
                self._callables.append(cb)
        self._immediates, self._schedule = _build(self._callables)

    def __len__(self):
        return len(self._callables)

    def start(self):
        """Starts running (will not return until :py:meth:`.stop` is called).

        NOTE(harlowja): If this worker has no contained callables this raises
        a runtime error and does not run since it is impossible to periodically
        run nothing.
        """
        if not self._callables:
            raise RuntimeError("A periodic worker can not start"
                               " without any callables")
        while not self._tombstone.is_set():
            if self._immediates:
                # Run & schedule its next execution.
                index = self._immediates.pop()
                cb = self._callables[index]
                LOG.blather("Calling immediate '%r'", cb)
                _safe_call(cb, 'immediate')
                self._schedule.push_next(cb, index)
            else:
                # Figure out when we should run next (by selecting the
                # minimum item from the heap, where the minimum should be
                # the callable that needs to run next and has the lowest
                # next desired run time).
                now = _now()
                next_run, index = self._schedule.pop()
                when_next = next_run - now
                if when_next <= 0:
                    # Run & schedule its next execution.
                    cb = self._callables[index]
                    LOG.blather("Calling periodic '%r' (it runs every"
                                " %s seconds)", cb, cb._periodic_spacing)
                    _safe_call(cb, 'periodic')
                    self._schedule.push_next(cb, index, now=now)
                else:
                    # Gotta wait...
                    self._schedule.push(next_run, index)
                    self._tombstone.wait(when_next)

    def stop(self):
        """Sets the tombstone (this stops any further executions)."""
        self._tombstone.set()

    def reset(self):
        """Resets the tombstone and re-queues up any immediate executions."""
        self._tombstone.clear()
        self._immediates, self._schedule = _build(self._callables)
