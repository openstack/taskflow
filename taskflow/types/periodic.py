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

from oslo_utils import reflection
import six

from taskflow import logging
from taskflow.utils import misc
from taskflow.utils import threading_utils as tu

LOG = logging.getLogger(__name__)

# Find a monotonic providing time (or fallback to using time.time()
# which isn't *always* accurate but will suffice).
_now = misc.find_monotonic(allow_time_time=True)

# Attributes expected on periodic tagged/decorated functions or methods...
_PERIODIC_ATTRS = tuple([
    '_periodic',
    '_periodic_spacing',
    '_periodic_run_immediately',
])


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
                    consume = True
                    for attr_name in _PERIODIC_ATTRS:
                        if not hasattr(member, attr_name):
                            consume = False
                            break
                    if consume:
                        callables.append(member)
        return cls(callables)

    def __init__(self, callables, tombstone=None):
        if tombstone is None:
            self._tombstone = tu.Event()
        else:
            # Allows someone to share an event (if they so want to...)
            self._tombstone = tombstone
        almost_callables = list(callables)
        for cb in almost_callables:
            if not six.callable(cb):
                raise ValueError("Periodic callback must be callable")
            for attr_name in _PERIODIC_ATTRS:
                if not hasattr(cb, attr_name):
                    raise ValueError("Periodic callback missing required"
                                     " attribute '%s'" % attr_name)
        self._callables = tuple((cb, reflection.get_callable_name(cb))
                                for cb in almost_callables)
        self._schedule = []
        now = _now()
        for i, (cb, cb_name) in enumerate(self._callables):
            spacing = cb._periodic_spacing
            next_run = now + spacing
            heapq.heappush(self._schedule, (next_run, i))
        self._immediates = self._fetch_immediates(self._callables)

    def __len__(self):
        return len(self._callables)

    @staticmethod
    def _fetch_immediates(callables):
        immediates = []
        # Reverse order is used since these are later popped off (and to
        # ensure the popping order is first -> last we need to append them
        # in the opposite ordering last -> first).
        for (cb, cb_name) in reversed(callables):
            if cb._periodic_run_immediately:
                immediates.append((cb, cb_name))
        return immediates

    @staticmethod
    def _safe_call(cb, cb_name, kind='periodic'):
        try:
            cb()
        except Exception:
            LOG.warn("Failed to call %s callable '%s'",
                     kind, cb_name, exc_info=True)

    def start(self):
        """Starts running (will not stop/return until the tombstone is set).

        NOTE(harlowja): If this worker has no contained callables this raises
        a runtime error and does not run since it is impossible to periodically
        run nothing.
        """
        if not self._callables:
            raise RuntimeError("A periodic worker can not start"
                               " without any callables")
        while not self._tombstone.is_set():
            if self._immediates:
                cb, cb_name = self._immediates.pop()
                LOG.debug("Calling immediate callable '%s'", cb_name)
                self._safe_call(cb, cb_name, kind='immediate')
            else:
                # Figure out when we should run next (by selecting the
                # minimum item from the heap, where the minimum should be
                # the callable that needs to run next and has the lowest
                # next desired run time).
                now = _now()
                next_run, i = heapq.heappop(self._schedule)
                when_next = next_run - now
                if when_next <= 0:
                    cb, cb_name = self._callables[i]
                    spacing = cb._periodic_spacing
                    LOG.debug("Calling periodic callable '%s' (it runs every"
                              " %s seconds)", cb_name, spacing)
                    self._safe_call(cb, cb_name)
                    # Run again someday...
                    next_run = now + spacing
                    heapq.heappush(self._schedule, (next_run, i))
                else:
                    # Gotta wait...
                    heapq.heappush(self._schedule, (next_run, i))
                    self._tombstone.wait(when_next)

    def stop(self):
        """Sets the tombstone (this stops any further executions)."""
        self._tombstone.set()

    def reset(self):
        """Resets the tombstone and re-queues up any immediate executions."""
        self._tombstone.clear()
        self._immediates = self._fetch_immediates(self._callables)
