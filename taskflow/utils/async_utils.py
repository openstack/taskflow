# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

from concurrent import futures as _futures
from concurrent.futures import _base
from oslo_utils import importutils

greenthreading = importutils.try_import('eventlet.green.threading')

from taskflow.types import futures
from taskflow.utils import eventlet_utils as eu


_DONE_STATES = frozenset([
    _base.CANCELLED_AND_NOTIFIED,
    _base.FINISHED,
])


def make_completed_future(result, exception=False):
    """Make a future completed with a given result."""
    future = futures.Future()
    if exception:
        future.set_exception(result)
    else:
        future.set_result(result)
    return future


def wait_for_any(fs, timeout=None):
    """Wait for one of the futures to complete.

    Works correctly with both green and non-green futures (but not both
    together, since this can't be guaranteed to avoid dead-lock due to how
    the waiting implementations are different when green threads are being
    used).

    Returns pair (done futures, not done futures).
    """
    green_fs = sum(1 for f in fs if isinstance(f, futures.GreenFuture))
    if not green_fs:
        return _futures.wait(fs,
                             timeout=timeout,
                             return_when=_futures.FIRST_COMPLETED)
    else:
        non_green_fs = len(fs) - green_fs
        if non_green_fs:
            raise RuntimeError("Can not wait on %s green futures and %s"
                               " non-green futures in the same `wait_for_any`"
                               " call" % (green_fs, non_green_fs))
        else:
            return _wait_for_any_green(fs, timeout=timeout)


class _GreenWaiter(object):
    """Provides the event that wait_for_any() blocks on."""
    def __init__(self):
        self.event = greenthreading.Event()

    def add_result(self, future):
        self.event.set()

    def add_exception(self, future):
        self.event.set()

    def add_cancelled(self, future):
        self.event.set()


def _partition_futures(fs):
    done = set()
    not_done = set()
    for f in fs:
        if f._state in _DONE_STATES:
            done.add(f)
        else:
            not_done.add(f)
    return done, not_done


def _wait_for_any_green(fs, timeout=None):
    eu.check_for_eventlet(RuntimeError('Eventlet is needed to wait on'
                                       ' green futures'))

    with _base._AcquireFutures(fs):
        done, not_done = _partition_futures(fs)
        if done:
            return _base.DoneAndNotDoneFutures(done, not_done)
        waiter = _GreenWaiter()
        for f in fs:
            f._waiters.append(waiter)

    waiter.event.wait(timeout)
    for f in fs:
        f._waiters.remove(waiter)

    with _base._AcquireFutures(fs):
        done, not_done = _partition_futures(fs)
        return _base.DoneAndNotDoneFutures(done, not_done)
