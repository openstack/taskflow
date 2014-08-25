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

import logging

from concurrent import futures

try:
    from eventlet.green import threading as greenthreading
    from eventlet import greenpool
    from eventlet import patcher as greenpatcher
    from eventlet import queue as greenqueue
    EVENTLET_AVAILABLE = True
except ImportError:
    EVENTLET_AVAILABLE = False


from taskflow.utils import lock_utils

LOG = logging.getLogger(__name__)

_DONE_STATES = frozenset([
    futures._base.CANCELLED_AND_NOTIFIED,
    futures._base.FINISHED,
])


class _WorkItem(object):
    def __init__(self, future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        if not self.future.set_running_or_notify_cancel():
            return
        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException as e:
            self.future.set_exception(e)
        else:
            self.future.set_result(result)


class _Worker(object):
    def __init__(self, executor, work, work_queue):
        self.executor = executor
        self.work = work
        self.work_queue = work_queue

    def __call__(self):
        # Run our main piece of work.
        try:
            self.work.run()
        finally:
            # Consume any delayed work before finishing (this is how we finish
            # work that was to big for the pool size, but needs to be finished
            # no matter).
            while True:
                try:
                    w = self.work_queue.get_nowait()
                except greenqueue.Empty:
                    break
                else:
                    try:
                        w.run()
                    finally:
                        self.work_queue.task_done()


class GreenFuture(futures.Future):
    def __init__(self):
        super(GreenFuture, self).__init__()
        assert EVENTLET_AVAILABLE, 'eventlet is needed to use a green future'
        # NOTE(harlowja): replace the built-in condition with a greenthread
        # compatible one so that when getting the result of this future the
        # functions will correctly yield to eventlet. If this is not done then
        # waiting on the future never actually causes the greenthreads to run
        # and thus you wait for infinity.
        if not greenpatcher.is_monkey_patched('threading'):
            self._condition = greenthreading.Condition()


class GreenExecutor(futures.Executor):
    """A greenthread backed executor."""

    def __init__(self, max_workers=1000):
        assert EVENTLET_AVAILABLE, 'eventlet is needed to use a green executor'
        self._max_workers = int(max_workers)
        if self._max_workers <= 0:
            raise ValueError('Max workers must be greater than zero')
        self._pool = greenpool.GreenPool(self._max_workers)
        self._delayed_work = greenqueue.Queue()
        self._shutdown_lock = greenthreading.Lock()
        self._shutdown = False
        self._workers_created = 0

    @property
    def workers_created(self):
        return self._workers_created

    @property
    def amount_delayed(self):
        return self._delayed_work.qsize()

    @property
    def alive(self):
        return not self._shutdown

    @lock_utils.locked(lock='_shutdown_lock')
    def submit(self, fn, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = GreenFuture()
        work = _WorkItem(f, fn, args, kwargs)
        if not self._spin_up(work):
            self._delayed_work.put(work)
        return f

    def _spin_up(self, work):
        alive = self._pool.running() + self._pool.waiting()
        if alive < self._max_workers:
            self._pool.spawn_n(_Worker(self, work, self._delayed_work))
            self._workers_created += 1
            return True
        return False

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
        if wait:
            self._pool.waitall()
            # NOTE(harlowja): Fixed in eventlet 0.15 (remove when able to use)
            if not self._delayed_work.empty():
                self._delayed_work.join()


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
    """Partitions the input futures into done and not done lists."""
    done = set()
    not_done = set()
    for f in fs:
        if f._state in _DONE_STATES:
            done.add(f)
        else:
            not_done.add(f)
    return (done, not_done)


def wait_for_any(fs, timeout=None):
    assert EVENTLET_AVAILABLE, ('eventlet is needed to wait on green futures')
    with futures._base._AcquireFutures(fs):
        (done, not_done) = _partition_futures(fs)
        if done:
            return (done, not_done)
        waiter = _GreenWaiter()
        for f in fs:
            f._waiters.append(waiter)
    waiter.event.wait(timeout)
    for f in fs:
        f._waiters.remove(waiter)
    with futures._base._AcquireFutures(fs):
        return _partition_futures(fs)
