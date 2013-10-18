# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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
import threading

from eventlet.green import threading as gthreading

from eventlet import greenpool
from eventlet import patcher
from eventlet import queue

from concurrent import futures

from taskflow.utils import lock_utils

LOG = logging.getLogger(__name__)

# NOTE(harlowja): this object signals to threads that they should stop
# working and rest in peace.
_TOMBSTONE = object()


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
    def __init__(self, executor, work_queue, worker_id):
        self.executor = executor
        self.work_queue = work_queue
        self.worker_id = worker_id

    def __call__(self):
        try:
            while True:
                work = self.work_queue.get(block=True)
                if work is _TOMBSTONE:
                    # NOTE(harlowja): give notice to other workers (this is
                    # basically a chain of tombstone calls that will cause all
                    # the workers on the queue to eventually shut-down).
                    self.work_queue.put(_TOMBSTONE)
                    break
                else:
                    work.run()
        except BaseException:
            LOG.critical("Exception in worker %s of '%s'",
                         self.worker_id, self.executor, exc_info=True)


class _GreenFuture(futures.Future):
    def __init__(self):
        super(_GreenFuture, self).__init__()
        # NOTE(harlowja): replace the built-in condition with a greenthread
        # compatible one so that when getting the result of this future the
        # functions will correctly yield to eventlet. If this is not done then
        # waiting on the future never actually causes the greenthreads to run
        # and thus you wait for infinity.
        if not patcher.is_monkey_patched('threading'):
            self._condition = gthreading.Condition()


class GreenExecutor(futures.Executor):
    """A greenthread backed executor."""

    def __init__(self, max_workers=1000):
        assert int(max_workers) > 0, 'Max workers must be greater than zero'
        self._max_workers = int(max_workers)
        self._pool = greenpool.GreenPool(self._max_workers)
        self._work_queue = queue.LightQueue()
        self._shutdown_lock = threading.RLock()
        self._shutdown = False

    @lock_utils.locked(lock='_shutdown_lock')
    def submit(self, fn, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError('cannot schedule new futures after shutdown')
        f = _GreenFuture()
        w = _WorkItem(f, fn, args, kwargs)
        self._work_queue.put(w)
        # Spin up any new workers (since they are spun up on demand and
        # not at executor initialization).
        self._spin_up()
        return f

    def _spin_up(self):
        cur_am = (self._pool.running() + self._pool.waiting())
        if cur_am < self._max_workers and cur_am < self._work_queue.qsize():
            # Spin up a new worker to do the work as we are behind.
            worker = _Worker(self, self._work_queue, cur_am + 1)
            self._pool.spawn(worker)

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            self._work_queue.put(_TOMBSTONE)
        if wait:
            self._pool.waitall()
