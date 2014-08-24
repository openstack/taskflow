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

from concurrent import futures as _futures
from concurrent.futures import process as _process
from concurrent.futures import thread as _thread

try:
    from eventlet.green import threading as greenthreading
    from eventlet import greenpool
    from eventlet import patcher as greenpatcher
    from eventlet import queue as greenqueue
    EVENTLET_AVAILABLE = True
except ImportError:
    EVENTLET_AVAILABLE = False

from taskflow.utils import threading_utils as tu


# NOTE(harlowja): Allows for simpler access to this type...
Future = _futures.Future


class ThreadPoolExecutor(_thread.ThreadPoolExecutor):
    """Executor that uses a thread pool to execute calls asynchronously.

    See: https://docs.python.org/dev/library/concurrent.futures.html
    """
    def __init__(self, max_workers=None):
        if max_workers is None:
            max_workers = tu.get_optimal_thread_count()
        super(ThreadPoolExecutor, self).__init__(max_workers=max_workers)
        if self._max_workers <= 0:
            raise ValueError("Max workers must be greater than zero")

    @property
    def alive(self):
        return not self._shutdown


class ProcessPoolExecutor(_process.ProcessPoolExecutor):
    """Executor that uses a process pool to execute calls asynchronously.

    See: https://docs.python.org/dev/library/concurrent.futures.html
    """
    def __init__(self, max_workers=None):
        if max_workers is None:
            max_workers = tu.get_optimal_thread_count()
        super(ProcessPoolExecutor, self).__init__(max_workers=max_workers)
        if self._max_workers <= 0:
            raise ValueError("Max workers must be greater than zero")

    @property
    def alive(self):
        return not self._shutdown_thread


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


class SynchronousExecutor(_futures.Executor):
    """Executor that uses the caller to execute calls synchronously.

    This provides an interface to a caller that looks like an executor but
    will execute the calls inside the caller thread instead of executing it
    in a external process/thread for when this type of functionality is
    useful to provide...
    """

    def __init__(self):
        self._shutoff = False

    @property
    def alive(self):
        return not self._shutoff

    def shutdown(self, wait=True):
        self._shutoff = True

    def submit(self, fn, *args, **kwargs):
        if self._shutoff:
            raise RuntimeError('Can not schedule new futures'
                               ' after being shutdown')
        f = Future()
        runner = _WorkItem(f, fn, args, kwargs)
        runner.run()
        return f


class _GreenWorker(object):
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


class GreenFuture(Future):
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


class GreenThreadPoolExecutor(_futures.Executor):
    """Executor that uses a green thread pool to execute calls asynchronously.

    See: https://docs.python.org/dev/library/concurrent.futures.html
    and http://eventlet.net/doc/modules/greenpool.html for information on
    how this works.
    """

    def __init__(self, max_workers=1000):
        assert EVENTLET_AVAILABLE, 'eventlet is needed to use a green executor'
        if max_workers <= 0:
            raise ValueError("Max workers must be greater than zero")
        self._max_workers = max_workers
        self._pool = greenpool.GreenPool(self._max_workers)
        self._delayed_work = greenqueue.Queue()
        self._shutdown_lock = greenthreading.Lock()
        self._shutdown = False

    @property
    def alive(self):
        return not self._shutdown

    def submit(self, fn, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('Can not schedule new futures'
                                   ' after being shutdown')
            f = GreenFuture()
            work = _WorkItem(f, fn, args, kwargs)
            if not self._spin_up(work):
                self._delayed_work.put(work)
            return f

    def _spin_up(self, work):
        alive = self._pool.running() + self._pool.waiting()
        if alive < self._max_workers:
            self._pool.spawn_n(_GreenWorker(self, work, self._delayed_work))
            return True
        return False

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            if not self._shutdown:
                self._shutdown = True
                shutoff = True
            else:
                shutoff = False
        if wait and shutoff:
            self._pool.waitall()
            self._delayed_work.join()
