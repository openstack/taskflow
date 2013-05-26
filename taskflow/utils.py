# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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

import contextlib
import logging
import threading
import time

LOG = logging.getLogger(__name__)


def safe_attr(obj, name, default=None):
    return getattr(obj, name, default)


def await(check_functor, timeout=None):
    if timeout is not None:
        end_time = time.time() + max(0, timeout)
    else:
        end_time = None
    # Use the same/similar scheme that the python condition class uses.
    delay = 0.0005
    while not check_functor():
        time.sleep(delay)
        if end_time is not None:
            remaining = end_time - time.time()
            if remaining <= 0:
                return False
            delay = min(delay * 2, remaining, 0.05)
        else:
            delay = min(delay * 2, 0.05)
    return True


class RollbackAccumulator(object):
    """A utility class that can help in organizing 'undo' like code
    so that said code be rolled back on failure (automatically or manually)
    by activating rollback callables that were inserted during said codes
    progression."""

    def __init__(self):
        self._rollbacks = []

    def add(self, *callables):
        self._rollbacks.extend(callables)

    def reset(self):
        self._rollbacks = []

    def __len__(self):
        return len(self._rollbacks)

    def __iter__(self):
        # Rollbacks happen in the reverse order that they were added.
        return reversed(self._rollbacks)

    def __enter__(self):
        return self

    def rollback(self, cause):
        LOG.warn("Activating %s rollbacks due to %s.", len(self), cause)
        for (i, f) in enumerate(self):
            LOG.debug("Calling rollback %s: %s", i + 1, f)
            try:
                f(cause)
            except Exception:
                LOG.exception(("Failed rolling back %s: %s due "
                               "to inner exception."), i + 1, f)

    def __exit__(self, type, value, tb):
        if any((value, type, tb)):
            self.rollback(value)


class ReaderWriterLock(object):
    """A simple reader-writer lock.

    Several readers can hold the lock simultaneously, and only one writer.
    Write locks have priority over reads to prevent write starvation.

    Public domain @ http://majid.info/blog/a-reader-writer-lock-for-python/
    """

    def __init__(self):
        self.rwlock = 0
        self.writers_waiting = 0
        self.monitor = threading.Lock()
        self.readers_ok = threading.Condition(self.monitor)
        self.writers_ok = threading.Condition(self.monitor)

    @contextlib.contextmanager
    def acquire(self, read=True):
        """Acquire a read or write lock in a context manager."""
        try:
            if read:
                self.acquire_read()
            else:
                self.acquire_write()
            yield self
        finally:
            self.release()

    def acquire_read(self):
        """Acquire a read lock.

        Several threads can hold this typeof lock.
        It is exclusive with write locks."""

        self.monitor.acquire()
        while self.rwlock < 0 or self.writers_waiting:
            self.readers_ok.wait()
        self.rwlock += 1
        self.monitor.release()

    def acquire_write(self):
        """Acquire a write lock.

        Only one thread can hold this lock, and only when no read locks
        are also held."""

        self.monitor.acquire()
        while self.rwlock != 0:
            self.writers_waiting += 1
            self.writers_ok.wait()
            self.writers_waiting -= 1
        self.rwlock = -1
        self.monitor.release()

    def release(self):
        """Release a lock, whether read or write."""

        self.monitor.acquire()
        if self.rwlock < 0:
            self.rwlock = 0
        else:
            self.rwlock -= 1
        wake_writers = self.writers_waiting and self.rwlock == 0
        wake_readers = self.writers_waiting == 0
        self.monitor.release()
        if wake_writers:
            self.writers_ok.acquire()
            self.writers_ok.notify()
            self.writers_ok.release()
        elif wake_readers:
            self.readers_ok.acquire()
            self.readers_ok.notifyAll()
            self.readers_ok.release()


class LazyPluggable(object):
    """A pluggable backend loaded lazily based on some value."""

    def __init__(self, pivot, **backends):
        self.__backends = backends
        self.__pivot = pivot
        self.__backend = None

    def __get_backend(self):
        if not self.__backend:
            backend_name = 'sqlalchemy'
            backend = self.__backends[backend_name]
            if isinstance(backend, tuple):
                name = backend[0]
                fromlist = backend[1]
            else:
                name = backend
                fromlist = backend

            self.__backend = __import__(name, None, None, fromlist)
        return self.__backend

    def __getattr__(self, key):
        backend = self.__get_backend()
        return getattr(backend, key)

