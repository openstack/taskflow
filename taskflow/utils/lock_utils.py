# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 OpenStack Foundation.
# All Rights Reserved.
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

# This is a modified version of what was in oslo-incubator lockutils.py from
# commit 5039a610355e5265fb9fbd1f4023e8160750f32e but this one does not depend
# on oslo.cfg or the very large oslo-incubator oslo logging module (which also
# pulls in oslo.cfg) and is reduced to only what taskflow currently wants to
# use from that code.

import errno
import logging
import os
import threading
import time

from taskflow.utils import misc

LOG = logging.getLogger(__name__)
WAIT_TIME = 0.01


def locked(*args, **kwargs):
    """A decorator that looks for a given attribute (typically a lock or a list
    of locks) and before executing the decorated function uses the given lock
    or list of locks as a context manager, automatically releasing on exit.
    """

    def decorator(f):
        attr_name = kwargs.get('lock', '_lock')

        @misc.wraps(f)
        def wrapper(*args, **kwargs):
            lock = getattr(args[0], attr_name)
            if isinstance(lock, (tuple, list)):
                lock = MultiLock(locks=list(lock))
            with lock:
                return f(*args, **kwargs)

        return wrapper

    # This is needed to handle when the decorator has args or the decorator
    # doesn't have args, python is rather weird here...
    if kwargs or not args:
        return decorator
    else:
        if len(args) == 1:
            return decorator(args[0])
        else:
            return decorator


class MultiLock(object):
    """A class which can attempt to obtain many locks at once and release
    said locks when exiting.

    Useful as a context manager around many locks (instead of having to nest
    said individual context managers).
    """

    def __init__(self, locks):
        assert len(locks) > 0, "Zero locks requested"
        self._locks = locks
        self._locked = [False] * len(locks)

    def __enter__(self):

        def is_locked(lock):
            # NOTE(harlowja): the threading2 lock doesn't seem to have this
            # attribute, so thats why we are checking it existing first.
            if hasattr(lock, 'locked'):
                return lock.locked()
            return False

        for i in range(0, len(self._locked)):
            if self._locked[i] or is_locked(self._locks[i]):
                raise threading.ThreadError("Lock %s not previously released"
                                            % (i + 1))
            self._locked[i] = False
        for (i, lock) in enumerate(self._locks):
            self._locked[i] = lock.acquire()

    def __exit__(self, type, value, traceback):
        for (i, locked) in enumerate(self._locked):
            try:
                if locked:
                    self._locks[i].release()
                    self._locked[i] = False
            except threading.ThreadError:
                LOG.exception("Unable to release lock %s", i + 1)


class _InterProcessLock(object):
    """Lock implementation which allows multiple locks, working around
    issues like bugs.debian.org/cgi-bin/bugreport.cgi?bug=632857 and does
    not require any cleanup. Since the lock is always held on a file
    descriptor rather than outside of the process, the lock gets dropped
    automatically if the process crashes, even if __exit__ is not executed.

    There are no guarantees regarding usage by multiple green threads in a
    single process here. This lock works only between processes.

    Note these locks are released when the descriptor is closed, so it's not
    safe to close the file descriptor while another green thread holds the
    lock. Just opening and closing the lock file can break synchronisation,
    so lock files must be accessed only using this abstraction.
    """

    def __init__(self, name):
        self._lockfile = None
        self._fname = name

    @property
    def path(self):
        return self._fname

    def __enter__(self):
        self._lockfile = open(self.path, 'w')

        while True:
            try:
                # Using non-blocking locks since green threads are not
                # patched to deal with blocking locking calls.
                # Also upon reading the MSDN docs for locking(), it seems
                # to have a laughable 10 attempts "blocking" mechanism.
                self.trylock()
                return self
            except IOError as e:
                if e.errno in (errno.EACCES, errno.EAGAIN):
                    time.sleep(WAIT_TIME)
                else:
                    raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.unlock()
            self._lockfile.close()
        except IOError:
            LOG.exception("Could not release the acquired lock `%s`",
                          self.path)

    def trylock(self):
        raise NotImplementedError()

    def unlock(self):
        raise NotImplementedError()


class _WindowsLock(_InterProcessLock):
    def trylock(self):
        msvcrt.locking(self._lockfile.fileno(), msvcrt.LK_NBLCK, 1)

    def unlock(self):
        msvcrt.locking(self._lockfile.fileno(), msvcrt.LK_UNLCK, 1)


class _PosixLock(_InterProcessLock):
    def trylock(self):
        fcntl.lockf(self._lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def unlock(self):
        fcntl.lockf(self._lockfile, fcntl.LOCK_UN)


if os.name == 'nt':
    import msvcrt
    InterProcessLock = _WindowsLock
else:
    import fcntl
    InterProcessLock = _PosixLock
