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

import collections
import contextlib
import errno
import os
import threading
import time

from oslo_utils import importutils
import six

from taskflow import logging
from taskflow.utils import misc

# Used for the reader-writer lock get the right thread 'hack' (needed below).
eventlet = importutils.try_import('eventlet')
eventlet_patcher = importutils.try_import('eventlet.patcher')

LOG = logging.getLogger(__name__)


@contextlib.contextmanager
def try_lock(lock):
    """Attempts to acquire a lock, and auto releases if acquired (on exit)."""
    # NOTE(harlowja): the keyword argument for 'blocking' does not work
    # in py2.x and only is fixed in py3.x (this adjustment is documented
    # and/or debated in http://bugs.python.org/issue10789); so we'll just
    # stick to the format that works in both (oddly the keyword argument
    # works in py2.x but only with reentrant locks).
    was_locked = lock.acquire(False)
    try:
        yield was_locked
    finally:
        if was_locked:
            lock.release()


def locked(*args, **kwargs):
    """A locking decorator.

    It will look for a provided attribute (typically a lock or a list
    of locks) on the first argument of the function decorated (typically this
    is the 'self' object) and before executing the decorated function it
    activates the given lock or list of locks as a context manager,
    automatically releasing that lock on exit.

    NOTE(harlowja): if no attribute is provided then by default the attribute
    named '_lock' is looked for in the instance object this decorator is
    attached to.

    NOTE(harlowja): when we get the wrapt module approved we can address the
    correctness of this decorator with regards to classmethods, to keep sanity
    and correctness it is recommended to avoid using this on classmethods, once
    https://review.openstack.org/#/c/94754/ is merged this will be refactored
    and that use-case can be provided in a correct manner.
    """

    def decorator(f):
        attr_name = kwargs.get('lock', '_lock')

        @six.wraps(f)
        def wrapper(self, *args, **kwargs):
            attr_value = getattr(self, attr_name)
            if isinstance(attr_value, (tuple, list)):
                lock = MultiLock(attr_value)
            else:
                lock = attr_value
            with lock:
                return f(self, *args, **kwargs)

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


class ReaderWriterLock(object):
    """A reader/writer lock.

    This lock allows for simultaneous readers to exist but only one writer
    to exist for use-cases where it is useful to have such types of locks.

    Currently a reader can not escalate its read lock to a write lock and
    a writer can not acquire a read lock while it owns or is waiting on
    the write lock.

    In the future these restrictions may be relaxed.

    This can be eventually removed if http://bugs.python.org/issue8800 ever
    gets accepted into the python standard threading library...
    """
    WRITER = 'w'
    READER = 'r'

    @staticmethod
    def _fetch_current_thread_functor():
        # Until https://github.com/eventlet/eventlet/issues/172 is resolved
        # or addressed we have to use complicated workaround to get a object
        # that will not be recycled; the usage of threading.current_thread()
        # doesn't appear to currently be monkey patched and therefore isn't
        # reliable to use (and breaks badly when used as all threads share
        # the same current_thread() object)...
        if eventlet is not None and eventlet_patcher is not None:
            if eventlet_patcher.is_monkey_patched('thread'):
                return lambda: eventlet.getcurrent()
        return lambda: threading.current_thread()

    def __init__(self):
        self._writer = None
        self._pending_writers = collections.deque()
        self._readers = collections.deque()
        self._cond = threading.Condition()
        self._current_thread = self._fetch_current_thread_functor()

    @property
    def has_pending_writers(self):
        """Returns if there are writers waiting to become the *one* writer."""
        self._cond.acquire()
        try:
            return bool(self._pending_writers)
        finally:
            self._cond.release()

    def is_writer(self, check_pending=True):
        """Returns if the caller is the active writer or a pending writer."""
        self._cond.acquire()
        try:
            me = self._current_thread()
            if self._writer is not None and self._writer == me:
                return True
            if check_pending:
                return me in self._pending_writers
            else:
                return False
        finally:
            self._cond.release()

    @property
    def owner(self):
        """Returns whether the lock is locked by a writer or reader."""
        self._cond.acquire()
        try:
            if self._writer is not None:
                return self.WRITER
            if self._readers:
                return self.READER
            return None
        finally:
            self._cond.release()

    def is_reader(self):
        """Returns if the caller is one of the readers."""
        self._cond.acquire()
        try:
            return self._current_thread() in self._readers
        finally:
            self._cond.release()

    @contextlib.contextmanager
    def read_lock(self):
        """Context manager that grants a read lock.

        Will wait until no active or pending writers.

        Raises a RuntimeError if an active or pending writer tries to acquire
        a read lock.
        """
        me = self._current_thread()
        if self.is_writer():
            raise RuntimeError("Writer %s can not acquire a read lock"
                               " while holding/waiting for the write lock"
                               % me)
        self._cond.acquire()
        try:
            while True:
                # No active writer; we are good to become a reader.
                if self._writer is None:
                    self._readers.append(me)
                    break
                # An active writer; guess we have to wait.
                self._cond.wait()
        finally:
            self._cond.release()
        try:
            yield self
        finally:
            # I am no longer a reader, remove *one* occurrence of myself.
            # If the current thread acquired two read locks, then it will
            # still have to remove that other read lock; this allows for
            # basic reentrancy to be possible.
            self._cond.acquire()
            try:
                self._readers.remove(me)
                self._cond.notify_all()
            finally:
                self._cond.release()

    @contextlib.contextmanager
    def write_lock(self):
        """Context manager that grants a write lock.

        Will wait until no active readers. Blocks readers after acquiring.

        Raises a RuntimeError if an active reader attempts to acquire a lock.
        """
        me = self._current_thread()
        if self.is_reader():
            raise RuntimeError("Reader %s to writer privilege"
                               " escalation not allowed" % me)
        if self.is_writer(check_pending=False):
            # Already the writer; this allows for basic reentrancy.
            yield self
        else:
            self._cond.acquire()
            try:
                self._pending_writers.append(me)
                while True:
                    # No readers, and no active writer, am I next??
                    if len(self._readers) == 0 and self._writer is None:
                        if self._pending_writers[0] == me:
                            self._writer = self._pending_writers.popleft()
                            break
                    self._cond.wait()
            finally:
                self._cond.release()
            try:
                yield self
            finally:
                self._cond.acquire()
                try:
                    self._writer = None
                    self._cond.notify_all()
                finally:
                    self._cond.release()


class MultiLock(object):
    """A class which attempts to obtain & release many locks at once.

    It is typically useful as a context manager around many locks (instead of
    having to nest individual lock context managers, which can become pretty
    awkward looking).

    NOTE(harlowja): The locks that will be obtained will be in the order the
    locks are given in the constructor, they will be acquired in order and
    released in reverse order (so ordering matters).
    """

    def __init__(self, locks):
        if not isinstance(locks, tuple):
            locks = tuple(locks)
        if len(locks) <= 0:
            raise ValueError("Zero locks requested")
        self._locks = locks
        self._local = threading.local()

    @property
    def _lock_stacks(self):
        # This is weird, but this is how thread locals work (in that each
        # thread will need to check if it has already created the attribute and
        # if not then create it and set it to the thread local variable...)
        #
        # This isn't done in the constructor since the constructor is only
        # activated by one of the many threads that could use this object,
        # and that means that the attribute will only exist for that one
        # thread.
        try:
            return self._local.stacks
        except AttributeError:
            self._local.stacks = []
            return self._local.stacks

    def __enter__(self):
        return self.acquire()

    @property
    def obtained(self):
        """Returns how many locks were last acquired/obtained."""
        try:
            return self._lock_stacks[-1]
        except IndexError:
            return 0

    def __len__(self):
        return len(self._locks)

    def acquire(self):
        """This will attempt to acquire all the locks given in the constructor.

        If all the locks can not be acquired (and say only X of Y locks could
        be acquired then this will return false to signify that not all the
        locks were able to be acquired, you can later use the :attr:`.obtained`
        property to determine how many were obtained during the last
        acquisition attempt).

        NOTE(harlowja): When not all locks were acquired it is still required
        to release since under partial acquisition the acquired locks
        must still be released. For example if 4 out of 5 locks were acquired
        this will return false, but the user **must** still release those
        other 4 to avoid causing locking issues...
        """
        gotten = 0
        for lock in self._locks:
            try:
                acked = lock.acquire()
            except (threading.ThreadError, RuntimeError) as e:
                # If we have already gotten some set of the desired locks
                # make sure we track that and ensure that we later release them
                # instead of losing them.
                if gotten:
                    self._lock_stacks.append(gotten)
                raise threading.ThreadError(
                    "Unable to acquire lock %s/%s due to '%s'"
                    % (gotten + 1, len(self._locks), e))
            else:
                if not acked:
                    break
                else:
                    gotten += 1
        if gotten:
            self._lock_stacks.append(gotten)
        return gotten == len(self._locks)

    def __exit__(self, type, value, traceback):
        self.release()

    def release(self):
        """Releases any past acquired locks (partial or otherwise)."""
        height = len(self._lock_stacks)
        if not height:
            # Raise the same error type as the threading.Lock raises so that
            # it matches the behavior of the built-in class (it's odd though
            # that the threading.RLock raises a runtime error on this same
            # method instead...)
            raise threading.ThreadError('Release attempted on unlocked lock')
        # Cleans off one level of the stack (this is done so that if there
        # are multiple __enter__() and __exit__() pairs active that this will
        # only remove one level (the last one), and not all levels...
        leftover = self._lock_stacks[-1]
        while leftover:
            lock = self._locks[leftover - 1]
            try:
                lock.release()
            except (threading.ThreadError, RuntimeError) as e:
                # Ensure that we adjust the lock stack under failure so that
                # if release is attempted again that we do not try to release
                # the locks we already released...
                self._lock_stacks[-1] = leftover
                raise threading.ThreadError(
                    "Unable to release lock %s/%s due to '%s'"
                    % (leftover, len(self._locks), e))
            else:
                leftover -= 1
        # At the end only clear it off, so that under partial failure we don't
        # lose any locks...
        self._lock_stacks.pop()


class _InterProcessLock(object):
    """An interprocess locking implementation.

    This is a lock implementation which allows multiple locks, working around
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
        self.lockfile = None
        self.fname = name

    def acquire(self):
        basedir = os.path.dirname(self.fname)

        if not os.path.exists(basedir):
            misc.ensure_tree(basedir)
            LOG.debug('Created lock path: %s', basedir)

        self.lockfile = open(self.fname, 'w')

        while True:
            try:
                # Using non-blocking locks since green threads are not
                # patched to deal with blocking locking calls.
                # Also upon reading the MSDN docs for locking(), it seems
                # to have a laughable 10 attempts "blocking" mechanism.
                self.trylock()
                LOG.debug('Got file lock "%s"', self.fname)
                return True
            except IOError as e:
                if e.errno in (errno.EACCES, errno.EAGAIN):
                    # external locks synchronise things like iptables
                    # updates - give it some time to prevent busy spinning
                    time.sleep(0.01)
                else:
                    raise threading.ThreadError("Unable to acquire lock on"
                                                " `%(filename)s` due to"
                                                " %(exception)s" %
                                                {
                                                    'filename': self.fname,
                                                    'exception': e,
                                                })

    def __enter__(self):
        self.acquire()
        return self

    def release(self):
        try:
            self.unlock()
            self.lockfile.close()
            LOG.debug('Released file lock "%s"', self.fname)
        except IOError:
            LOG.exception("Could not release the acquired lock `%s`",
                          self.fname)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def exists(self):
        return os.path.exists(self.fname)

    def trylock(self):
        raise NotImplementedError()

    def unlock(self):
        raise NotImplementedError()


class _WindowsLock(_InterProcessLock):
    def trylock(self):
        msvcrt.locking(self.lockfile.fileno(), msvcrt.LK_NBLCK, 1)

    def unlock(self):
        msvcrt.locking(self.lockfile.fileno(), msvcrt.LK_UNLCK, 1)


class _PosixLock(_InterProcessLock):
    def trylock(self):
        fcntl.lockf(self.lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def unlock(self):
        fcntl.lockf(self.lockfile, fcntl.LOCK_UN)


if os.name == 'nt':
    import msvcrt
    InterProcessLock = _WindowsLock
else:
    import fcntl
    InterProcessLock = _PosixLock
