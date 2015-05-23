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

import contextlib
import threading

import six

from taskflow import logging
from taskflow.utils import misc

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

    NOTE(harlowja): if no attribute name is provided then by default the
    attribute named '_lock' is looked for (this attribute is expected to be
    the lock/list of locks object/s) in the instance object this decorator
    is attached to.
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
        for left in misc.countdown_iter(self._lock_stacks[-1]):
            lock_idx = left - 1
            lock = self._locks[lock_idx]
            try:
                lock.release()
            except (threading.ThreadError, RuntimeError) as e:
                # Ensure that we adjust the lock stack under failure so that
                # if release is attempted again that we do not try to release
                # the locks we already released...
                self._lock_stacks[-1] = left
                raise threading.ThreadError(
                    "Unable to release lock %s/%s due to '%s'"
                    % (left, len(self._locks), e))
        # At the end only clear it off, so that under partial failure we don't
        # lose any locks...
        self._lock_stacks.pop()
