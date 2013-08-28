# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#    Copyright (C) 2013 Rackspace Hosting All Rights Reserved.
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

import collections
import contextlib
import copy
import inspect
import logging
import sys
import threading
import threading2
import time
import types

from distutils import version

from taskflow.openstack.common import uuidutils

TASK_FACTORY_ATTRIBUTE = '_TaskFlow_task_factory'
LOG = logging.getLogger(__name__)


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


def get_callable_name(function):
    """Generate a name from callable

    Tries to do the best to guess fully qualified callable name.
    """
    im_class = getattr(function, 'im_class', None)
    if im_class is not None:
        if im_class is type:
            # this is bound class method
            im_class = function.im_self
        parts = (im_class.__module__, im_class.__name__,
                 function.__name__)
    elif isinstance(function, types.FunctionType):
        parts = (function.__module__, function.__name__)
    else:
        im_class = type(function)
        if im_class is type:
            im_class = function
        parts = (im_class.__module__, im_class.__name__)
    return '.'.join(parts)


def is_bound_method(method):
    return getattr(method, 'im_self', None) is not None


def get_required_callable_args(function):
    """Get names of argument required by callable"""

    if isinstance(function, type):
        bound = True
        function = function.__init__
    elif isinstance(function, (types.FunctionType, types.MethodType)):
        bound = is_bound_method(function)
        function = getattr(function, '__wrapped__', function)
    else:
        function = function.__call__
        bound = is_bound_method(function)

    argspec = inspect.getargspec(function)
    f_args = argspec.args
    if argspec.defaults:
        f_args = f_args[:-len(argspec.defaults)]
    if bound:
        f_args = f_args[1:]
    return f_args


def get_task_version(task):
    """Gets a tasks *string* version, whether it is a task object/function."""
    task_version = getattr(task, 'version')
    if isinstance(task_version, (list, tuple)):
        task_version = '.'.join(str(item) for item in task_version)
    if task_version is not None and not isinstance(task_version, basestring):
        task_version = str(task_version)
    return task_version


def is_version_compatible(version_1, version_2):
    """Checks for major version compatibility of two *string" versions."""
    try:
        version_1_tmp = version.StrictVersion(version_1)
        version_2_tmp = version.StrictVersion(version_2)
    except ValueError:
        version_1_tmp = version.LooseVersion(version_1)
        version_2_tmp = version.LooseVersion(version_2)
    version_1 = version_1_tmp
    version_2 = version_2_tmp
    if version_1 == version_2 or version_1.version[0] == version_2.version[0]:
        return True
    return False


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

        for i in xrange(0, len(self._locked)):
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


class CountDownLatch(object):
    """Similar in concept to the java count down latch."""

    def __init__(self, count=0):
        self.count = count
        self.lock = threading.Condition()

    def countDown(self):
        with self.lock:
            self.count -= 1
            if self.count <= 0:
                self.lock.notifyAll()

    def await(self, timeout=None):
        end_time = None
        if timeout is not None:
            timeout = max(0, timeout)
            end_time = time.time() + timeout
        time_up = False
        with self.lock:
            while True:
                # Stop waiting on these 2 conditions.
                if time_up or self.count <= 0:
                    break
                # Was this a spurious wakeup or did we really end??
                self.lock.wait(timeout=timeout)
                if end_time is not None:
                    if time.time() >= end_time:
                        time_up = True
                    else:
                        # Reduce the timeout so that we don't wait extra time
                        # over what we initially were requested to.
                        timeout = end_time - time.time()
            return self.count <= 0


class LastFedIter(object):
    """An iterator which yields back the first item and then yields back
    results from the provided iterator.
    """

    def __init__(self, first, rest_itr):
        self.first = first
        self.rest_itr = rest_itr

    def __iter__(self):
        yield self.first
        for i in self.rest_itr:
            yield i


class ThreadGroupExecutor(object):
    """A simple thread executor that spins up new threads (or greenthreads) for
    each task to be completed (no pool limit is enforced).

    TODO(harlowja): Likely if we use the more advanced executors that come with
    the concurrent.futures library we can just get rid of this.
    """

    def __init__(self, daemonize=True):
        self._threads = []
        self._group = threading2.ThreadGroup()
        self._daemonize = daemonize

    def submit(self, fn, *args, **kwargs):
        t = threading2.Thread(target=fn, group=self._group,
                              args=args, kwargs=kwargs)
        t.daemon = self._daemonize
        self._threads.append(t)
        t.start()

    def await_termination(self, timeout=None):
        if not self._threads:
            return
        return self._group.join(timeout)


class FlowFailure(object):
    """When a task failure occurs the following object will be given to revert
       and can be used to interrogate what caused the failure.
    """

    def __init__(self, runner, flow, exc, exc_info=None):
        self.runner = runner
        self.flow = flow
        self.exc = exc
        if not exc_info:
            self.exc_info = sys.exc_info()
        else:
            self.exc_info = exc_info


class RollbackTask(object):
    """A helper task that on being called will call the underlying callable
    tasks revert method (if said method exists).
    """

    def __init__(self, context, task, result):
        self.task = task
        self.result = result
        self.context = context

    def __str__(self):
        return str(self.task)

    def __call__(self, cause):
        if ((hasattr(self.task, "revert") and
             isinstance(self.task.revert, collections.Callable))):
            self.task.revert(self.context, self.result, cause)


class Runner(object):
    """A helper class that wraps a task and can find the needed inputs for
    the task to run, as well as providing a uuid and other useful functionality
    for users of the task.

    TODO(harlowja): replace with the task details object or a subclass of
    that???
    """

    def __init__(self, task, uuid=None):
        assert isinstance(task, collections.Callable)
        task_factory = getattr(task, TASK_FACTORY_ATTRIBUTE, None)
        if task_factory:
            self.task = task_factory(task)
        else:
            self.task = task
        self.providers = {}
        self.result = None
        if not uuid:
            self._id = uuidutils.generate_uuid()
        else:
            self._id = str(uuid)

    @property
    def uuid(self):
        return str(self._id)

    @property
    def requires(self):
        return self.task.requires

    @property
    def provides(self):
        return self.task.provides

    @property
    def optional(self):
        return self.task.optional

    @property
    def runs_before(self):
        return []

    @property
    def version(self):
        return get_task_version(self.task)

    @property
    def name(self):
        return self.task.name

    def reset(self):
        self.result = None

    def __str__(self):
        lines = ["Runner: %s" % (self.name)]
        lines.append("%s" % (self.uuid))
        lines.append("%s" % (self.version))
        return "; ".join(lines)

    def __call__(self, *args, **kwargs):
        # Find all of our inputs first.
        kwargs = dict(kwargs)
        for (k, who_made) in self.providers.iteritems():
            if k in kwargs:
                continue
            try:
                kwargs[k] = who_made.result[k]
            except (TypeError, KeyError):
                pass
        optional_keys = self.optional
        optional_keys = optional_keys - set(kwargs.keys())
        for k in optional_keys:
            for who_ran in self.runs_before:
                matched = False
                if k in who_ran.provides:
                    try:
                        kwargs[k] = who_ran.result[k]
                        matched = True
                    except (TypeError, KeyError):
                        pass
                if matched:
                    break
        # Ensure all required keys are either existent or set to none.
        for k in self.requires:
            if k not in kwargs:
                kwargs[k] = None
        # And now finally run.
        self.result = self.task(*args, **kwargs)
        return self.result


class AOTRunner(Runner):
    """A runner that knows who runs before this runner ahead of time from a
    known list of previous runners.
    """

    def __init__(self, task):
        super(AOTRunner, self).__init__(task)
        self._runs_before = []

    @property
    def runs_before(self):
        return self._runs_before

    @runs_before.setter
    def runs_before(self, runs_before):
        self._runs_before = list(runs_before)


class TransitionNotifier(object):
    """A utility helper class that can be used to subscribe to
    notifications of events occuring as well as allow a entity to post said
    notifications to subscribers.
    """

    RESERVED_KEYS = ('details',)
    ANY = '*'

    def __init__(self):
        self._listeners = collections.defaultdict(list)

    def reset(self):
        self._listeners = collections.defaultdict(list)

    def notify(self, state, details):
        listeners = list(self._listeners.get(self.ANY, []))
        for i in self._listeners[state]:
            if i not in listeners:
                listeners.append(i)
        if not listeners:
            return
        for (callback, args, kwargs) in listeners:
            if args is None:
                args = []
            if kwargs is None:
                kwargs = {}
            kwargs['details'] = details
            try:
                callback(state, *args, **kwargs)
            except Exception:
                LOG.exception(("Failure calling callback %s to notify about"
                               " state transition %s"), callback, state)

    def register(self, state, callback, args=None, kwargs=None):
        assert isinstance(callback, collections.Callable)
        for i, (cb, args, kwargs) in enumerate(self._listeners.get(state, [])):
            if cb is callback:
                raise ValueError("Callback %s already registered" % (callback))
        if kwargs:
            for k in self.RESERVED_KEYS:
                if k in kwargs:
                    raise KeyError(("Reserved key '%s' not allowed in "
                                    "kwargs") % k)
            kwargs = copy.copy(kwargs)
        if args:
            args = copy.copy(args)
        self._listeners[state].append((callback, args, kwargs))

    def deregister(self, state, callback):
        if state not in self._listeners:
            return
        for i, (cb, args, kwargs) in enumerate(self._listeners[state]):
            if cb is callback:
                self._listeners[state].pop(i)
                break


class RollbackAccumulator(object):
    """A utility class that can help in organizing 'undo' like code
    so that said code be rolled back on failure (automatically or manually)
    by activating rollback callables that were inserted during said codes
    progression.
    """

    def __init__(self):
        self._rollbacks = []

    def add(self, *callables):
        self._rollbacks.extend(callables)

    def reset(self):
        self._rollbacks = []

    def __len__(self):
        return len(self._rollbacks)

    def __enter__(self):
        return self

    def rollback(self, cause):
        LOG.warn("Activating %s rollbacks due to %s.", len(self), cause)
        for (i, f) in enumerate(reversed(self._rollbacks)):
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
        It is exclusive with write locks.
        """

        self.monitor.acquire()
        while self.rwlock < 0 or self.writers_waiting:
            self.readers_ok.wait()
        self.rwlock += 1
        self.monitor.release()

    def acquire_write(self):
        """Acquire a write lock.

        Only one thread can hold this lock, and only when no read locks
        are also held.
        """

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
