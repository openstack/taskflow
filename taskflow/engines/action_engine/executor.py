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

import abc
import functools
import multiprocessing
from multiprocessing import managers
import os
import pickle
import threading

from oslo.utils import excutils
from oslo.utils import timeutils
import six
from six.moves import queue as compat_queue
from six.moves import range as compat_range

from taskflow import logging
from taskflow import task as task_atom
from taskflow.types import failure
from taskflow.types import futures
from taskflow.types import notifier
from taskflow.types import timing
from taskflow.utils import async_utils
from taskflow.utils import threading_utils

# Execution and reversion events.
EXECUTED = 'executed'
REVERTED = 'reverted'

# See http://bugs.python.org/issue1457119 for why this is so complex...
_PICKLE_ERRORS = [pickle.PickleError, TypeError]
try:
    import cPickle as _cPickle
    _PICKLE_ERRORS.append(_cPickle.PickleError)
except ImportError:
    pass
_PICKLE_ERRORS = tuple(_PICKLE_ERRORS)
_SEND_ERRORS = (IOError, EOFError)
_UPDATE_PROGRESS = task_atom.EVENT_UPDATE_PROGRESS

LOG = logging.getLogger(__name__)


def _maybe_forever(limit=None):
    if limit is None:
        while True:
            yield
    else:
        for i in compat_range(0, limit):
            yield


def _execute_task(task, arguments, progress_callback=None):
    with notifier.register_deregister(task.notifier,
                                      _UPDATE_PROGRESS,
                                      callback=progress_callback):
        try:
            task.pre_execute()
            result = task.execute(**arguments)
        except Exception:
            # NOTE(imelnikov): wrap current exception with Failure
            # object and return it.
            result = failure.Failure()
        finally:
            task.post_execute()
    return (EXECUTED, result)


def _revert_task(task, arguments, result, failures, progress_callback=None):
    arguments = arguments.copy()
    arguments[task_atom.REVERT_RESULT] = result
    arguments[task_atom.REVERT_FLOW_FAILURES] = failures
    with notifier.register_deregister(task.notifier,
                                      _UPDATE_PROGRESS,
                                      callback=progress_callback):
        try:
            task.pre_revert()
            result = task.revert(**arguments)
        except Exception:
            # NOTE(imelnikov): wrap current exception with Failure
            # object and return it.
            result = failure.Failure()
        finally:
            task.post_revert()
    return (REVERTED, result)


class _JoinedWorkItem(object):
    """The piece of work that will executed by a process executor.

    This will call the target function, then wait until the queues items
    have been completed (via calls to task_done) before offically being
    finished.

    NOTE(harlowja): this is done so that the task function will *not* return
    until all of its notifications have been proxied back to its originating
    task. If we didn't do this then the executor would see this task as done
    and then potentially start tasks that are successors of the task that just
    finished even though notifications are still left to be sent from the
    previously finished task...
    """

    def __init__(self, queue, func, task, *args, **kwargs):
        self._queue = queue
        self._func = func
        self._task = task
        self._args = args
        self._kwargs = kwargs

    def _on_finish(self):
        w = timing.StopWatch()
        w.start()
        self._queue.join()
        LOG.blather("Waited %0.2f seconds until task '%s' emitted"
                    " notifications were depleted", w.elapsed(), self._task)

    def __call__(self):
        args = self._args
        kwargs = self._kwargs
        try:
            return self._func(self._task, *args, **kwargs)
        finally:
            self._on_finish()


class _EventSender(object):
    """Sends event information from a child worker process to its creator."""

    def __init__(self, queue):
        self._queue = queue
        self._pid = None

    def __call__(self, event_type, details):
        # NOTE(harlowja): this is done in late in execution to ensure that this
        # happens in the child process and not the parent process (where the
        # constructor is called).
        if self._pid is None:
            self._pid = os.getpid()
        message = {
            'created_on': timeutils.utcnow(),
            'sender': {
                'pid': self._pid,
            },
            'body': {
                'event_type': event_type,
                'details': details,
            },
        }
        try:
            self._queue.put(message)
        except _PICKLE_ERRORS:
            LOG.warn("Failed serializing message %s", message, exc_info=True)
        except _SEND_ERRORS:
            LOG.warn("Failed sending message %s", message, exc_info=True)


class _EventTarget(object):
    """An immutable helper object that represents a target of an event."""

    def __init__(self, future, task, queue):
        self.future = future
        self.task = task
        self.queue = queue


class _EventDispatcher(object):
    """Dispatches event information received from child worker processes."""

    # When the run() method is busy (typically in a thread) we want to set
    # these so that the thread can know how long to sleep when there is no
    # active work to dispatch (when there is active targets, there queues
    # will have amount/count of items removed before returning to work on
    # the next target...)
    _SPIN_PERIODICITY = 0.01
    _SPIN_DISPATCH_AMOUNT = 1

    # TODO(harlowja): look again at using a non-polling mechanism that uses
    # select instead of queues to achieve better ability to detect when
    # messages are ready/available...

    def __init__(self, dispatch_periodicity=None):
        if dispatch_periodicity is None:
            dispatch_periodicity = self._SPIN_PERIODICITY
        if dispatch_periodicity <= 0:
            raise ValueError("Provided dispatch periodicity must be greater"
                             " than zero and not '%s'" % dispatch_periodicity)
        self._targets = set()
        self._dead = threading_utils.Event()
        self._lock = threading.Lock()
        self._periodicity = dispatch_periodicity
        self._stop_when_empty = False

    def register(self, target):
        with self._lock:
            self._targets.add(target)

    def _dispatch_until_empty(self, target, limit=None):
        it = _maybe_forever(limit=limit)
        while True:
            try:
                six.next(it)
            except StopIteration:
                break
            else:
                try:
                    message = target.queue.get_nowait()
                except compat_queue.Empty:
                    break
                else:
                    try:
                        self._dispatch(target.task, message)
                    finally:
                        target.queue.task_done()

    def deregister(self, target):
        with self._lock:
            try:
                self._targets.remove(target)
            except KeyError:
                pass

    def reset(self):
        self._stop_when_empty = False
        while self._targets:
            self.deregister(self._targets.pop())
        self._dead.clear()

    def interrupt(self):
        self._stop_when_empty = True
        self._dead.set()

    def _dispatch(self, task, message):
        LOG.blather("Dispatching message %s to task '%s'", message, task)
        body = message['body']
        task.notifier.notify(body['event_type'], body['details'])

    def _dispatch_iter(self, targets):
        # A generator that yields at certain points to allow the main run()
        # method to use this to dispatch in iterations (and also allows it
        # to check if it has been stopped between iterations).
        for target in targets:
            if target not in self._targets:
                # Must of been removed...
                continue
            # NOTE(harlowja): Limits are used here to avoid one
            # task unequally dispatching, this forces round-robin
            # like behavior...
            self._dispatch_until_empty(target,
                                       limit=self._SPIN_DISPATCH_AMOUNT)
            yield target

    def run(self):
        w = timing.StopWatch(duration=self._periodicity)
        while (not self._dead.is_set() or
               (self._stop_when_empty and self._targets)):
            w.restart()
            with self._lock:
                targets = self._targets.copy()
            for _target in self._dispatch_iter(targets):
                if self._stop_when_empty:
                    continue
                if self._dead.is_set():
                    break
            leftover = w.leftover()
            if leftover:
                self._dead.wait(leftover)


@six.add_metaclass(abc.ABCMeta)
class TaskExecutor(object):
    """Executes and reverts tasks.

    This class takes task and its arguments and executes or reverts it.
    It encapsulates knowledge on how task should be executed or reverted:
    right now, on separate thread, on another machine, etc.
    """

    @abc.abstractmethod
    def execute_task(self, task, task_uuid, arguments,
                     progress_callback=None):
        """Schedules task execution."""

    @abc.abstractmethod
    def revert_task(self, task, task_uuid, arguments, result, failures,
                    progress_callback=None):
        """Schedules task reversion."""

    def wait_for_any(self, fs, timeout=None):
        """Wait for futures returned by this executor to complete."""
        return async_utils.wait_for_any(fs, timeout=timeout)

    def start(self):
        """Prepare to execute tasks."""
        pass

    def stop(self):
        """Finalize task executor."""
        pass


class SerialTaskExecutor(TaskExecutor):
    """Executes tasks one after another."""

    def __init__(self):
        self._executor = futures.SynchronousExecutor()

    def execute_task(self, task, task_uuid, arguments, progress_callback=None):
        fut = self._executor.submit(_execute_task,
                                    task, arguments,
                                    progress_callback=progress_callback)
        fut.atom = task
        return fut

    def revert_task(self, task, task_uuid, arguments, result, failures,
                    progress_callback=None):
        fut = self._executor.submit(_revert_task,
                                    task, arguments, result, failures,
                                    progress_callback=progress_callback)
        fut.atom = task
        return fut


class ParallelTaskExecutor(TaskExecutor):
    """Executes tasks in parallel.

    Submits tasks to an executor which should provide an interface similar
    to concurrent.Futures.Executor.
    """

    def __init__(self, executor=None, max_workers=None):
        self._executor = executor
        self._max_workers = max_workers
        self._own_executor = executor is None

    @abc.abstractmethod
    def _create_executor(self, max_workers=None):
        """Called when an executor has not been provided to make one."""

    def _submit_task(self, func, task, *args, **kwargs):
        fut = self._executor.submit(func, task, *args, **kwargs)
        fut.atom = task
        return fut

    def execute_task(self, task, task_uuid, arguments, progress_callback=None):
        return self._submit_task(_execute_task, task, arguments,
                                 progress_callback=progress_callback)

    def revert_task(self, task, task_uuid, arguments, result, failures,
                    progress_callback=None):
        return self._submit_task(_revert_task, task, arguments, result,
                                 failures, progress_callback=progress_callback)

    def start(self):
        if self._own_executor:
            if self._max_workers is not None:
                max_workers = self._max_workers
            else:
                max_workers = threading_utils.get_optimal_thread_count()
            self._executor = self._create_executor(max_workers=max_workers)

    def stop(self):
        if self._own_executor:
            self._executor.shutdown(wait=True)
            self._executor = None


class ParallelThreadTaskExecutor(ParallelTaskExecutor):
    """Executes tasks in parallel using a thread pool executor."""

    def _create_executor(self, max_workers=None):
        return futures.ThreadPoolExecutor(max_workers=max_workers)


class ParallelProcessTaskExecutor(ParallelTaskExecutor):
    """Executes tasks in parallel using a process pool executor.

    NOTE(harlowja): this executor executes tasks in external processes, so that
    implies that tasks that are sent to that external process are pickleable
    since this is how the multiprocessing works (sending pickled objects back
    and forth) and that the bound handlers (for progress updating in
    particular) are proxied correctly from that external process to the one
    that is alive in the parent process to ensure that callbacks registered in
    the parent are executed on events in the child.
    """

    def __init__(self, executor=None, max_workers=None,
                 dispatch_periodicity=None):
        super(ParallelProcessTaskExecutor, self).__init__(
            executor=executor, max_workers=max_workers)
        self._manager = multiprocessing.Manager()
        self._dispatcher = _EventDispatcher(
            dispatch_periodicity=dispatch_periodicity)
        self._worker = None

    def _queue_factory(self):
        return self._manager.JoinableQueue()

    def _create_executor(self, max_workers=None):
        return futures.ProcessPoolExecutor(max_workers=max_workers)

    def start(self):
        super(ParallelProcessTaskExecutor, self).start()
        # TODO(harlowja): do something else here besides accessing a state
        # of the manager internals (it doesn't seem to expose any way to know
        # this information)...
        if self._manager._state.value == managers.State.SHUTDOWN:
            self._manager = multiprocessing.Manager()
        if self._manager._state.value == managers.State.INITIAL:
            self._manager.start()
        if not threading_utils.is_alive(self._worker):
            self._dispatcher.reset()
            self._worker = threading_utils.daemon_thread(self._dispatcher.run)
            self._worker.start()

    def stop(self):
        self._dispatcher.interrupt()
        super(ParallelProcessTaskExecutor, self).stop()
        if threading_utils.is_alive(self._worker):
            self._worker.join()
            self._worker = None
        self._dispatcher.reset()
        self._manager.shutdown()
        self._manager.join()

    def _rebind_task(self, task, clone, queue, progress_callback=None):
        # Creates and binds proxies for all events the task could receive
        # so that when the clone runs in another process that this task
        # can recieve the same notifications (thus making it look like the
        # the notifications are transparently happening in this process).
        needed = set()
        for (event_type, listeners) in task.notifier.listeners_iter():
            if listeners:
                needed.add(event_type)
        if progress_callback is not None:
            needed.add(_UPDATE_PROGRESS)
        for event_type in needed:
            clone.notifier.register(event_type, _EventSender(queue))
        return needed

    def _submit_task(self, func, task, *args, **kwargs):
        """Submit a function to run the given task (with given args/kwargs).

        NOTE(harlowja): Adjust all events to be proxies instead since we want
        those callbacks to be activated in this process, not in the child,
        also since typically callbacks are functors (or callables) we can
        not pickle those in the first place...

        To make sure people understand how this works, the following is a
        lengthy description of what is going on here, read at will:

        So to ensure that we are proxying task triggered events that occur
        in the executed subprocess (which will be created and used by the
        thing using the multiprocessing based executor) we need to establish
        a link between that process and this process that ensures that when a
        event is triggered in that task in that process that a corresponding
        event is triggered on the original task that was requested to be ran
        in this process.

        To accomplish this we have to create a copy of the task (without
        any listeners) and then reattach a new set of listeners that will
        now instead of calling the desired listeners just place messages
        for this process (a dispatcher thread that is created in this class)
        to dispatch to the original task (using a per task queue that is used
        and associated to know which task to proxy back too, since it is
        possible that there many be *many* subprocess running at the same
        time, each running a different task).

        Once the subprocess task has finished execution, the executor will
        then trigger a callback (``on_done`` in this case) that will remove
        the task + queue from the dispatcher (which will stop any further
        proxying back to the original task).
        """
        progress_callback = kwargs.pop('progress_callback', None)
        clone = task.copy(retain_listeners=False)
        queue = self._queue_factory()
        bound = self._rebind_task(task, clone, queue,
                                  progress_callback=progress_callback)
        LOG.blather("Bound %s event types to clone of '%s'", bound, task)
        if progress_callback is not None:
            binder = functools.partial(task.notifier.register,
                                       _UPDATE_PROGRESS, progress_callback)
            unbinder = functools.partial(task.notifier.deregister,
                                         _UPDATE_PROGRESS, progress_callback)
        else:
            binder = unbinder = lambda: None

        # Ensure the target task (not the clone) is ready and able to receive
        # dispatched messages (and start the dispatching process by
        # registering) with the dispatcher.
        binder()
        work = _JoinedWorkItem(queue, func, clone, *args, **kwargs)
        try:
            fut = self._executor.submit(work)
        except RuntimeError:
            with excutils.save_and_reraise_exception():
                unbinder()

        # This will trigger the proxying to begin...
        target = _EventTarget(fut, task, queue)
        self._dispatcher.register(target)

        def on_done(unbinder, target, fut):
            self._dispatcher.deregister(target)
            unbinder()

        fut.atom = task
        fut.add_done_callback(functools.partial(on_done, unbinder, target))
        return fut
