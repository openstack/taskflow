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
import collections
from multiprocessing import managers
import os
import pickle
import threading

import futurist
from oslo_utils import excutils
from oslo_utils import reflection
from oslo_utils import timeutils
from oslo_utils import uuidutils
import six
from six.moves import queue as compat_queue

from taskflow import logging
from taskflow import task as task_atom
from taskflow.types import failure
from taskflow.types import notifier
from taskflow.utils import threading_utils

# Execution and reversion outcomes.
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

# Message types/kind sent from worker/child processes...
_KIND_COMPLETE_ME = 'complete_me'
_KIND_EVENT = 'event'

LOG = logging.getLogger(__name__)


def _execute_retry(retry, arguments):
    try:
        result = retry.execute(**arguments)
    except Exception:
        result = failure.Failure()
    return (EXECUTED, result)


def _revert_retry(retry, arguments):
    try:
        result = retry.revert(**arguments)
    except Exception:
        result = failure.Failure()
    return (REVERTED, result)


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


class _ViewableSyncManager(managers.SyncManager):
    """Manager that exposes its state as methods."""

    def is_shutdown(self):
        return self._state.value == managers.State.SHUTDOWN

    def is_running(self):
        return self._state.value == managers.State.STARTED


class _Channel(object):
    """Helper wrapper around a multiprocessing queue used by a worker."""

    def __init__(self, queue, identity):
        self._queue = queue
        self._identity = identity
        self._sent_messages = collections.defaultdict(int)
        self._pid = None

    @property
    def sent_messages(self):
        return self._sent_messages

    def put(self, message):
        # NOTE(harlowja): this is done in late in execution to ensure that this
        # happens in the child process and not the parent process (where the
        # constructor is called).
        if self._pid is None:
            self._pid = os.getpid()
        message.update({
            'sent_on': timeutils.utcnow(),
            'sender': {
                'pid': self._pid,
                'id': self._identity,
            },
        })
        if 'body' not in message:
            message['body'] = {}
        try:
            self._queue.put(message)
        except _PICKLE_ERRORS:
            LOG.warn("Failed serializing message %s", message, exc_info=True)
            return False
        except _SEND_ERRORS:
            LOG.warn("Failed sending message %s", message, exc_info=True)
            return False
        else:
            self._sent_messages[message['kind']] += 1
            return True


class _WaitWorkItem(object):
    """The piece of work that will executed by a process executor.

    This will call the target function, then wait until the tasks emitted
    events/items have been depleted before offically being finished.

    NOTE(harlowja): this is done so that the task function will *not* return
    until all of its notifications have been proxied back to its originating
    task. If we didn't do this then the executor would see this task as done
    and then potentially start tasks that are successors of the task that just
    finished even though notifications are still left to be sent from the
    previously finished task...
    """

    def __init__(self, channel, barrier,
                 func, task, *args, **kwargs):
        self._channel = channel
        self._barrier = barrier
        self._func = func
        self._task = task
        self._args = args
        self._kwargs = kwargs

    def _on_finish(self):
        sent_events = self._channel.sent_messages.get(_KIND_EVENT, 0)
        if sent_events:
            message = {
                'created_on': timeutils.utcnow(),
                'kind': _KIND_COMPLETE_ME,
            }
            if self._channel.put(message):
                watch = timeutils.StopWatch()
                watch.start()
                self._barrier.wait()
                LOG.trace("Waited %s seconds until task '%s' %s emitted"
                          " notifications were depleted", watch.elapsed(),
                          self._task, sent_events)

    def __call__(self):
        args = self._args
        kwargs = self._kwargs
        try:
            return self._func(self._task, *args, **kwargs)
        finally:
            self._on_finish()


class _EventSender(object):
    """Sends event information from a child worker process to its creator."""

    def __init__(self, channel):
        self._channel = channel

    def __call__(self, event_type, details):
        message = {
            'created_on': timeutils.utcnow(),
            'kind': _KIND_EVENT,
            'body': {
                'event_type': event_type,
                'details': details,
            },
        }
        self._channel.put(message)


class _Target(object):
    """An immutable helper object that represents a target of a message."""

    def __init__(self, task, barrier, identity):
        self.task = task
        self.barrier = barrier
        self.identity = identity
        # Counters used to track how many message 'kinds' were proxied...
        self.dispatched = collections.defaultdict(int)

    def __repr__(self):
        return "<%s at 0x%x targeting '%s' with identity '%s'>" % (
            reflection.get_class_name(self), id(self),
            self.task, self.identity)


class _Dispatcher(object):
    """Dispatches messages received from child worker processes."""

    # When the run() method is busy (typically in a thread) we want to set
    # these so that the thread can know how long to sleep when there is no
    # active work to dispatch.
    _SPIN_PERIODICITY = 0.01

    def __init__(self, dispatch_periodicity=None):
        if dispatch_periodicity is None:
            dispatch_periodicity = self._SPIN_PERIODICITY
        if dispatch_periodicity <= 0:
            raise ValueError("Provided dispatch periodicity must be greater"
                             " than zero and not '%s'" % dispatch_periodicity)
        self._targets = {}
        self._dead = threading.Event()
        self._dispatch_periodicity = dispatch_periodicity
        self._stop_when_empty = False

    def register(self, identity, target):
        self._targets[identity] = target

    def deregister(self, identity):
        try:
            target = self._targets.pop(identity)
        except KeyError:
            pass
        else:
            # Just incase set the barrier to unblock any worker...
            target.barrier.set()
            if LOG.isEnabledFor(logging.TRACE):
                LOG.trace("Dispatched %s messages %s to target '%s' during"
                          " the lifetime of its existence in the dispatcher",
                          sum(six.itervalues(target.dispatched)),
                          dict(target.dispatched), target)

    def reset(self):
        self._stop_when_empty = False
        self._dead.clear()
        if self._targets:
            leftover = set(six.iterkeys(self._targets))
            while leftover:
                self.deregister(leftover.pop())

    def interrupt(self):
        self._stop_when_empty = True
        self._dead.set()

    def _dispatch(self, message):
        if LOG.isEnabledFor(logging.TRACE):
            LOG.trace("Dispatching message %s (it took %s seconds"
                      " for it to arrive for processing after being"
                      " sent)", message,
                      timeutils.delta_seconds(message['sent_on'],
                                              timeutils.utcnow()))
        try:
            kind = message['kind']
            sender = message['sender']
            body = message['body']
        except (KeyError, ValueError, TypeError):
            LOG.warn("Badly formatted message %s received", message,
                     exc_info=True)
            return
        target = self._targets.get(sender['id'])
        if target is None:
            # Must of been removed...
            return
        if kind == _KIND_COMPLETE_ME:
            target.dispatched[kind] += 1
            target.barrier.set()
        elif kind == _KIND_EVENT:
            task = target.task
            target.dispatched[kind] += 1
            task.notifier.notify(body['event_type'], body['details'])
        else:
            LOG.warn("Unknown message '%s' found in message from sender"
                     " %s to target '%s'", kind, sender, target)

    def run(self, queue):
        watch = timeutils.StopWatch(duration=self._dispatch_periodicity)
        while (not self._dead.is_set() or
               (self._stop_when_empty and self._targets)):
            watch.restart()
            leftover = watch.leftover()
            while leftover:
                try:
                    message = queue.get(timeout=leftover)
                except compat_queue.Empty:
                    break
                else:
                    self._dispatch(message)
                    leftover = watch.leftover()
            leftover = watch.leftover()
            if leftover:
                self._dead.wait(leftover)


class SerialRetryExecutor(object):
    """Executes and reverts retries."""

    def __init__(self):
        self._executor = futurist.SynchronousExecutor()

    def start(self):
        """Prepare to execute retries."""
        self._executor.restart()

    def stop(self):
        """Finalize retry executor."""
        self._executor.shutdown()

    def execute_retry(self, retry, arguments):
        """Schedules retry execution."""
        fut = self._executor.submit(_execute_retry, retry, arguments)
        fut.atom = retry
        return fut

    def revert_retry(self, retry, arguments):
        """Schedules retry reversion."""
        fut = self._executor.submit(_revert_retry, retry, arguments)
        fut.atom = retry
        return fut


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

    def start(self):
        """Prepare to execute tasks."""

    def stop(self):
        """Finalize task executor."""


class SerialTaskExecutor(TaskExecutor):
    """Executes tasks one after another."""

    def __init__(self):
        self._executor = futurist.SynchronousExecutor()

    def start(self):
        self._executor.restart()

    def stop(self):
        self._executor.shutdown()

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

    constructor_options = [
        ('max_workers', lambda v: v if v is None else int(v)),
    ]
    """
    Optional constructor keyword arguments this executor supports. These will
    typically be passed via engine options (by a engine user) and converted
    into the correct type before being sent into this
    classes ``__init__`` method.
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
            self._executor = self._create_executor(
                max_workers=self._max_workers)

    def stop(self):
        if self._own_executor:
            self._executor.shutdown(wait=True)
            self._executor = None


class ParallelThreadTaskExecutor(ParallelTaskExecutor):
    """Executes tasks in parallel using a thread pool executor."""

    def _create_executor(self, max_workers=None):
        return futurist.ThreadPoolExecutor(max_workers=max_workers)


class ParallelGreenThreadTaskExecutor(ParallelThreadTaskExecutor):
    """Executes tasks in parallel using a greenthread pool executor."""

    DEFAULT_WORKERS = 1000
    """
    Default number of workers when ``None`` is passed; being that
    greenthreads don't map to native threads or processors very well this
    is more of a guess/somewhat arbitrary, but it does match what the eventlet
    greenpool default size is (so at least it's consistent with what eventlet
    does).
    """

    def _create_executor(self, max_workers=None):
        if max_workers is None:
            max_workers = self.DEFAULT_WORKERS
        return futurist.GreenThreadPoolExecutor(max_workers=max_workers)


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

    constructor_options = [
        ('max_workers', lambda v: v if v is None else int(v)),
        ('dispatch_periodicity', lambda v: v if v is None else float(v)),
    ]
    """
    Optional constructor keyword arguments this executor supports. These will
    typically be passed via engine options (by a engine user) and converted
    into the correct type before being sent into this
    classes ``__init__`` method.
    """

    def __init__(self, executor=None, max_workers=None,
                 dispatch_periodicity=None):
        super(ParallelProcessTaskExecutor, self).__init__(
            executor=executor, max_workers=max_workers)
        self._manager = _ViewableSyncManager()
        self._dispatcher = _Dispatcher(
            dispatch_periodicity=dispatch_periodicity)
        # Only created after starting...
        self._worker = None
        self._queue = None

    def _create_executor(self, max_workers=None):
        return futurist.ProcessPoolExecutor(max_workers=max_workers)

    def start(self):
        if threading_utils.is_alive(self._worker):
            raise RuntimeError("Worker thread must be stopped via stop()"
                               " before starting/restarting")
        super(ParallelProcessTaskExecutor, self).start()
        # These don't seem restartable; make a new one...
        if self._manager.is_shutdown():
            self._manager = _ViewableSyncManager()
        if not self._manager.is_running():
            self._manager.start()
        self._dispatcher.reset()
        self._queue = self._manager.Queue()
        self._worker = threading_utils.daemon_thread(self._dispatcher.run,
                                                     self._queue)
        self._worker.start()

    def stop(self):
        self._dispatcher.interrupt()
        super(ParallelProcessTaskExecutor, self).stop()
        if threading_utils.is_alive(self._worker):
            self._worker.join()
            self._worker = None
            self._queue = None
        self._dispatcher.reset()
        self._manager.shutdown()
        self._manager.join()

    def _rebind_task(self, task, clone, channel, progress_callback=None):
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
        if needed:
            sender = _EventSender(channel)
            for event_type in needed:
                clone.notifier.register(event_type, sender)

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
        to dispatch to the original task (using a common queue + per task
        sender identity/target that is used and associated to know which task
        to proxy back too, since it is possible that there many be *many*
        subprocess running at the same time, each running a different task
        and using the same common queue to submit messages back to).

        Once the subprocess task has finished execution, the executor will
        then trigger a callback that will remove the task + target from the
        dispatcher (which will stop any further proxying back to the original
        task).
        """
        progress_callback = kwargs.pop('progress_callback', None)
        clone = task.copy(retain_listeners=False)
        identity = uuidutils.generate_uuid()
        target = _Target(task, self._manager.Event(), identity)
        channel = _Channel(self._queue, identity)
        self._rebind_task(task, clone, channel,
                          progress_callback=progress_callback)

        def register():
            if progress_callback is not None:
                task.notifier.register(_UPDATE_PROGRESS, progress_callback)
            self._dispatcher.register(identity, target)

        def deregister():
            if progress_callback is not None:
                task.notifier.deregister(_UPDATE_PROGRESS, progress_callback)
            self._dispatcher.deregister(identity)

        register()
        work = _WaitWorkItem(channel, target.barrier,
                             func, clone, *args, **kwargs)
        try:
            fut = self._executor.submit(work)
        except RuntimeError:
            with excutils.save_and_reraise_exception():
                deregister()

        fut.atom = task
        fut.add_done_callback(lambda fut: deregister())
        return fut
