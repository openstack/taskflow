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

import futurist

from taskflow import task as ta
from taskflow.types import failure
from taskflow.types import notifier

# Execution and reversion outcomes.
EXECUTED = 'executed'
REVERTED = 'reverted'


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
                                      ta.EVENT_UPDATE_PROGRESS,
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
    arguments[ta.REVERT_RESULT] = result
    arguments[ta.REVERT_FLOW_FAILURES] = failures
    with notifier.register_deregister(task.notifier,
                                      ta.EVENT_UPDATE_PROGRESS,
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


class TaskExecutor(object, metaclass=abc.ABCMeta):
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
