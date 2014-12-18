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

import six

from taskflow import task as task_atom
from taskflow.types import failure
from taskflow.types import futures
from taskflow.types import notifier
from taskflow.utils import async_utils
from taskflow.utils import threading_utils

# Execution and reversion events.
EXECUTED = 'executed'
REVERTED = 'reverted'


def _execute_task(task, arguments, progress_callback=None):
    with notifier.register_deregister(task.notifier,
                                      task_atom.EVENT_UPDATE_PROGRESS,
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
                                      task_atom.EVENT_UPDATE_PROGRESS,
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


@six.add_metaclass(abc.ABCMeta)
class TaskExecutor(object):
    """Executes and reverts tasks.

    This class takes task and its arguments and executes or reverts it.
    It encapsulates knowledge on how task should be executed or reverted:
    right now, on separate thread, on another machine, etc.
    """

    @abc.abstractmethod
    def execute_task(self, task, task_uuid, arguments, progress_callback=None):
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
        self._create_executor = executor is None

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

    def start(self):
        if self._create_executor:
            if self._max_workers is not None:
                max_workers = self._max_workers
            else:
                max_workers = threading_utils.get_optimal_thread_count()
            self._executor = futures.ThreadPoolExecutor(max_workers)

    def stop(self):
        if self._create_executor:
            self._executor.shutdown(wait=True)
            self._executor = None
