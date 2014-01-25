# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

from concurrent import futures
import six

from taskflow.utils import async_utils
from taskflow.utils import misc
from taskflow.utils import threading_utils

# Execution and reversion events.
EXECUTED = 'executed'
REVERTED = 'reverted'


def _execute_task(task, arguments, progress_callback):
    with task.autobind('update_progress', progress_callback):
        try:
            result = task.execute(**arguments)
        except Exception:
            # NOTE(imelnikov): wrap current exception with Failure
            # object and return it.
            result = misc.Failure()
    return (task, EXECUTED, result)


def _revert_task(task, arguments, result, failures, progress_callback):
    kwargs = arguments.copy()
    kwargs['result'] = result
    kwargs['flow_failures'] = failures
    with task.autobind('update_progress', progress_callback):
        try:
            result = task.revert(**kwargs)
        except Exception:
            # NOTE(imelnikov): wrap current exception with Failure
            # object and return it.
            result = misc.Failure()
    return (task, REVERTED, result)


@six.add_metaclass(abc.ABCMeta)
class TaskExecutorBase(object):
    """Executes and reverts tasks.

    This class takes task and its arguments and executes or reverts it.
    It encapsulates knowledge on how task should be executed or reverted:
    right now, on separate thread, on another machine, etc.
    """

    @abc.abstractmethod
    def execute_task(self, task, arguments, progress_callback=None):
        """Schedules task execution."""

    @abc.abstractmethod
    def revert_task(self, task, arguments, result, failures,
                    progress_callback=None):
        """Schedules task reversion."""

    @abc.abstractmethod
    def wait_for_any(self, fs, timeout=None):
        """Wait for futures returned by this executor to complete."""

    def start(self):
        """Prepare to execute tasks."""
        pass

    def stop(self):
        """Finalize task executor."""
        pass


class SerialTaskExecutor(TaskExecutorBase):
    """Execute task one after another."""

    def execute_task(self, task, arguments, progress_callback=None):
        return async_utils.make_completed_future(
            _execute_task(task, arguments, progress_callback))

    def revert_task(self, task, arguments, result, failures,
                    progress_callback=None):
        return async_utils.make_completed_future(
            _revert_task(task, arguments, result,
                         failures, progress_callback))

    def wait_for_any(self, fs, timeout=None):
        # NOTE(imelnikov): this executor returns only done futures.
        return fs, []


class ParallelTaskExecutor(TaskExecutorBase):
    """Executes tasks in parallel.

    Submits tasks to executor which should provide interface similar
    to concurrent.Futures.Executor.
    """

    def __init__(self, executor=None):
        self._executor = executor
        self._own_executor = executor is None

    def execute_task(self, task, arguments, progress_callback=None):
        return self._executor.submit(
            _execute_task, task, arguments, progress_callback)

    def revert_task(self, task, arguments, result, failures,
                    progress_callback=None):
        return self._executor.submit(
            _revert_task, task,
            arguments, result, failures, progress_callback)

    def wait_for_any(self, fs, timeout=None):
        return async_utils.wait_for_any(fs, timeout)

    def start(self):
        if self._own_executor:
            thread_count = threading_utils.get_optimal_thread_count()
            self._executor = futures.ThreadPoolExecutor(thread_count)

    def stop(self):
        if self._own_executor:
            self._executor.shutdown(wait=True)
            self._executor = None
