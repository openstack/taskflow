# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

from taskflow import exceptions as excp
from taskflow import retry as retry_atom
from taskflow import states as st
from taskflow import task as task_atom
from taskflow.types import failure


class _RetryScheduler(object):
    def __init__(self, runtime):
        self._runtime = runtime
        self._retry_action = runtime.retry_action
        self._storage = runtime.storage

    @staticmethod
    def handles(atom):
        return isinstance(atom, retry_atom.Retry)

    def schedule(self, retry):
        """Schedules the given retry atom for *future* completion.

        Depending on the atoms stored intention this may schedule the retry
        atom for reversion or execution.
        """
        intention = self._storage.get_atom_intention(retry.name)
        if intention == st.EXECUTE:
            return self._retry_action.execute(retry)
        elif intention == st.REVERT:
            return self._retry_action.revert(retry)
        elif intention == st.RETRY:
            self._retry_action.change_state(retry, st.RETRYING)
            self._runtime.retry_subflow(retry)
            return self._retry_action.execute(retry)
        else:
            raise excp.ExecutionFailure("Unknown how to schedule retry with"
                                        " intention: %s" % intention)


class _TaskScheduler(object):
    def __init__(self, runtime):
        self._storage = runtime.storage
        self._task_action = runtime.task_action

    @staticmethod
    def handles(atom):
        return isinstance(atom, task_atom.BaseTask)

    def schedule(self, task):
        """Schedules the given task atom for *future* completion.

        Depending on the atoms stored intention this may schedule the task
        atom for reversion or execution.
        """
        intention = self._storage.get_atom_intention(task.name)
        if intention == st.EXECUTE:
            return self._task_action.schedule_execution(task)
        elif intention == st.REVERT:
            return self._task_action.schedule_reversion(task)
        else:
            raise excp.ExecutionFailure("Unknown how to schedule task with"
                                        " intention: %s" % intention)


class Scheduler(object):
    """Schedules atoms using actions to schedule."""

    def __init__(self, runtime):
        self._schedulers = [
            _RetryScheduler(runtime),
            _TaskScheduler(runtime),
        ]

    def _schedule_node(self, node):
        """Schedule a single node for execution."""
        for sched in self._schedulers:
            if sched.handles(node):
                return sched.schedule(node)
        else:
            raise TypeError("Unknown how to schedule '%s' (%s)"
                            % (node, type(node)))

    def schedule(self, nodes):
        """Schedules the provided nodes for *future* completion.

        This method should schedule a future for each node provided and return
        a set of those futures to be waited on (or used for other similar
        purposes). It should also return any failure objects that represented
        scheduling failures that may have occurred during this scheduling
        process.
        """
        futures = set()
        for node in nodes:
            try:
                futures.add(self._schedule_node(node))
            except Exception:
                # Immediately stop scheduling future work so that we can
                # exit execution early (rather than later) if a single task
                # fails to schedule correctly.
                return (futures, [failure.Failure()])
        return (futures, [])
