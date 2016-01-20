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

import weakref

from taskflow import exceptions as excp
from taskflow import states as st
from taskflow.types import failure


class RetryScheduler(object):
    """Schedules retry atoms."""

    def __init__(self, runtime):
        self._runtime = weakref.proxy(runtime)
        self._retry_action = runtime.retry_action
        self._storage = runtime.storage

    def schedule(self, retry):
        """Schedules the given retry atom for *future* completion.

        Depending on the atoms stored intention this may schedule the retry
        atom for reversion or execution.
        """
        intention = self._storage.get_atom_intention(retry.name)
        if intention == st.EXECUTE:
            return self._retry_action.schedule_execution(retry)
        elif intention == st.REVERT:
            return self._retry_action.schedule_reversion(retry)
        elif intention == st.RETRY:
            self._retry_action.change_state(retry, st.RETRYING)
            # This will force the subflow to start processing right *after*
            # this retry atom executes (since they will be blocked on their
            # predecessor getting out of the RETRYING/RUNNING state).
            self._runtime.retry_subflow(retry)
            return self._retry_action.schedule_execution(retry)
        else:
            raise excp.ExecutionFailure("Unknown how to schedule retry with"
                                        " intention: %s" % intention)


class TaskScheduler(object):
    """Schedules task atoms."""

    def __init__(self, runtime):
        self._storage = runtime.storage
        self._task_action = runtime.task_action

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
    """Safely schedules atoms using a runtime ``fetch_scheduler`` routine."""

    def __init__(self, runtime):
        self._runtime = weakref.proxy(runtime)

    def schedule(self, atoms):
        """Schedules the provided atoms for *future* completion.

        This method should schedule a future for each atom provided and return
        a set of those futures to be waited on (or used for other similar
        purposes). It should also return any failure objects that represented
        scheduling failures that may have occurred during this scheduling
        process.
        """
        futures = set()
        for atom in atoms:
            scheduler = self._runtime.fetch_scheduler(atom)
            try:
                futures.add(scheduler.schedule(atom))
            except Exception:
                # Immediately stop scheduling future work so that we can
                # exit execution early (rather than later) if a single atom
                # fails to schedule correctly.
                return (futures, [failure.Failure()])
        return (futures, [])
