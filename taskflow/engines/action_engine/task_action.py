# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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

import logging

from taskflow import states
from taskflow.utils import misc

LOG = logging.getLogger(__name__)

SAVE_RESULT_STATES = (states.SUCCESS, states.FAILURE)


class TaskAction(object):
    def __init__(self, storage, task_executor, notifier):
        self._storage = storage
        self._task_executor = task_executor
        self._notifier = notifier

    def _change_state(self, task, state, result=None, progress=None):
        old_state = self._storage.get_task_state(task.name)
        if not states.check_task_transition(old_state, state):
            return False
        if state in SAVE_RESULT_STATES:
            self._storage.save(task.name, result, state)
        else:
            self._storage.set_task_state(task.name, state)
        if progress is not None:
            self._storage.set_task_progress(task.name, progress)

        task_uuid = self._storage.get_task_uuid(task.name)
        details = dict(task_name=task.name,
                       task_uuid=task_uuid,
                       result=result)
        self._notifier.notify(state, details)
        if progress is not None:
            task.update_progress(progress)
        return True

    def _on_update_progress(self, task, event_data, progress, **kwargs):
        """Should be called when task updates its progress."""
        try:
            self._storage.set_task_progress(task.name, progress, kwargs)
        except Exception:
            # Update progress callbacks should never fail, so capture and log
            # the emitted exception instead of raising it.
            LOG.exception("Failed setting task progress for %s to %0.3f",
                          task, progress)

    def schedule_execution(self, task):
        if not self._change_state(task, states.RUNNING, progress=0.0):
            return
        kwargs = self._storage.fetch_mapped_args(task.rebind)
        return self._task_executor.execute_task(task, kwargs,
                                                self._on_update_progress)

    def complete_execution(self, task, result):
        if isinstance(result, misc.Failure):
            self._change_state(task, states.FAILURE, result=result)
        else:
            self._change_state(task, states.SUCCESS,
                               result=result, progress=1.0)

    def schedule_reversion(self, task):
        if not self._change_state(task, states.REVERTING, progress=0.0):
            return
        kwargs = self._storage.fetch_mapped_args(task.rebind)
        task_result = self._storage.get(task.name)
        failures = self._storage.get_failures()
        future = self._task_executor.revert_task(task, kwargs,
                                                 task_result, failures,
                                                 self._on_update_progress)
        return future

    def complete_reversion(self, task, rev_result):
        if isinstance(rev_result, misc.Failure):
            self._change_state(task, states.FAILURE)
        else:
            self._change_state(task, states.REVERTED, progress=1.0)

    def wait_for_any(self, fs, timeout):
        return self._task_executor.wait_for_any(fs, timeout)
