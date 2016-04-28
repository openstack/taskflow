# -*- coding: utf-8 -*-

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

import functools

from taskflow.engines.action_engine.actions import base
from taskflow import logging
from taskflow import states
from taskflow import task as task_atom
from taskflow.types import failure

LOG = logging.getLogger(__name__)


class TaskAction(base.Action):
    """An action that handles scheduling, state changes, ... of task atoms."""

    def __init__(self, storage, notifier, task_executor):
        super(TaskAction, self).__init__(storage, notifier)
        self._task_executor = task_executor

    def _is_identity_transition(self, old_state, state, task, progress=None):
        if state in self.SAVE_RESULT_STATES:
            # saving result is never identity transition
            return False
        if state != old_state:
            # changing state is not identity transition by definition
            return False
        # NOTE(imelnikov): last thing to check is that the progress has
        # changed, which means progress is not None and is different from
        # what is stored in the database.
        if progress is None:
            return False
        old_progress = self._storage.get_task_progress(task.name)
        if old_progress != progress:
            return False
        return True

    def change_state(self, task, state,
                     progress=None, result=base.Action.NO_RESULT):
        old_state = self._storage.get_atom_state(task.name)
        if self._is_identity_transition(old_state, state, task,
                                        progress=progress):
            # NOTE(imelnikov): ignore identity transitions in order
            # to avoid extra write to storage backend and, what's
            # more important, extra notifications.
            return
        if state in self.SAVE_RESULT_STATES:
            save_result = None
            if result is not self.NO_RESULT:
                save_result = result
            self._storage.save(task.name, save_result, state)
        else:
            self._storage.set_atom_state(task.name, state)
        if progress is not None:
            self._storage.set_task_progress(task.name, progress)
        task_uuid = self._storage.get_atom_uuid(task.name)
        details = {
            'task_name': task.name,
            'task_uuid': task_uuid,
            'old_state': old_state,
        }
        if result is not self.NO_RESULT:
            details['result'] = result
        self._notifier.notify(state, details)
        if progress is not None:
            task.update_progress(progress)

    def _on_update_progress(self, task, event_type, details):
        """Should be called when task updates its progress."""
        try:
            progress = details.pop('progress')
        except KeyError:
            pass
        else:
            try:
                self._storage.set_task_progress(task.name, progress,
                                                details=details)
            except Exception:
                # Update progress callbacks should never fail, so capture and
                # log the emitted exception instead of raising it.
                LOG.exception("Failed setting task progress for %s to %0.3f",
                              task, progress)

    def schedule_execution(self, task):
        self.change_state(task, states.RUNNING, progress=0.0)
        arguments = self._storage.fetch_mapped_args(
            task.rebind,
            atom_name=task.name,
            optional_args=task.optional
        )
        if task.notifier.can_be_registered(task_atom.EVENT_UPDATE_PROGRESS):
            progress_callback = functools.partial(self._on_update_progress,
                                                  task)
        else:
            progress_callback = None
        task_uuid = self._storage.get_atom_uuid(task.name)
        return self._task_executor.execute_task(
            task, task_uuid, arguments,
            progress_callback=progress_callback)

    def complete_execution(self, task, result):
        if isinstance(result, failure.Failure):
            self.change_state(task, states.FAILURE, result=result)
        else:
            self.change_state(task, states.SUCCESS,
                              result=result, progress=1.0)

    def schedule_reversion(self, task):
        self.change_state(task, states.REVERTING, progress=0.0)
        arguments = self._storage.fetch_mapped_args(
            task.revert_rebind,
            atom_name=task.name,
            optional_args=task.revert_optional
        )
        task_uuid = self._storage.get_atom_uuid(task.name)
        task_result = self._storage.get(task.name)
        failures = self._storage.get_failures()
        if task.notifier.can_be_registered(task_atom.EVENT_UPDATE_PROGRESS):
            progress_callback = functools.partial(self._on_update_progress,
                                                  task)
        else:
            progress_callback = None
        return self._task_executor.revert_task(
            task, task_uuid, arguments, task_result, failures,
            progress_callback=progress_callback)

    def complete_reversion(self, task, result):
        if isinstance(result, failure.Failure):
            self.change_state(task, states.REVERT_FAILURE, result=result)
        else:
            self.change_state(task, states.REVERTED, progress=1.0,
                              result=result)
