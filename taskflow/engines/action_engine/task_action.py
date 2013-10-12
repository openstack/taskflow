# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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

import contextlib
import logging

from taskflow.engines.action_engine import base_action as base
from taskflow.openstack.common import excutils
from taskflow import states
from taskflow.utils import misc

LOG = logging.getLogger(__name__)

RESET_TASK_STATES = (states.PENDING,)
SAVE_RESULT_STATES = (states.SUCCESS, states.FAILURE)
ALREADY_FINISHED_STATES = (states.SUCCESS,)
NEVER_RAN_STATES = (states.PENDING,)


@contextlib.contextmanager
def _autobind(task, bind_name, bind_func, **kwargs):
    try:
        task.bind(bind_name, bind_func, **kwargs)
        yield task
    finally:
        task.unbind(bind_name, bind_func)


class TaskAction(base.Action):

    def __init__(self, task, task_id):
        self._task = task
        self._id = task_id

    @property
    def name(self):
        return self._task.name

    @property
    def uuid(self):
        return self._id

    def _change_state(self, engine, state,
                      result=None, progress=None, force=False):
        """Update result and change state."""
        old_state = engine.storage.get_task_state(self.uuid)
        state_check = states.check_task_transition(old_state, state)
        if not force and not state_check:
            # NOTE(harlowja): if we are forcing this state change, we don't
            # care if the state transition should be ignored, if it's not being
            # forced then we just ignore this state change.
            return
        if state in RESET_TASK_STATES:
            engine.storage.reset(self.uuid)
        if state in SAVE_RESULT_STATES:
            engine.storage.save(self.uuid, result, state)
        else:
            engine.storage.set_task_state(self.uuid, state)
        if progress is not None:
            engine.storage.set_task_progress(self.uuid, progress)
        engine._on_task_state_change(self, state, result=result)

    def _on_update_progress(self, task, event_data, progress, **kwargs):
        """Update task progress value that stored in engine."""
        try:
            engine = event_data['engine']
            engine.storage.set_task_progress(self.uuid, progress, **kwargs)
        except Exception:
            # Update progress callbacks should never fail, so capture and log
            # the emitted exception instead of raising it.
            LOG.exception("Failed setting task progress for %s (%s) to %0.3f",
                          task, self.uuid, progress)

    def _force_state(self, engine, state, progress, result=None):
        self._change_state(engine, state,
                           result=result, progress=progress, force=True)
        self._task.update_progress(progress)

    def execute(self, engine):
        if engine.storage.get_task_state(self.uuid) in ALREADY_FINISHED_STATES:
            # Skip tasks that already finished.
            return
        self._force_state(engine, states.RUNNING, 0.0)
        with _autobind(self._task,
                       'update_progress', self._on_update_progress,
                       engine=engine):
            try:
                kwargs = engine.storage.fetch_mapped_args(self._task.rebind)
                result = self._task.execute(**kwargs)
            except Exception:
                failure = misc.Failure()
                self._change_state(engine, states.FAILURE, result=failure)
                failure.reraise()
        self._force_state(engine, states.SUCCESS, 1.0, result=result)

    def revert(self, engine):
        if engine.storage.get_task_state(self.uuid) in NEVER_RAN_STATES:
            # NOTE(imelnikov): in all the other states, the task
            # execution was at least attempted, so we should give
            # task a chance for cleanup
            return
        self._force_state(engine, states.REVERTING, 0.0)
        with _autobind(self._task,
                       'update_progress', self._on_update_progress,
                       engine=engine):
            kwargs = engine.storage.fetch_mapped_args(self._task.rebind)
            try:
                self._task.revert(result=engine.storage.get(self._id),
                                  **kwargs)
            except Exception:
                with excutils.save_and_reraise_exception():
                    self._change_state(engine, states.FAILURE)
        self._force_state(engine, states.REVERTED, 1.0)
        self._force_state(engine, states.PENDING, 0.0)
