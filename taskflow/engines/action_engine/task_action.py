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

from taskflow.engines.action_engine import base_action as base
from taskflow import exceptions
from taskflow.openstack.common import excutils
from taskflow.openstack.common import uuidutils
from taskflow import states
from taskflow.utils import misc


class TaskAction(base.Action):

    def __init__(self, task, engine):
        self._task = task
        self._result_mapping = task.save_as
        self._args_mapping = task.rebind
        try:
            self._id = engine.storage.get_uuid_by_name(self._task.name)
        except exceptions.NotFound:
            # TODO(harlowja): we might need to save whether the results of this
            # task will be a tuple + other additional metadata when doing this
            # add to the underlying storage backend for later resumption of
            # this task.
            self._id = uuidutils.generate_uuid()
            engine.storage.add_task(task_name=self.name, uuid=self.uuid)
        engine.storage.set_result_mapping(self.uuid, self._result_mapping)

    @property
    def name(self):
        return self._task.name

    @property
    def uuid(self):
        return self._id

    def _change_state(self, engine, state):
        """Check and update state of task."""
        engine.storage.set_task_state(self.uuid, state)
        engine.on_task_state_change(self, state)

    def _update_result(self, engine, state, result=None):
        """Update result and change state."""
        if state == states.PENDING:
            engine.storage.reset(self.uuid)
        else:
            engine.storage.save(self.uuid, result, state)
        engine.on_task_state_change(self, state, result)

    def execute(self, engine):
        if engine.storage.get_task_state(self.uuid) == states.SUCCESS:
            return
        try:
            kwargs = engine.storage.fetch_mapped_args(self._args_mapping)
            self._change_state(engine, states.RUNNING)
            result = self._task.execute(**kwargs)
        except Exception:
            failure = misc.Failure()
            self._update_result(engine, states.FAILURE, failure)
            failure.reraise()
        else:
            self._update_result(engine, states.SUCCESS, result)

    def revert(self, engine):
        if engine.storage.get_task_state(self.uuid) == states.PENDING:
            # NOTE(imelnikov): in all the other states, the task
            #  execution was at least attempted, so we should give
            #  task a chance for cleanup
            return
        kwargs = engine.storage.fetch_mapped_args(self._args_mapping)
        self._change_state(engine, states.REVERTING)
        try:
            self._task.revert(result=engine.storage.get(self._id),
                              **kwargs)
            self._change_state(engine, states.REVERTED)
        except Exception:
            with excutils.save_and_reraise_exception():
                self._change_state(engine, states.FAILURE)
        else:
            self._update_result(engine, states.PENDING)
