
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

from taskflow.engines.action_engine import seq_action
from taskflow.engines.action_engine import task_action

from taskflow import blocks
from taskflow import states
from taskflow import storage as t_storage
from taskflow.utils import flow_utils
from taskflow.utils import misc


class ActionEngine(object):
    """Generic action-based engine.

    Converts the flow to recursive structure of actions.
    """

    def __init__(self, flow, action_map, storage):
        self._action_map = action_map
        self.notifier = flow_utils.TransitionNotifier()
        self.task_notifier = flow_utils.TransitionNotifier()
        self.storage = storage
        self.failures = []
        self._root = self.to_action(flow)

    def to_action(self, pattern):
        try:
            factory = self._action_map[type(pattern)]
        except KeyError:
            raise ValueError('Action of unknown type: %s (type %s)'
                             % (pattern, type(pattern)))
        return factory(pattern, self)

    def _revert(self, current_failure):
        self._change_state(states.REVERTING)
        self._root.revert(self)
        self._change_state(states.REVERTED)
        if self.failures:
            self.failures[0].reraise()
        else:
            current_failure.reraise()

    def run(self):
        self._change_state(states.RUNNING)
        try:
            self._root.execute(self)
        except Exception:
            self._revert(misc.Failure())
        else:
            self._change_state(states.SUCCESS)

    def _change_state(self, state):
        self.storage.set_flow_state(state)
        details = dict(engine=self)
        self.notifier.notify(state, details)

    def on_task_state_change(self, task_action, state, result=None):
        if isinstance(result, misc.Failure):
            self.failures.append(result)
        details = dict(engine=self,
                       task_name=task_action.name,
                       task_uuid=task_action.uuid,
                       result=result)
        self.task_notifier.notify(state, details)


class SingleThreadedActionEngine(ActionEngine):
    def __init__(self, flow, flow_detail=None):
        ActionEngine.__init__(self, flow, {
            blocks.Task: task_action.TaskAction,
            blocks.LinearFlow: seq_action.SequentialAction,
            blocks.ParallelFlow: seq_action.SequentialAction
        }, t_storage.Storage(flow_detail))
