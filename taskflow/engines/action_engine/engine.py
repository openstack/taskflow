
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


class ActionEngine(object):
    """Generic action-based engine

    Converts the flow to recursive structure of actions.
    """

    def __init__(self, flow, action_map):
        self._action_map = action_map
        self._root = self._to_action(flow)

    def _to_action(self, pattern):
        try:
            factory = self._action_map[type(pattern)]
        except KeyError:
            raise ValueError('Action of unknown type: %s (type %s)'
                             % (pattern, type(pattern)))
        return factory(pattern, self._to_action)

    def run(self):
        status = self._root.execute(self)
        if status == states.FAILURE:
            self._root.revert(self)


class SingleThreadedActionEngine(ActionEngine):
    def __init__(self, flow):
        ActionEngine.__init__(self, flow, {
            blocks.Task: task_action.TaskAction,
            blocks.LinearFlow: seq_action.SequentialAction,
            blocks.ParallelFlow: seq_action.SequentialAction
        })
