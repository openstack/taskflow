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
from taskflow import states


class SequentialAction(base.Action):

    def __init__(self, pattern, to_action):
        self._history = []
        self._actions = [to_action(pat) for pat in pattern.children]

    def execute(self, engine):
        state = states.SUCCESS
        for action in self._actions:
            #TODO(imelnikov): save history to storage
            self._history.append(action)
            state = action.execute(engine)
            if state != states.SUCCESS:
                break
        return state

    def revert(self, engine):
        while self._history:
            action = self._history[-1]
            action.revert(engine)
            self._history.pop()
