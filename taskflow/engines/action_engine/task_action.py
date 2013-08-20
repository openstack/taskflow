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

import sys


class TaskAction(base.Action):

    def __init__(self, block, _to_action):
        self._task = block.task
        if isinstance(self._task, type):
            self._task = self._task()
        self.state = states.PENDING

    def execute(self, engine):
        # TODO(imelnikov): notifications
        self.state = states.RUNNING
        try:
            # TODO(imelnikov): pass only necessary args to task
            self._task.execute()
        except Exception:
            # TODO(imelnikov): save exception information
            print sys.exc_info()
            self.state = states.FAILURE
        else:
            self.state = states.SUCCESS
        return self.state

    def revert(self, engine):
        if self.state == states.PENDING:  # pragma: no cover
            # NOTE(imelnikov): in all the other states, the task
            #  execution was at least attempted, so we should give
            #  task a chance for cleanup
            return
        try:
            self._task.revert()
        except Exception:
            self.state = states.FAILURE
            raise
        else:
            self.state = states.PENDING
