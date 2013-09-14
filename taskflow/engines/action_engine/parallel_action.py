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
from taskflow.utils import misc


class ParallelAction(base.Action):

    def __init__(self):
        self._actions = []

    def add(self, action):
        self._actions.append(action)

    def _map(self, engine, fn):
        executor = engine.executor

        def call_fn(action):
            try:
                fn(action)
            except Exception:
                return misc.Failure()
            else:
                return None

        failures = []
        result_iter = executor.map(call_fn, self._actions)
        for result in result_iter:
            if isinstance(result, misc.Failure):
                failures.append(result)
        if failures:
            failures[0].reraise()

    def execute(self, engine):
        self._map(engine, lambda action: action.execute(engine))

    def revert(self, engine):
        self._map(engine, lambda action: action.revert(engine))
