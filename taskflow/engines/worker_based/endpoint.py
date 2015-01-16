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

from oslo_utils import reflection

from taskflow.engines.action_engine import executor


class Endpoint(object):
    """Represents a single task with execute/revert methods."""

    def __init__(self, task_cls):
        self._task_cls = task_cls
        self._task_cls_name = reflection.get_class_name(task_cls)
        self._executor = executor.SerialTaskExecutor()

    def __str__(self):
        return self._task_cls_name

    @property
    def name(self):
        return self._task_cls_name

    def generate(self, name=None):
        # NOTE(skudriashev): Note that task is created here with the `name`
        # argument passed to its constructor. This will be a problem when
        # task's constructor requires any other arguments.
        return self._task_cls(name=name)

    def execute(self, task, **kwargs):
        event, result = self._executor.execute_task(task, **kwargs).result()
        return result

    def revert(self, task, **kwargs):
        event, result = self._executor.revert_task(task, **kwargs).result()
        return result
