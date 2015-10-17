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

from taskflow.engines.worker_based import endpoint as ep
from taskflow import task
from taskflow import test
from taskflow.tests import utils


class Task(task.Task):

    def __init__(self, a, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

    def execute(self, *args, **kwargs):
        pass


class TestEndpoint(test.TestCase):

    def setUp(self):
        super(TestEndpoint, self).setUp()
        self.task_cls = utils.TaskOneReturn
        self.task_uuid = 'task-uuid'
        self.task_args = {'context': 'context'}
        self.task_cls_name = reflection.get_class_name(self.task_cls)
        self.task_ep = ep.Endpoint(self.task_cls)
        self.task_result = 1

    def test_creation(self):
        task = self.task_ep.generate()
        self.assertEqual(self.task_cls_name, self.task_ep.name)
        self.assertIsInstance(task, self.task_cls)
        self.assertEqual(self.task_cls_name, task.name)

    def test_creation_with_task_name(self):
        task_name = 'test'
        task = self.task_ep.generate(name=task_name)
        self.assertEqual(self.task_cls_name, self.task_ep.name)
        self.assertIsInstance(task, self.task_cls)
        self.assertEqual(task_name, task.name)

    def test_creation_task_with_constructor_args(self):
        # NOTE(skudriashev): Exception is expected here since task
        # is created without any arguments passing to its constructor.
        endpoint = ep.Endpoint(Task)
        self.assertRaises(TypeError, endpoint.generate)

    def test_to_str(self):
        self.assertEqual(self.task_cls_name, str(self.task_ep))

    def test_execute(self):
        task = self.task_ep.generate(self.task_cls_name)
        result = self.task_ep.execute(task,
                                      task_uuid=self.task_uuid,
                                      arguments=self.task_args,
                                      progress_callback=None)
        self.assertEqual(self.task_result, result)

    def test_revert(self):
        task = self.task_ep.generate(self.task_cls_name)
        result = self.task_ep.revert(task,
                                     task_uuid=self.task_uuid,
                                     arguments=self.task_args,
                                     progress_callback=None,
                                     result=self.task_result,
                                     failures={})
        self.assertEqual(None, result)
