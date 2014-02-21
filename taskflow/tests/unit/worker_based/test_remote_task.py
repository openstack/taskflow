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

import mock

from concurrent import futures

from taskflow.engines.worker_based import remote_task as rt
from taskflow import test
from taskflow.tests import utils
from taskflow.utils import misc
from taskflow.utils import persistence_utils as pu


class TestRemoteTask(test.TestCase):

    def setUp(self):
        super(TestRemoteTask, self).setUp()
        self.task = utils.DummyTask()
        self.task_uuid = 'task-uuid'
        self.task_action = 'execute'
        self.task_args = {'context': 'context'}
        self.timeout = 60

    def remote_task(self, **kwargs):
        task_kwargs = dict(task=self.task,
                           uuid=self.task_uuid,
                           action=self.task_action,
                           arguments=self.task_args,
                           progress_callback=None,
                           timeout=self.timeout)
        task_kwargs.update(kwargs)
        return rt.RemoteTask(**task_kwargs)

    def remote_task_request(self, **kwargs):
        request = dict(task=self.task.name,
                       task_name=self.task.name,
                       task_version=self.task.version,
                       action=self.task_action,
                       arguments=self.task_args)
        request.update(kwargs)
        return request

    def test_creation(self):
        remote_task = self.remote_task()
        self.assertEqual(remote_task.uuid, self.task_uuid)
        self.assertEqual(remote_task.name, self.task.name)
        self.assertIsInstance(remote_task.result, futures.Future)
        self.assertFalse(remote_task.result.done())

    def test_repr(self):
        expected_name = '%s:%s' % (self.task.name, self.task_action)
        self.assertEqual(repr(self.remote_task()), expected_name)

    def test_request(self):
        remote_task = self.remote_task()
        request = self.remote_task_request()
        self.assertEqual(remote_task.request, request)

    def test_request_with_result(self):
        remote_task = self.remote_task(result=333)
        request = self.remote_task_request(result=('success', 333))
        self.assertEqual(remote_task.request, request)

    def test_request_with_result_none(self):
        remote_task = self.remote_task(result=None)
        request = self.remote_task_request(result=('success', None))
        self.assertEqual(remote_task.request, request)

    def test_request_with_result_failure(self):
        failure = misc.Failure.from_exception(RuntimeError('Woot!'))
        remote_task = self.remote_task(result=failure)
        request = self.remote_task_request(
            result=('failure', pu.failure_to_dict(failure)))
        self.assertEqual(remote_task.request, request)

    def test_request_with_failures(self):
        failure = misc.Failure.from_exception(RuntimeError('Woot!'))
        remote_task = self.remote_task(failures={self.task.name: failure})
        request = self.remote_task_request(
            failures={self.task.name: pu.failure_to_dict(failure)})
        self.assertEqual(remote_task.request, request)

    @mock.patch('time.time')
    def test_pending_not_expired(self, mock_time):
        mock_time.side_effect = [1, self.timeout]
        remote_task = self.remote_task()
        self.assertFalse(remote_task.expired)

    @mock.patch('time.time')
    def test_pending_expired(self, mock_time):
        mock_time.side_effect = [1, self.timeout + 2]
        remote_task = self.remote_task()
        self.assertTrue(remote_task.expired)

    @mock.patch('time.time')
    def test_running_not_expired(self, mock_time):
        mock_time.side_effect = [1, self.timeout]
        remote_task = self.remote_task()
        remote_task.set_running()
        self.assertFalse(remote_task.expired)

    def test_set_result(self):
        remote_task = self.remote_task()
        remote_task.set_result(111)
        result = remote_task.result.result()
        self.assertEqual(result, (self.task, 'executed', 111))

    def test_on_progress(self):
        progress_callback = mock.MagicMock(name='progress_callback')
        remote_task = self.remote_task(task=self.task,
                                       progress_callback=progress_callback)
        remote_task.on_progress('event_data', 0.0)
        remote_task.on_progress('event_data', 1.0)

        expected_calls = [
            mock.call(self.task, 'event_data', 0.0),
            mock.call(self.task, 'event_data', 1.0)
        ]
        self.assertEqual(progress_callback.mock_calls, expected_calls)
