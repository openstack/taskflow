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

from taskflow.engines.worker_based import protocol as pr
from taskflow import test
from taskflow.tests import utils
from taskflow.utils import misc


class TestProtocol(test.TestCase):

    def setUp(self):
        super(TestProtocol, self).setUp()
        self.task = utils.DummyTask()
        self.task_uuid = 'task-uuid'
        self.task_action = 'execute'
        self.task_args = {'a': 'a'}
        self.timeout = 60

    def request(self, **kwargs):
        request_kwargs = dict(task=self.task,
                              uuid=self.task_uuid,
                              action=self.task_action,
                              arguments=self.task_args,
                              progress_callback=None,
                              timeout=self.timeout)
        request_kwargs.update(kwargs)
        return pr.Request(**request_kwargs)

    def request_to_dict(self, **kwargs):
        to_dict = dict(task_cls=self.task.name,
                       task_name=self.task.name,
                       task_version=self.task.version,
                       action=self.task_action,
                       arguments=self.task_args)
        to_dict.update(kwargs)
        return to_dict

    def test_creation(self):
        request = self.request()
        self.assertEqual(request.uuid, self.task_uuid)
        self.assertEqual(request.task_cls, self.task.name)
        self.assertIsInstance(request.result, futures.Future)
        self.assertFalse(request.result.done())

    def test_str(self):
        request = self.request()
        self.assertEqual(str(request),
                         "<REQUEST> %s" % self.request_to_dict())

    def test_repr(self):
        expected = '%s:%s' % (self.task.name, self.task_action)
        self.assertEqual(repr(self.request()), expected)

    def test_to_dict_default(self):
        self.assertEqual(self.request().to_dict(), self.request_to_dict())

    def test_to_dict_with_result(self):
        self.assertEqual(self.request(result=333).to_dict(),
                         self.request_to_dict(result=('success', 333)))

    def test_to_dict_with_result_none(self):
        self.assertEqual(self.request(result=None).to_dict(),
                         self.request_to_dict(result=('success', None)))

    def test_to_dict_with_result_failure(self):
        failure = misc.Failure.from_exception(RuntimeError('Woot!'))
        expected = self.request_to_dict(result=('failure', failure.to_dict()))
        self.assertEqual(self.request(result=failure).to_dict(), expected)

    def test_to_dict_with_failures(self):
        failure = misc.Failure.from_exception(RuntimeError('Woot!'))
        request = self.request(failures={self.task.name: failure})
        expected = self.request_to_dict(
            failures={self.task.name: failure.to_dict()})
        self.assertEqual(request.to_dict(), expected)

    @mock.patch('taskflow.engines.worker_based.protocol.misc.wallclock')
    def test_pending_not_expired(self, mocked_wallclock):
        mocked_wallclock.side_effect = [1, self.timeout]
        self.assertFalse(self.request().expired)

    @mock.patch('taskflow.engines.worker_based.protocol.misc.wallclock')
    def test_pending_expired(self, mocked_wallclock):
        mocked_wallclock.side_effect = [1, self.timeout + 2]
        self.assertTrue(self.request().expired)

    @mock.patch('taskflow.engines.worker_based.protocol.misc.wallclock')
    def test_running_not_expired(self, mocked_wallclock):
        mocked_wallclock.side_effect = [1, self.timeout + 2]
        request = self.request()
        request.set_running()
        self.assertFalse(request.expired)

    def test_set_result(self):
        request = self.request()
        request.set_result(111)
        result = request.result.result()
        self.assertEqual(result, (self.task, 'executed', 111))

    def test_on_progress(self):
        progress_callback = mock.MagicMock(name='progress_callback')
        request = self.request(task=self.task,
                               progress_callback=progress_callback)
        request.on_progress('event_data', 0.0)
        request.on_progress('event_data', 1.0)

        expected_calls = [
            mock.call(self.task, 'event_data', 0.0),
            mock.call(self.task, 'event_data', 1.0)
        ]
        self.assertEqual(progress_callback.mock_calls, expected_calls)
