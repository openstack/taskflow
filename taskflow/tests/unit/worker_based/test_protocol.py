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

from concurrent import futures
from oslo.utils import timeutils

from taskflow.engines.worker_based import protocol as pr
from taskflow import exceptions as excp
from taskflow.openstack.common import uuidutils
from taskflow import test
from taskflow.test import mock
from taskflow.tests import utils
from taskflow.types import failure


class TestProtocolValidation(test.TestCase):
    def test_send_notify(self):
        msg = pr.Notify()
        pr.Notify.validate(msg.to_dict(), False)

    def test_send_notify_invalid(self):
        msg = {
            'all your base': 'are belong to us',
        }
        self.assertRaises(excp.InvalidFormat,
                          pr.Notify.validate, msg, False)

    def test_reply_notify(self):
        msg = pr.Notify(topic="bob", tasks=['a', 'b', 'c'])
        pr.Notify.validate(msg.to_dict(), True)

    def test_reply_notify_invalid(self):
        msg = {
            'topic': {},
            'tasks': 'not yours',
        }
        self.assertRaises(excp.InvalidFormat,
                          pr.Notify.validate, msg, True)

    def test_request(self):
        msg = pr.Request(utils.DummyTask("hi"), uuidutils.generate_uuid(),
                         pr.EXECUTE, {}, None, 1.0)
        pr.Request.validate(msg.to_dict())

    def test_request_invalid(self):
        msg = {
            'task_name': 1,
            'task_cls': False,
            'arguments': [],
        }
        self.assertRaises(excp.InvalidFormat, pr.Request.validate, msg)

    def test_request_invalid_action(self):
        msg = pr.Request(utils.DummyTask("hi"), uuidutils.generate_uuid(),
                         pr.EXECUTE, {}, None, 1.0)
        msg = msg.to_dict()
        msg['action'] = 'NOTHING'
        self.assertRaises(excp.InvalidFormat, pr.Request.validate, msg)

    def test_response_progress(self):
        msg = pr.Response(pr.PROGRESS, progress=0.5, event_data={})
        pr.Response.validate(msg.to_dict())

    def test_response_completion(self):
        msg = pr.Response(pr.SUCCESS, result=1)
        pr.Response.validate(msg.to_dict())

    def test_response_mixed_invalid(self):
        msg = pr.Response(pr.PROGRESS, progress=0.5, event_data={}, result=1)
        self.assertRaises(excp.InvalidFormat, pr.Response.validate, msg)

    def test_response_bad_state(self):
        msg = pr.Response('STUFF')
        self.assertRaises(excp.InvalidFormat, pr.Response.validate, msg)


class TestProtocol(test.TestCase):

    def setUp(self):
        super(TestProtocol, self).setUp()
        timeutils.set_time_override()
        self.addCleanup(timeutils.clear_time_override)
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

    def test_request_transitions(self):
        request = self.request()
        self.assertEqual(pr.WAITING, request.state)
        self.assertIn(request.state, pr.WAITING_STATES)
        self.assertRaises(excp.InvalidState, request.transition, pr.SUCCESS)
        self.assertFalse(request.transition(pr.WAITING))
        self.assertTrue(request.transition(pr.PENDING))
        self.assertTrue(request.transition(pr.RUNNING))
        self.assertTrue(request.transition(pr.SUCCESS))
        for s in (pr.PENDING, pr.WAITING):
            self.assertRaises(excp.InvalidState, request.transition, s)

    def test_creation(self):
        request = self.request()
        self.assertEqual(request.uuid, self.task_uuid)
        self.assertEqual(request.task_cls, self.task.name)
        self.assertIsInstance(request.result, futures.Future)
        self.assertFalse(request.result.done())

    def test_to_dict_default(self):
        self.assertEqual(self.request().to_dict(), self.request_to_dict())

    def test_to_dict_with_result(self):
        self.assertEqual(self.request(result=333).to_dict(),
                         self.request_to_dict(result=('success', 333)))

    def test_to_dict_with_result_none(self):
        self.assertEqual(self.request(result=None).to_dict(),
                         self.request_to_dict(result=('success', None)))

    def test_to_dict_with_result_failure(self):
        a_failure = failure.Failure.from_exception(RuntimeError('Woot!'))
        expected = self.request_to_dict(result=('failure',
                                                a_failure.to_dict()))
        self.assertEqual(self.request(result=a_failure).to_dict(), expected)

    def test_to_dict_with_failures(self):
        a_failure = failure.Failure.from_exception(RuntimeError('Woot!'))
        request = self.request(failures={self.task.name: a_failure})
        expected = self.request_to_dict(
            failures={self.task.name: a_failure.to_dict()})
        self.assertEqual(request.to_dict(), expected)

    def test_pending_not_expired(self):
        req = self.request()
        timeutils.advance_time_seconds(self.timeout - 1)
        self.assertFalse(req.expired)

    def test_pending_expired(self):
        req = self.request()
        timeutils.advance_time_seconds(self.timeout + 1)
        self.assertTrue(req.expired)

    def test_running_not_expired(self):
        request = self.request()
        request.transition(pr.PENDING)
        request.transition(pr.RUNNING)
        timeutils.advance_time_seconds(self.timeout + 1)
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
