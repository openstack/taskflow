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

from oslo_utils import uuidutils

from taskflow.engines.action_engine import executor
from taskflow.engines.worker_based import protocol as pr
from taskflow import exceptions as excp
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
        request = pr.Request(utils.DummyTask("hi"),
                             uuidutils.generate_uuid(),
                             pr.EXECUTE, {}, 1.0)
        pr.Request.validate(request.to_dict())

    def test_request_invalid(self):
        msg = {
            'task_name': 1,
            'task_cls': False,
            'arguments': [],
        }
        self.assertRaises(excp.InvalidFormat, pr.Request.validate, msg)

    def test_request_invalid_action(self):
        request = pr.Request(utils.DummyTask("hi"),
                             uuidutils.generate_uuid(),
                             pr.EXECUTE, {}, 1.0)
        request = request.to_dict()
        request['action'] = 'NOTHING'
        self.assertRaises(excp.InvalidFormat, pr.Request.validate, request)

    def test_response_progress(self):
        msg = pr.Response(pr.EVENT, details={'progress': 0.5},
                          event_type='blah')
        pr.Response.validate(msg.to_dict())

    def test_response_completion(self):
        msg = pr.Response(pr.SUCCESS, result=1)
        pr.Response.validate(msg.to_dict())

    def test_response_mixed_invalid(self):
        msg = pr.Response(pr.EVENT,
                          details={'progress': 0.5},
                          event_type='blah', result=1)
        self.assertRaises(excp.InvalidFormat, pr.Response.validate, msg)

    def test_response_bad_state(self):
        msg = pr.Response('STUFF')
        self.assertRaises(excp.InvalidFormat, pr.Response.validate, msg)


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
        self.assertEqual(pr.WAITING, request.current_state)
        self.assertIn(request.current_state, pr.WAITING_STATES)
        self.assertRaises(excp.InvalidState, request.transition, pr.SUCCESS)
        self.assertFalse(request.transition(pr.WAITING))
        self.assertTrue(request.transition(pr.PENDING))
        self.assertTrue(request.transition(pr.RUNNING))
        self.assertTrue(request.transition(pr.SUCCESS))
        for s in (pr.PENDING, pr.WAITING):
            self.assertRaises(excp.InvalidState, request.transition, s)

    def test_creation(self):
        request = self.request()
        self.assertEqual(self.task_uuid, request.uuid)
        self.assertEqual(self.task, request.task)
        self.assertFalse(request.future.done())

    def test_to_dict_default(self):
        request = self.request()
        self.assertEqual(self.request_to_dict(), request.to_dict())

    def test_to_dict_with_result(self):
        request = self.request(result=333)
        self.assertEqual(self.request_to_dict(result=('success', 333)),
                         request.to_dict())

    def test_to_dict_with_result_none(self):
        request = self.request(result=None)
        self.assertEqual(self.request_to_dict(result=('success', None)),
                         request.to_dict())

    def test_to_dict_with_result_failure(self):
        a_failure = failure.Failure.from_exception(RuntimeError('Woot!'))
        expected = self.request_to_dict(result=('failure',
                                                a_failure.to_dict()))
        request = self.request(result=a_failure)
        self.assertEqual(expected, request.to_dict())

    def test_to_dict_with_failures(self):
        a_failure = failure.Failure.from_exception(RuntimeError('Woot!'))
        request = self.request(failures={self.task.name: a_failure})
        expected = self.request_to_dict(
            failures={self.task.name: a_failure.to_dict()})
        self.assertEqual(expected, request.to_dict())

    def test_to_dict_with_invalid_json_failures(self):
        exc = RuntimeError(Exception("I am not valid JSON"))
        a_failure = failure.Failure.from_exception(exc)
        request = self.request(failures={self.task.name: a_failure})
        expected = self.request_to_dict(
            failures={self.task.name: a_failure.to_dict(include_args=False)})
        self.assertEqual(expected, request.to_dict())

    @mock.patch('oslo_utils.timeutils.now')
    def test_pending_not_expired(self, now):
        now.return_value = 0
        request = self.request()
        now.return_value = self.timeout - 1
        self.assertFalse(request.expired)

    @mock.patch('oslo_utils.timeutils.now')
    def test_pending_expired(self, now):
        now.return_value = 0
        request = self.request()
        now.return_value = self.timeout + 1
        self.assertTrue(request.expired)

    @mock.patch('oslo_utils.timeutils.now')
    def test_running_not_expired(self, now):
        now.return_value = 0
        request = self.request()
        request.transition(pr.PENDING)
        request.transition(pr.RUNNING)
        now.return_value = self.timeout + 1
        self.assertFalse(request.expired)

    def test_set_result(self):
        request = self.request()
        request.set_result(111)
        result = request.future.result()
        self.assertEqual((executor.EXECUTED, 111), result)
