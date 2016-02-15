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

import threading
import time

from taskflow.engines.worker_based import executor
from taskflow.engines.worker_based import protocol as pr
from taskflow import task as task_atom
from taskflow import test
from taskflow.test import mock
from taskflow.tests import utils as test_utils
from taskflow.types import failure


class TestWorkerTaskExecutor(test.MockTestCase):

    def setUp(self):
        super(TestWorkerTaskExecutor, self).setUp()
        self.task = test_utils.DummyTask()
        self.task_uuid = 'task-uuid'
        self.task_args = {'a': 'a'}
        self.task_result = 'task-result'
        self.task_failures = {}
        self.timeout = 60
        self.broker_url = 'broker-url'
        self.executor_uuid = 'executor-uuid'
        self.executor_exchange = 'executor-exchange'
        self.executor_topic = 'test-topic1'
        self.proxy_started_event = threading.Event()

        # patch classes
        self.proxy_mock, self.proxy_inst_mock = self.patchClass(
            executor.proxy, 'Proxy')
        self.request_mock, self.request_inst_mock = self.patchClass(
            executor.pr, 'Request', autospec=False)

        # other mocking
        self.proxy_inst_mock.start.side_effect = self._fake_proxy_start
        self.proxy_inst_mock.stop.side_effect = self._fake_proxy_stop
        self.request_inst_mock.uuid = self.task_uuid
        self.request_inst_mock.expired = False
        self.request_inst_mock.created_on = 0
        self.request_inst_mock.task_cls = self.task.name
        self.message_mock = mock.MagicMock(name='message')
        self.message_mock.properties = {'correlation_id': self.task_uuid,
                                        'type': pr.RESPONSE}

    def _fake_proxy_start(self):
        self.proxy_started_event.set()
        while self.proxy_started_event.is_set():
            time.sleep(0.01)

    def _fake_proxy_stop(self):
        self.proxy_started_event.clear()

    def executor(self, reset_master_mock=True, **kwargs):
        executor_kwargs = dict(uuid=self.executor_uuid,
                               exchange=self.executor_exchange,
                               topics=[self.executor_topic],
                               url=self.broker_url)
        executor_kwargs.update(kwargs)
        ex = executor.WorkerTaskExecutor(**executor_kwargs)
        if reset_master_mock:
            self.resetMasterMock()
        return ex

    def test_creation(self):
        ex = self.executor(reset_master_mock=False)
        master_mock_calls = [
            mock.call.Proxy(self.executor_uuid, self.executor_exchange,
                            on_wait=ex._on_wait,
                            url=self.broker_url, transport=mock.ANY,
                            transport_options=mock.ANY,
                            retry_options=mock.ANY),
            mock.call.proxy.dispatcher.type_handlers.update(mock.ANY),
        ]
        self.assertEqual(master_mock_calls, self.master_mock.mock_calls)

    def test_on_message_response_state_running(self):
        response = pr.Response(pr.RUNNING)
        ex = self.executor()
        ex._ongoing_requests[self.task_uuid] = self.request_inst_mock
        ex._process_response(response.to_dict(), self.message_mock)

        expected_calls = [
            mock.call.transition_and_log_error(pr.RUNNING, logger=mock.ANY),
        ]
        self.assertEqual(expected_calls, self.request_inst_mock.mock_calls)

    def test_on_message_response_state_progress(self):
        response = pr.Response(pr.EVENT,
                               event_type=task_atom.EVENT_UPDATE_PROGRESS,
                               details={'progress': 1.0})
        ex = self.executor()
        ex._ongoing_requests[self.task_uuid] = self.request_inst_mock
        ex._process_response(response.to_dict(), self.message_mock)

        expected_calls = [
            mock.call.task.notifier.notify(task_atom.EVENT_UPDATE_PROGRESS,
                                           {'progress': 1.0}),
        ]
        self.assertEqual(expected_calls, self.request_inst_mock.mock_calls)

    def test_on_message_response_state_failure(self):
        a_failure = failure.Failure.from_exception(Exception('test'))
        failure_dict = a_failure.to_dict()
        response = pr.Response(pr.FAILURE, result=failure_dict)
        ex = self.executor()
        ex._ongoing_requests[self.task_uuid] = self.request_inst_mock
        ex._process_response(response.to_dict(), self.message_mock)

        self.assertEqual(0, len(ex._ongoing_requests))
        expected_calls = [
            mock.call.transition_and_log_error(pr.FAILURE, logger=mock.ANY),
            mock.call.set_result(result=test_utils.FailureMatcher(a_failure))
        ]
        self.assertEqual(expected_calls, self.request_inst_mock.mock_calls)

    def test_on_message_response_state_success(self):
        response = pr.Response(pr.SUCCESS, result=self.task_result,
                               event='executed')
        ex = self.executor()
        ex._ongoing_requests[self.task_uuid] = self.request_inst_mock
        ex._process_response(response.to_dict(), self.message_mock)

        expected_calls = [
            mock.call.transition_and_log_error(pr.SUCCESS, logger=mock.ANY),
            mock.call.set_result(result=self.task_result)
        ]
        self.assertEqual(expected_calls, self.request_inst_mock.mock_calls)

    def test_on_message_response_unknown_state(self):
        response = pr.Response(state='<unknown>')
        ex = self.executor()
        ex._ongoing_requests[self.task_uuid] = self.request_inst_mock
        ex._process_response(response.to_dict(), self.message_mock)

        self.assertEqual([], self.request_inst_mock.mock_calls)

    def test_on_message_response_unknown_task(self):
        self.message_mock.properties['correlation_id'] = '<unknown>'
        response = pr.Response(pr.RUNNING)
        ex = self.executor()
        ex._ongoing_requests[self.task_uuid] = self.request_inst_mock
        ex._process_response(response.to_dict(), self.message_mock)

        self.assertEqual([], self.request_inst_mock.mock_calls)

    def test_on_message_response_no_correlation_id(self):
        self.message_mock.properties = {'type': pr.RESPONSE}
        response = pr.Response(pr.RUNNING)
        ex = self.executor()
        ex._ongoing_requests[self.task_uuid] = self.request_inst_mock
        ex._process_response(response.to_dict(), self.message_mock)

        self.assertEqual([], self.request_inst_mock.mock_calls)

    def test_on_wait_task_not_expired(self):
        ex = self.executor()
        ex._ongoing_requests[self.task_uuid] = self.request_inst_mock

        self.assertEqual(1, len(ex._ongoing_requests))
        ex._on_wait()
        self.assertEqual(1, len(ex._ongoing_requests))

    @mock.patch('oslo_utils.timeutils.now')
    def test_on_wait_task_expired(self, mock_now):
        mock_now.side_effect = [0, 120]

        self.request_inst_mock.expired = True
        self.request_inst_mock.created_on = 0

        ex = self.executor()
        ex._ongoing_requests[self.task_uuid] = self.request_inst_mock
        self.assertEqual(1, len(ex._ongoing_requests))

        ex._on_wait()
        self.assertEqual(0, len(ex._ongoing_requests))

    def test_execute_task(self):
        ex = self.executor()
        ex._finder._add(self.executor_topic, [self.task.name])
        ex.execute_task(self.task, self.task_uuid, self.task_args)

        expected_calls = [
            mock.call.Request(self.task, self.task_uuid, 'execute',
                              self.task_args, timeout=self.timeout,
                              result=mock.ANY, failures=mock.ANY),
            mock.call.request.transition_and_log_error(pr.PENDING,
                                                       logger=mock.ANY),
            mock.call.proxy.publish(self.request_inst_mock,
                                    self.executor_topic,
                                    reply_to=self.executor_uuid,
                                    correlation_id=self.task_uuid)
        ]
        self.assertEqual(expected_calls, self.master_mock.mock_calls)

    def test_revert_task(self):
        ex = self.executor()
        ex._finder._add(self.executor_topic, [self.task.name])
        ex.revert_task(self.task, self.task_uuid, self.task_args,
                       self.task_result, self.task_failures)

        expected_calls = [
            mock.call.Request(self.task, self.task_uuid, 'revert',
                              self.task_args, timeout=self.timeout,
                              failures=self.task_failures,
                              result=self.task_result),
            mock.call.request.transition_and_log_error(pr.PENDING,
                                                       logger=mock.ANY),
            mock.call.proxy.publish(self.request_inst_mock,
                                    self.executor_topic,
                                    reply_to=self.executor_uuid,
                                    correlation_id=self.task_uuid)
        ]
        self.assertEqual(expected_calls, self.master_mock.mock_calls)

    def test_execute_task_topic_not_found(self):
        ex = self.executor()
        ex.execute_task(self.task, self.task_uuid, self.task_args)

        expected_calls = [
            mock.call.Request(self.task, self.task_uuid, 'execute',
                              self.task_args, timeout=self.timeout,
                              result=mock.ANY, failures=mock.ANY),
        ]
        self.assertEqual(expected_calls, self.master_mock.mock_calls)

    def test_execute_task_publish_error(self):
        self.proxy_inst_mock.publish.side_effect = Exception('Woot!')
        ex = self.executor()
        ex._finder._add(self.executor_topic, [self.task.name])
        ex.execute_task(self.task, self.task_uuid, self.task_args)

        expected_calls = [
            mock.call.Request(self.task, self.task_uuid, 'execute',
                              self.task_args, timeout=self.timeout,
                              result=mock.ANY, failures=mock.ANY),
            mock.call.request.transition_and_log_error(pr.PENDING,
                                                       logger=mock.ANY),
            mock.call.proxy.publish(self.request_inst_mock,
                                    self.executor_topic,
                                    reply_to=self.executor_uuid,
                                    correlation_id=self.task_uuid),
            mock.call.request.transition_and_log_error(pr.FAILURE,
                                                       logger=mock.ANY),
            mock.call.request.set_result(mock.ANY)
        ]
        self.assertEqual(expected_calls, self.master_mock.mock_calls)

    def test_start_stop(self):
        ex = self.executor()
        ex.start()

        # make sure proxy thread started
        self.assertTrue(self.proxy_started_event.wait(test_utils.WAIT_TIMEOUT))

        # stop executor
        ex.stop()

        self.master_mock.assert_has_calls([
            mock.call.proxy.start(),
            mock.call.proxy.wait(),
            mock.call.proxy.stop()
        ], any_order=True)

    def test_start_already_running(self):
        ex = self.executor()
        ex.start()

        # make sure proxy thread started
        self.assertTrue(self.proxy_started_event.wait(test_utils.WAIT_TIMEOUT))

        # start executor again
        self.assertRaises(RuntimeError, ex.start)

        # stop executor
        ex.stop()

        self.master_mock.assert_has_calls([
            mock.call.proxy.start(),
            mock.call.proxy.wait(),
            mock.call.proxy.stop()
        ], any_order=True)

    def test_stop_not_running(self):
        self.executor().stop()

        self.assertEqual([], self.master_mock.mock_calls)

    def test_stop_not_alive(self):
        self.proxy_inst_mock.start.side_effect = None

        # start executor
        ex = self.executor()
        ex.start()

        # stop executor
        ex.stop()

        # since proxy thread is already done - stop is not called
        self.master_mock.assert_has_calls([
            mock.call.proxy.start(),
            mock.call.proxy.wait()
        ], any_order=True)

    def test_restart(self):
        ex = self.executor()
        ex.start()

        # make sure thread started
        self.assertTrue(self.proxy_started_event.wait(test_utils.WAIT_TIMEOUT))

        # restart executor
        ex.stop()
        ex.start()

        # make sure thread started
        self.assertTrue(self.proxy_started_event.wait(test_utils.WAIT_TIMEOUT))

        # stop executor
        ex.stop()

        self.master_mock.assert_has_calls([
            mock.call.proxy.start(),
            mock.call.proxy.wait(),
            mock.call.proxy.stop(),
            mock.call.proxy.start(),
            mock.call.proxy.wait(),
            mock.call.proxy.stop()
        ], any_order=True)
