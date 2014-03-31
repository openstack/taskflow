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
import threading
import time

from concurrent import futures
from kombu import exceptions as kombu_exc

from taskflow.engines.worker_based import executor
from taskflow.engines.worker_based import protocol as pr
from taskflow import test
from taskflow.tests import utils
from taskflow.utils import misc


class TestWorkerTaskExecutor(test.MockTestCase):

    def setUp(self):
        super(TestWorkerTaskExecutor, self).setUp()
        self.task = utils.DummyTask()
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
        self.proxy_mock, self.proxy_inst_mock = self._patch_class(
            executor.proxy, 'Proxy')
        self.request_mock, self.request_inst_mock = self._patch_class(
            executor.pr, 'Request', autospec=False)

        # other mocking
        self.proxy_inst_mock.start.side_effect = self._fake_proxy_start
        self.proxy_inst_mock.stop.side_effect = self._fake_proxy_stop
        self.request_inst_mock.uuid = self.task_uuid
        self.request_inst_mock.expired = False
        self.request_inst_mock.task_cls = self.task.name
        self.wait_for_any_mock = self._patch(
            'taskflow.engines.worker_based.executor.async_utils.wait_for_any')
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
            self._reset_master_mock()
        return ex

    def test_creation(self):
        ex = self.executor(reset_master_mock=False)

        master_mock_calls = [
            mock.call.Proxy(self.executor_uuid, self.executor_exchange,
                            ex._on_message, ex._on_wait, url=self.broker_url)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    def test_on_message_response_state_running(self):
        response = pr.Response(pr.RUNNING)
        ex = self.executor()
        ex._requests_cache.set(self.task_uuid, self.request_inst_mock)
        ex._on_message(response.to_dict(), self.message_mock)

        self.assertEqual(self.request_inst_mock.mock_calls,
                         [mock.call.set_running()])
        self.assertEqual(self.message_mock.mock_calls, [mock.call.ack()])

    def test_on_message_response_state_progress(self):
        response = pr.Response(pr.PROGRESS, progress=1.0)
        ex = self.executor()
        ex._requests_cache.set(self.task_uuid, self.request_inst_mock)
        ex._on_message(response.to_dict(), self.message_mock)

        self.assertEqual(self.request_inst_mock.mock_calls,
                         [mock.call.on_progress(progress=1.0)])
        self.assertEqual(self.message_mock.mock_calls, [mock.call.ack()])

    def test_on_message_response_state_failure(self):
        failure = misc.Failure.from_exception(Exception('test'))
        failure_dict = failure.to_dict()
        response = pr.Response(pr.FAILURE, result=failure_dict)
        ex = self.executor()
        ex._requests_cache.set(self.task_uuid, self.request_inst_mock)
        ex._on_message(response.to_dict(), self.message_mock)

        self.assertEqual(len(ex._requests_cache._data), 0)
        self.assertEqual(self.request_inst_mock.mock_calls, [
            mock.call.set_result(result=utils.FailureMatcher(failure))
        ])
        self.assertEqual(self.message_mock.mock_calls, [mock.call.ack()])

    def test_on_message_response_state_success(self):
        response = pr.Response(pr.SUCCESS, result=self.task_result,
                               event='executed')
        ex = self.executor()
        ex._requests_cache.set(self.task_uuid, self.request_inst_mock)
        ex._on_message(response.to_dict(), self.message_mock)

        self.assertEqual(self.request_inst_mock.mock_calls,
                         [mock.call.set_result(result=self.task_result,
                                               event='executed')])
        self.assertEqual(self.message_mock.mock_calls, [mock.call.ack()])

    def test_on_message_response_unknown_state(self):
        response = pr.Response(state='<unknown>')
        ex = self.executor()
        ex._requests_cache.set(self.task_uuid, self.request_inst_mock)
        ex._on_message(response.to_dict(), self.message_mock)

        self.assertEqual(self.request_inst_mock.mock_calls, [])
        self.assertEqual(self.message_mock.mock_calls, [mock.call.ack()])

    def test_on_message_response_unknown_task(self):
        self.message_mock.properties['correlation_id'] = '<unknown>'
        response = pr.Response(pr.RUNNING)
        ex = self.executor()
        ex._requests_cache.set(self.task_uuid, self.request_inst_mock)
        ex._on_message(response.to_dict(), self.message_mock)

        self.assertEqual(self.request_inst_mock.mock_calls, [])
        self.assertEqual(self.message_mock.mock_calls, [mock.call.ack()])

    def test_on_message_response_no_correlation_id(self):
        self.message_mock.properties = {'type': pr.RESPONSE}
        response = pr.Response(pr.RUNNING)
        ex = self.executor()
        ex._requests_cache.set(self.task_uuid, self.request_inst_mock)
        ex._on_message(response.to_dict(), self.message_mock)

        self.assertEqual(self.request_inst_mock.mock_calls, [])
        self.assertEqual(self.message_mock.mock_calls, [mock.call.ack()])

    @mock.patch('taskflow.engines.worker_based.executor.LOG.warning')
    def test_on_message_unknown_type(self, mocked_warning):
        self.message_mock.properties = {'correlation_id': self.task_uuid,
                                        'type': '<unknown>'}
        ex = self.executor()
        ex._on_message({}, self.message_mock)
        self.assertTrue(mocked_warning.called)

    @mock.patch('taskflow.engines.worker_based.executor.LOG.warning')
    def test_on_message_no_type(self, mocked_warning):
        self.message_mock.properties = {'correlation_id': self.task_uuid}
        ex = self.executor()
        ex._on_message({}, self.message_mock)
        self.assertTrue(mocked_warning.called)

    @mock.patch('taskflow.engines.worker_based.executor.LOG.exception')
    def test_on_message_acknowledge_raises(self, mocked_exception):
        self.message_mock.ack.side_effect = kombu_exc.MessageStateError()
        self.executor()._on_message({}, self.message_mock)
        self.assertTrue(mocked_exception.called)

    def test_on_wait_task_not_expired(self):
        ex = self.executor()
        ex._requests_cache.set(self.task_uuid, self.request_inst_mock)

        self.assertEqual(len(ex._requests_cache._data), 1)
        ex._on_wait()
        self.assertEqual(len(ex._requests_cache._data), 1)

    def test_on_wait_task_expired(self):
        self.request_inst_mock.expired = True
        ex = self.executor()
        ex._requests_cache.set(self.task_uuid, self.request_inst_mock)

        self.assertEqual(len(ex._requests_cache._data), 1)
        ex._on_wait()
        self.assertEqual(len(ex._requests_cache._data), 0)

    def test_remove_task_non_existent(self):
        ex = self.executor()
        ex._requests_cache.set(self.task_uuid, self.request_inst_mock)

        self.assertEqual(len(ex._requests_cache._data), 1)
        ex._requests_cache.delete(self.task_uuid)
        self.assertEqual(len(ex._requests_cache._data), 0)

        # delete non-existent
        ex._requests_cache.delete(self.task_uuid)
        self.assertEqual(len(ex._requests_cache._data), 0)

    def test_execute_task(self):
        self.message_mock.properties['type'] = pr.NOTIFY
        notify = pr.Notify(topic=self.executor_topic, tasks=[self.task.name])
        ex = self.executor()
        ex._on_message(notify.to_dict(), self.message_mock)
        ex.execute_task(self.task, self.task_uuid, self.task_args)

        expected_calls = [
            mock.call.Request(self.task, self.task_uuid, 'execute',
                              self.task_args, None, self.timeout),
            mock.call.request.set_pending(),
            mock.call.proxy.publish(msg=self.request_inst_mock,
                                    routing_key=self.executor_topic,
                                    reply_to=self.executor_uuid,
                                    correlation_id=self.task_uuid)
        ]
        self.assertEqual(self.master_mock.mock_calls, expected_calls)

    def test_revert_task(self):
        self.message_mock.properties['type'] = pr.NOTIFY
        notify = pr.Notify(topic=self.executor_topic, tasks=[self.task.name])
        ex = self.executor()
        ex._on_message(notify.to_dict(), self.message_mock)
        ex.revert_task(self.task, self.task_uuid, self.task_args,
                       self.task_result, self.task_failures)

        expected_calls = [
            mock.call.Request(self.task, self.task_uuid, 'revert',
                              self.task_args, None, self.timeout,
                              failures=self.task_failures,
                              result=self.task_result),
            mock.call.request.set_pending(),
            mock.call.proxy.publish(msg=self.request_inst_mock,
                                    routing_key=self.executor_topic,
                                    reply_to=self.executor_uuid,
                                    correlation_id=self.task_uuid)
        ]
        self.assertEqual(self.master_mock.mock_calls, expected_calls)

    def test_execute_task_topic_not_found(self):
        workers_info = {self.executor_topic: ['<unknown>']}
        ex = self.executor(workers_info=workers_info)
        ex.execute_task(self.task, self.task_uuid, self.task_args)

        expected_calls = [
            mock.call.Request(self.task, self.task_uuid, 'execute',
                              self.task_args, None, self.timeout)
        ]
        self.assertEqual(self.master_mock.mock_calls, expected_calls)

    def test_execute_task_publish_error(self):
        self.message_mock.properties['type'] = pr.NOTIFY
        self.proxy_inst_mock.publish.side_effect = Exception('Woot!')
        notify = pr.Notify(topic=self.executor_topic, tasks=[self.task.name])
        ex = self.executor()
        ex._on_message(notify.to_dict(), self.message_mock)
        ex.execute_task(self.task, self.task_uuid, self.task_args)

        expected_calls = [
            mock.call.Request(self.task, self.task_uuid, 'execute',
                              self.task_args, None, self.timeout),
            mock.call.request.set_pending(),
            mock.call.proxy.publish(msg=self.request_inst_mock,
                                    routing_key=self.executor_topic,
                                    reply_to=self.executor_uuid,
                                    correlation_id=self.task_uuid),
            mock.call.request.set_result(mock.ANY)
        ]
        self.assertEqual(self.master_mock.mock_calls, expected_calls)

    def test_wait_for_any(self):
        fs = [futures.Future(), futures.Future()]
        ex = self.executor()
        ex.wait_for_any(fs)

        expected_calls = [
            mock.call(fs, None)
        ]
        self.assertEqual(self.wait_for_any_mock.mock_calls, expected_calls)

    def test_wait_for_any_with_timeout(self):
        timeout = 30
        fs = [futures.Future(), futures.Future()]
        ex = self.executor()
        ex.wait_for_any(fs, timeout)

        master_mock_calls = [
            mock.call(fs, timeout)
        ]
        self.assertEqual(self.wait_for_any_mock.mock_calls, master_mock_calls)

    def test_start_stop(self):
        ex = self.executor()
        ex.start()

        # make sure proxy thread started
        self.proxy_started_event.wait()

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
        self.proxy_started_event.wait()

        # start executor again
        ex.start()

        # stop executor
        ex.stop()

        self.master_mock.assert_has_calls([
            mock.call.proxy.start(),
            mock.call.proxy.wait(),
            mock.call.proxy.stop()
        ], any_order=True)

    def test_stop_not_running(self):
        self.executor().stop()

        self.assertEqual(self.master_mock.mock_calls, [])

    def test_stop_not_alive(self):
        self.proxy_inst_mock.start.side_effect = None

        # start executor
        ex = self.executor()
        ex.start()

        # wait until executor thread is done
        ex._proxy_thread.join()

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
        self.proxy_started_event.wait()

        # restart executor
        ex.stop()
        ex.start()

        # make sure thread started
        self.proxy_started_event.wait()

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
