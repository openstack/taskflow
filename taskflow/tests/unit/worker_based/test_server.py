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

from kombu import exceptions as exc

from taskflow.engines.worker_based import endpoint as ep
from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import server
from taskflow import test
from taskflow.tests import utils
from taskflow.utils import misc
from taskflow.utils import persistence_utils as pu


class TestServer(test.MockTestCase):

    def setUp(self):
        super(TestServer, self).setUp()
        self.server_topic = 'server-topic'
        self.server_exchange = 'server-exchange'
        self.broker_url = 'test-url'
        self.task_uuid = 'task-uuid'
        self.task_args = {'x': 1}
        self.task_action = 'execute'
        self.task_name = 'taskflow.tests.utils.TaskOneArgOneReturn'
        self.task_version = (1, 0)
        self.reply_to = 'reply-to'
        self.endpoints = [ep.Endpoint(task_cls=utils.TaskOneArgOneReturn),
                          ep.Endpoint(task_cls=utils.TaskWithFailure),
                          ep.Endpoint(task_cls=utils.ProgressingTask)]
        self.resp_running = dict(state=pr.RUNNING)

        # patch classes
        self.proxy_mock, self.proxy_inst_mock = self._patch_class(
            server.proxy, 'Proxy')

        # other mocking
        self.proxy_inst_mock.is_running = True
        self.executor_mock = mock.MagicMock(name='executor')
        self.message_mock = mock.MagicMock(name='message')
        self.message_mock.properties = {'correlation_id': self.task_uuid,
                                        'reply_to': self.reply_to}
        self.master_mock.attach_mock(self.executor_mock, 'executor')
        self.master_mock.attach_mock(self.message_mock, 'message')

    def server(self, reset_master_mock=False, **kwargs):
        server_kwargs = dict(topic=self.server_topic,
                             exchange=self.server_exchange,
                             executor=self.executor_mock,
                             endpoints=self.endpoints,
                             url=self.broker_url)
        server_kwargs.update(kwargs)
        s = server.Server(**server_kwargs)
        if reset_master_mock:
            self._reset_master_mock()
        return s

    def request(self, **kwargs):
        request = dict(task=self.task_name,
                       task_name=self.task_name,
                       action=self.task_action,
                       task_version=self.task_version,
                       arguments=self.task_args)
        request.update(kwargs)
        return request

    @staticmethod
    def resp_progress(progress):
        return dict(state=pr.PROGRESS, progress=progress, event_data={})

    @staticmethod
    def resp_success(result):
        return dict(state=pr.SUCCESS, result=result)

    @staticmethod
    def resp_failure(result, **kwargs):
        response = dict(state=pr.FAILURE, result=result)
        response.update(kwargs)
        return response

    def test_creation(self):
        s = self.server()

        # check calls
        master_mock_calls = [
            mock.call.Proxy(self.server_topic, self.server_exchange,
                            s._on_message, url=self.broker_url)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)
        self.assertEqual(len(s._endpoints), 3)

    def test_creation_with_endpoints(self):
        s = self.server(endpoints=self.endpoints)

        # check calls
        master_mock_calls = [
            mock.call.Proxy(self.server_topic, self.server_exchange,
                            s._on_message, url=self.broker_url)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)
        self.assertEqual(len(s._endpoints), len(self.endpoints))

    def test_on_message_proxy_running_ack_success(self):
        request = self.request()
        s = self.server(reset_master_mock=True)
        s._on_message(request, self.message_mock)

        # check calls
        master_mock_calls = [
            mock.call.message.ack(),
            mock.call.executor.submit(s._process_request, request,
                                      self.message_mock)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    def test_on_message_proxy_running_ack_failure(self):
        self.message_mock.ack.side_effect = exc.MessageStateError('Woot!')
        s = self.server(reset_master_mock=True)
        s._on_message({}, self.message_mock)

        # check calls
        master_mock_calls = [
            mock.call.message.ack()
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    def test_on_message_proxy_not_running_requeue_success(self):
        self.proxy_inst_mock.is_running = False
        s = self.server(reset_master_mock=True)
        s._on_message({}, self.message_mock)

        # check calls
        master_mock_calls = [
            mock.call.message.requeue()
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    def test_on_message_proxy_not_running_requeue_failure(self):
        self.message_mock.requeue.side_effect = exc.MessageStateError('Woot!')
        self.proxy_inst_mock.is_running = False
        s = self.server(reset_master_mock=True)
        s._on_message({}, self.message_mock)

        # check calls
        master_mock_calls = [
            mock.call.message.requeue()
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    def test_parse_request(self):
        request = self.request()
        task, action, task_args = server.Server._parse_request(**request)

        self.assertEqual((task, action, task_args),
                         (self.task_name, self.task_action,
                          dict(task_name=self.task_name,
                               arguments=self.task_args)))

    def test_parse_request_with_success_result(self):
        request = self.request(action='revert', result=('success', 1))
        task, action, task_args = server.Server._parse_request(**request)

        self.assertEqual((task, action, task_args),
                         (self.task_name, 'revert',
                          dict(task_name=self.task_name,
                               arguments=self.task_args,
                               result=1)))

    def test_parse_request_with_failure_result(self):
        failure = misc.Failure.from_exception(Exception('test'))
        failure_dict = pu.failure_to_dict(failure)
        request = self.request(action='revert',
                               result=('failure', failure_dict))
        task, action, task_args = server.Server._parse_request(**request)

        self.assertEqual((task, action, task_args),
                         (self.task_name, 'revert',
                          dict(task_name=self.task_name,
                               arguments=self.task_args,
                               result=utils.FailureMatcher(failure))))

    def test_parse_request_with_failures(self):
        failures = [misc.Failure.from_exception(Exception('test1')),
                    misc.Failure.from_exception(Exception('test2'))]
        failures_dict = dict((str(i), pu.failure_to_dict(f))
                             for i, f in enumerate(failures))
        request = self.request(action='revert', failures=failures_dict)
        task, action, task_args = server.Server._parse_request(**request)

        self.assertEqual(
            (task, action, task_args),
            (self.task_name, 'revert',
             dict(task_name=self.task_name,
                  arguments=self.task_args,
                  failures=dict((str(i), utils.FailureMatcher(f))
                                for i, f in enumerate(failures)))))

    @mock.patch("taskflow.engines.worker_based.server.LOG.exception")
    def test_reply_publish_failure(self, mocked_exception):
        self.proxy_inst_mock.publish.side_effect = RuntimeError('Woot!')

        # create server and process request
        s = self.server(reset_master_mock=True, endpoints=self.endpoints)
        s._reply(self.reply_to, self.task_uuid)

        self.assertEqual(self.master_mock.mock_calls, [
            mock.call.proxy.publish({'state': 'FAILURE'}, self.reply_to,
                                    correlation_id=self.task_uuid)
        ])
        self.assertTrue(mocked_exception.called)

    def test_on_update_progress(self):
        request = self.request(task='taskflow.tests.utils.ProgressingTask',
                               arguments={})

        # create server and process request
        s = self.server(reset_master_mock=True, endpoints=self.endpoints)
        s._process_request(request, self.message_mock)

        # check calls
        master_mock_calls = [
            mock.call.proxy.publish(self.resp_running, self.reply_to,
                                    correlation_id=self.task_uuid),
            mock.call.proxy.publish(self.resp_progress(0.0), self.reply_to,
                                    correlation_id=self.task_uuid),
            mock.call.proxy.publish(self.resp_progress(1.0), self.reply_to,
                                    correlation_id=self.task_uuid),
            mock.call.proxy.publish(self.resp_success(5), self.reply_to,
                                    correlation_id=self.task_uuid)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    def test_process_request(self):
        # create server and process request
        s = self.server(reset_master_mock=True, endpoints=self.endpoints)
        s._process_request(self.request(), self.message_mock)

        # check calls
        master_mock_calls = [
            mock.call.proxy.publish(self.resp_running, self.reply_to,
                                    correlation_id=self.task_uuid),
            mock.call.proxy.publish(self.resp_success(1), self.reply_to,
                                    correlation_id=self.task_uuid)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    @mock.patch("taskflow.engines.worker_based.server.LOG.exception")
    def test_process_request_parse_message_failure(self, mocked_exception):
        self.message_mock.properties = {}
        request = self.request()
        s = self.server(reset_master_mock=True)
        s._process_request(request, self.message_mock)

        self.assertEqual(self.master_mock.mock_calls, [])
        self.assertTrue(mocked_exception.called)

    @mock.patch('taskflow.engines.worker_based.server.pu')
    def test_process_request_parse_failure(self, pu_mock):
        failure_dict = 'failure_dict'
        pu_mock.failure_to_dict.return_value = failure_dict
        pu_mock.failure_from_dict.side_effect = ValueError('Woot!')
        request = self.request(result=('failure', 1))

        # create server and process request
        s = self.server(reset_master_mock=True, endpoints=self.endpoints)
        s._process_request(request, self.message_mock)

        # check calls
        master_mock_calls = [
            mock.call.proxy.publish(self.resp_failure(failure_dict),
                                    self.reply_to,
                                    correlation_id=self.task_uuid)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    @mock.patch('taskflow.engines.worker_based.server.pu')
    def test_process_request_endpoint_not_found(self, pu_mock):
        failure_dict = 'failure_dict'
        pu_mock.failure_to_dict.return_value = failure_dict
        request = self.request(task='<unknown>')

        # create server and process request
        s = self.server(reset_master_mock=True, endpoints=self.endpoints)
        s._process_request(request, self.message_mock)

        # check calls
        master_mock_calls = [
            mock.call.proxy.publish(self.resp_failure(failure_dict),
                                    self.reply_to,
                                    correlation_id=self.task_uuid)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    @mock.patch('taskflow.engines.worker_based.server.pu')
    def test_process_request_execution_failure(self, pu_mock):
        failure_dict = 'failure_dict'
        pu_mock.failure_to_dict.return_value = failure_dict
        request = self.request(action='<unknown>')

        # create server and process request
        s = self.server(reset_master_mock=True, endpoints=self.endpoints)
        s._process_request(request, self.message_mock)

        # check calls
        master_mock_calls = [
            mock.call.proxy.publish(self.resp_running, self.reply_to,
                                    correlation_id=self.task_uuid),
            mock.call.proxy.publish(self.resp_failure(failure_dict),
                                    self.reply_to,
                                    correlation_id=self.task_uuid)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    @mock.patch('taskflow.engines.worker_based.server.pu')
    def test_process_request_task_failure(self, pu_mock):
        failure_dict = 'failure_dict'
        pu_mock.failure_to_dict.return_value = failure_dict
        request = self.request(task='taskflow.tests.utils.TaskWithFailure',
                               arguments={})

        # create server and process request
        s = self.server(reset_master_mock=True, endpoints=self.endpoints)
        s._process_request(request, self.message_mock)

        # check calls
        master_mock_calls = [
            mock.call.proxy.publish(self.resp_running, self.reply_to,
                                    correlation_id=self.task_uuid),
            mock.call.proxy.publish(self.resp_failure(failure_dict),
                                    self.reply_to,
                                    correlation_id=self.task_uuid)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    def test_start(self):
        self.server(reset_master_mock=True).start()

        # check calls
        master_mock_calls = [
            mock.call.proxy.start()
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    def test_wait(self):
        server = self.server(reset_master_mock=True)
        server.start()
        server.wait()

        # check calls
        master_mock_calls = [
            mock.call.proxy.start(),
            mock.call.proxy.wait()
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    def test_stop(self):
        self.server(reset_master_mock=True).stop()

        # check calls
        master_mock_calls = [
            mock.call.proxy.stop(),
            mock.call.executor.shutdown()
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)
