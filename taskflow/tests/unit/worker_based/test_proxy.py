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

import socket
import threading

from six.moves import mock

from taskflow.engines.worker_based import proxy
from taskflow import test


class TestProxy(test.MockTestCase):

    def setUp(self):
        super(TestProxy, self).setUp()
        self.topic = 'test-topic'
        self.broker_url = 'test-url'
        self.exchange_name = 'test-exchange'
        self.timeout = 5
        self.de_period = proxy.DRAIN_EVENTS_PERIOD

        # patch classes
        self.conn_mock, self.conn_inst_mock = self.patchClass(
            proxy.kombu, 'Connection')
        self.exchange_mock, self.exchange_inst_mock = self.patchClass(
            proxy.kombu, 'Exchange')
        self.queue_mock, self.queue_inst_mock = self.patchClass(
            proxy.kombu, 'Queue')
        self.producer_mock, self.producer_inst_mock = self.patchClass(
            proxy.kombu, 'Producer')

        # connection mocking
        self.conn_inst_mock.drain_events.side_effect = [
            socket.timeout, socket.timeout, KeyboardInterrupt]

        # connections mocking
        self.connections_mock = self.patch(
            "taskflow.engines.worker_based.proxy.kombu.connections",
            attach_as='connections')
        self.connections_mock.__getitem__().acquire().__enter__.return_value =\
            self.conn_inst_mock

        # producers mocking
        self.producers_mock = self.patch(
            "taskflow.engines.worker_based.proxy.kombu.producers",
            attach_as='producers')
        self.producers_mock.__getitem__().acquire().__enter__.return_value =\
            self.producer_inst_mock

        # consumer mocking
        self.conn_inst_mock.Consumer.return_value.__enter__ = mock.MagicMock()
        self.conn_inst_mock.Consumer.return_value.__exit__ = mock.MagicMock()

        # other mocking
        self.on_wait_mock = mock.MagicMock(name='on_wait')
        self.master_mock.attach_mock(self.on_wait_mock, 'on_wait')

        # reset master mock
        self.resetMasterMock()

    def _queue_name(self, topic):
        return "%s_%s" % (self.exchange_name, topic)

    def proxy_start_calls(self, calls, exc_type=mock.ANY):
        return [
            mock.call.Queue(name=self._queue_name(self.topic),
                            exchange=self.exchange_inst_mock,
                            routing_key=self.topic,
                            durable=False,
                            auto_delete=True,
                            channel=self.conn_inst_mock),
            mock.call.connection.Consumer(queues=self.queue_inst_mock,
                                          callbacks=[mock.ANY]),
            mock.call.connection.Consumer().__enter__(),
        ] + calls + [
            mock.call.connection.Consumer().__exit__(exc_type, mock.ANY,
                                                     mock.ANY)
        ]

    def proxy(self, reset_master_mock=False, **kwargs):
        proxy_kwargs = dict(topic=self.topic,
                            exchange_name=self.exchange_name,
                            url=self.broker_url,
                            type_handlers={})
        proxy_kwargs.update(kwargs)
        p = proxy.Proxy(**proxy_kwargs)
        if reset_master_mock:
            self.resetMasterMock()
        return p

    def test_creation(self):
        self.proxy()

        master_mock_calls = [
            mock.call.Connection(self.broker_url, transport=None,
                                 transport_options=None),
            mock.call.Exchange(name=self.exchange_name,
                               durable=False,
                               auto_delete=True)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    def test_creation_custom(self):
        transport_opts = {'context': 'context'}
        self.proxy(transport='memory', transport_options=transport_opts)

        master_mock_calls = [
            mock.call.Connection(self.broker_url, transport='memory',
                                 transport_options=transport_opts),
            mock.call.Exchange(name=self.exchange_name,
                               durable=False,
                               auto_delete=True)
        ]
        self.assertEqual(self.master_mock.mock_calls, master_mock_calls)

    def test_publish(self):
        msg_mock = mock.MagicMock()
        msg_data = 'msg-data'
        msg_mock.to_dict.return_value = msg_data
        routing_key = 'routing-key'
        task_uuid = 'task-uuid'
        kwargs = dict(a='a', b='b')

        self.proxy(reset_master_mock=True).publish(
            msg_mock, routing_key, correlation_id=task_uuid, **kwargs)

        master_mock_calls = [
            mock.call.Queue(name=self._queue_name(routing_key),
                            exchange=self.exchange_inst_mock,
                            routing_key=routing_key,
                            durable=False,
                            auto_delete=True),
            mock.call.producer.publish(body=msg_data,
                                       routing_key=routing_key,
                                       exchange=self.exchange_inst_mock,
                                       correlation_id=task_uuid,
                                       declare=[self.queue_inst_mock],
                                       type=msg_mock.TYPE,
                                       **kwargs)
        ]
        self.master_mock.assert_has_calls(master_mock_calls)

    def test_start(self):
        try:
            # KeyboardInterrupt will be raised after two iterations
            self.proxy(reset_master_mock=True).start()
        except KeyboardInterrupt:
            pass

        master_calls = self.proxy_start_calls([
            mock.call.connection.drain_events(timeout=self.de_period),
            mock.call.connection.drain_events(timeout=self.de_period),
            mock.call.connection.drain_events(timeout=self.de_period),
        ], exc_type=KeyboardInterrupt)
        self.master_mock.assert_has_calls(master_calls)

    def test_start_with_on_wait(self):
        try:
            # KeyboardInterrupt will be raised after two iterations
            self.proxy(reset_master_mock=True,
                       on_wait=self.on_wait_mock).start()
        except KeyboardInterrupt:
            pass

        master_calls = self.proxy_start_calls([
            mock.call.connection.drain_events(timeout=self.de_period),
            mock.call.on_wait(),
            mock.call.connection.drain_events(timeout=self.de_period),
            mock.call.on_wait(),
            mock.call.connection.drain_events(timeout=self.de_period),
        ], exc_type=KeyboardInterrupt)
        self.master_mock.assert_has_calls(master_calls)

    def test_start_with_on_wait_raises(self):
        self.on_wait_mock.side_effect = RuntimeError('Woot!')
        try:
            # KeyboardInterrupt will be raised after two iterations
            self.proxy(reset_master_mock=True,
                       on_wait=self.on_wait_mock).start()
        except KeyboardInterrupt:
            pass

        master_calls = self.proxy_start_calls([
            mock.call.connection.drain_events(timeout=self.de_period),
            mock.call.on_wait(),
        ], exc_type=RuntimeError)
        self.master_mock.assert_has_calls(master_calls)

    def test_stop(self):
        self.conn_inst_mock.drain_events.side_effect = socket.timeout

        # create proxy
        pr = self.proxy(reset_master_mock=True)

        # check that proxy is not running yes
        self.assertFalse(pr.is_running)

        # start proxy in separate thread
        t = threading.Thread(target=pr.start)
        t.daemon = True
        t.start()

        # make sure proxy is started
        pr.wait()

        # check that proxy is running now
        self.assertTrue(pr.is_running)

        # stop proxy and wait for thread to finish
        pr.stop()

        # wait for thread to finish
        t.join()

        self.assertFalse(pr.is_running)
