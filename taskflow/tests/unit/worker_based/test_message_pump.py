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

from oslo_utils import uuidutils

from taskflow.engines.worker_based import dispatcher
from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import proxy
from taskflow import test
from taskflow.test import mock
from taskflow.tests import utils as test_utils
from taskflow.types import latch
from taskflow.utils import threading_utils

TEST_EXCHANGE, TEST_TOPIC = ('test-exchange', 'test-topic')
POLLING_INTERVAL = 0.01


class TestMessagePump(test.TestCase):
    def test_notify(self):
        barrier = threading.Event()

        on_notify = mock.MagicMock()
        on_notify.side_effect = lambda *args, **kwargs: barrier.set()

        handlers = {pr.NOTIFY: dispatcher.Handler(on_notify)}
        p = proxy.Proxy(TEST_TOPIC, TEST_EXCHANGE, handlers,
                        transport='memory',
                        transport_options={
                            'polling_interval': POLLING_INTERVAL,
                        })

        t = threading_utils.daemon_thread(p.start)
        t.start()
        p.wait()
        p.publish(pr.Notify(), TEST_TOPIC)

        self.assertTrue(barrier.wait(test_utils.WAIT_TIMEOUT))
        p.stop()
        t.join()

        self.assertTrue(on_notify.called)
        on_notify.assert_called_with({}, mock.ANY)

    def test_response(self):
        barrier = threading.Event()

        on_response = mock.MagicMock()
        on_response.side_effect = lambda *args, **kwargs: barrier.set()

        handlers = {pr.RESPONSE: dispatcher.Handler(on_response)}
        p = proxy.Proxy(TEST_TOPIC, TEST_EXCHANGE, handlers,
                        transport='memory',
                        transport_options={
                            'polling_interval': POLLING_INTERVAL,
                        })

        t = threading_utils.daemon_thread(p.start)
        t.start()
        p.wait()
        resp = pr.Response(pr.RUNNING)
        p.publish(resp, TEST_TOPIC)

        self.assertTrue(barrier.wait(test_utils.WAIT_TIMEOUT))
        self.assertTrue(barrier.is_set())
        p.stop()
        t.join()

        self.assertTrue(on_response.called)
        on_response.assert_called_with(resp.to_dict(), mock.ANY)

    def test_multi_message(self):
        message_count = 30
        barrier = latch.Latch(message_count)
        countdown = lambda data, message: barrier.countdown()

        on_notify = mock.MagicMock()
        on_notify.side_effect = countdown

        on_response = mock.MagicMock()
        on_response.side_effect = countdown

        on_request = mock.MagicMock()
        on_request.side_effect = countdown

        handlers = {
            pr.NOTIFY: dispatcher.Handler(on_notify),
            pr.RESPONSE: dispatcher.Handler(on_response),
            pr.REQUEST: dispatcher.Handler(on_request),
        }
        p = proxy.Proxy(TEST_TOPIC, TEST_EXCHANGE, handlers,
                        transport='memory',
                        transport_options={
                            'polling_interval': POLLING_INTERVAL,
                        })

        t = threading_utils.daemon_thread(p.start)
        t.start()
        p.wait()

        for i in range(0, message_count):
            j = i % 3
            if j == 0:
                p.publish(pr.Notify(), TEST_TOPIC)
            elif j == 1:
                p.publish(pr.Response(pr.RUNNING), TEST_TOPIC)
            else:
                p.publish(pr.Request(test_utils.DummyTask("dummy_%s" % i),
                                     uuidutils.generate_uuid(),
                                     pr.EXECUTE, [], None), TEST_TOPIC)

        self.assertTrue(barrier.wait(test_utils.WAIT_TIMEOUT))
        self.assertEqual(0, barrier.needed)
        p.stop()
        t.join()

        self.assertTrue(on_notify.called)
        self.assertTrue(on_response.called)
        self.assertTrue(on_request.called)

        self.assertEqual(10, on_notify.call_count)
        self.assertEqual(10, on_response.call_count)
        self.assertEqual(10, on_request.call_count)

        call_count = sum([
            on_notify.call_count,
            on_response.call_count,
            on_request.call_count,
        ])
        self.assertEqual(message_count, call_count)
