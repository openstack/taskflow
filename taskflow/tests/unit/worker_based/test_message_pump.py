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

import mock

from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import proxy
from taskflow import test


class TestMessagePump(test.MockTestCase):
    def test_notify(self):
        barrier = threading.Event()

        on_notify = mock.MagicMock()
        on_notify.side_effect = lambda *args, **kwargs: barrier.set()

        handlers = {pr.NOTIFY: on_notify}
        p = proxy.Proxy("test", "test", handlers,
                        transport='memory',
                        transport_options={
                            'polling_interval': 0.01,
                        })

        t = threading.Thread(target=p.start)
        t.daemon = True
        t.start()
        p.wait()
        p.publish(pr.Notify(), 'test')

        barrier.wait(1.0)
        self.assertTrue(barrier.is_set())
        p.stop()
        t.join()

        self.assertTrue(on_notify.called)
        on_notify.assert_called_with({}, mock.ANY)

    def test_response(self):
        barrier = threading.Event()

        on_response = mock.MagicMock()
        on_response.side_effect = lambda *args, **kwargs: barrier.set()

        handlers = {pr.RESPONSE: on_response}
        p = proxy.Proxy("test", "test", handlers,
                        transport='memory',
                        transport_options={
                            'polling_interval': 0.01,
                        })

        t = threading.Thread(target=p.start)
        t.daemon = True
        t.start()
        p.wait()
        resp = pr.Response(pr.RUNNING)
        p.publish(resp, 'test')

        barrier.wait(1.0)
        self.assertTrue(barrier.is_set())
        p.stop()
        t.join()

        self.assertTrue(on_response.called)
        on_response.assert_called_with(resp.to_dict(), mock.ANY)
