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

try:
    from kombu import message  # noqa
except ImportError:
    from kombu.transport import base as message

from taskflow.engines.worker_based import dispatcher
from taskflow import test
from taskflow.test import mock


def mock_acked_message(ack_ok=True, **kwargs):
    msg = mock.create_autospec(message.Message, spec_set=True, instance=True,
                               channel=None, **kwargs)

    def ack_side_effect(*args, **kwargs):
        msg.acknowledged = True

    if ack_ok:
        msg.ack_log_error.side_effect = ack_side_effect
    msg.acknowledged = False
    return msg


class TestDispatcher(test.TestCase):
    def test_creation(self):
        on_hello = mock.MagicMock()
        handlers = {'hello': dispatcher.Handler(on_hello)}
        dispatcher.TypeDispatcher(type_handlers=handlers)

    def test_on_message(self):
        on_hello = mock.MagicMock()
        handlers = {'hello': dispatcher.Handler(on_hello)}
        d = dispatcher.TypeDispatcher(type_handlers=handlers)
        msg = mock_acked_message(properties={'type': 'hello'})
        d.on_message("", msg)
        self.assertTrue(on_hello.called)
        self.assertTrue(msg.ack_log_error.called)
        self.assertTrue(msg.acknowledged)

    def test_on_rejected_message(self):
        d = dispatcher.TypeDispatcher()
        msg = mock_acked_message(properties={'type': 'hello'})
        d.on_message("", msg)
        self.assertTrue(msg.reject_log_error.called)
        self.assertFalse(msg.acknowledged)

    def test_on_requeue_message(self):
        d = dispatcher.TypeDispatcher()
        d.requeue_filters.append(lambda data, message: True)
        msg = mock_acked_message()
        d.on_message("", msg)
        self.assertTrue(msg.requeue.called)
        self.assertFalse(msg.acknowledged)

    def test_failed_ack(self):
        on_hello = mock.MagicMock()
        handlers = {'hello': dispatcher.Handler(on_hello)}
        d = dispatcher.TypeDispatcher(type_handlers=handlers)
        msg = mock_acked_message(ack_ok=False,
                                 properties={'type': 'hello'})
        d.on_message("", msg)
        self.assertTrue(msg.ack_log_error.called)
        self.assertFalse(msg.acknowledged)
        self.assertFalse(on_hello.called)
