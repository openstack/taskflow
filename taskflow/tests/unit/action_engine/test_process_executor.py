# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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

import asyncore
import errno
import socket
import threading

from taskflow.engines.action_engine import process_executor as pu
from taskflow import task
from taskflow import test
from taskflow.test import mock
from taskflow.tests import utils as test_utils


class ProcessExecutorHelpersTest(test.TestCase):
    def test_reader(self):
        capture_buf = []

        def do_capture(identity, message_capture_func):
            capture_buf.append(message_capture_func())

        r = pu.Reader(b"secret", do_capture)
        for data in pu._encode_message(b"secret", ['hi'], b'me'):
            self.assertEqual(len(data), r.bytes_needed)
            r.feed(data)

        self.assertEqual(1, len(capture_buf))
        self.assertEqual(['hi'], capture_buf[0])

    def test_bad_hmac_reader(self):
        r = pu.Reader(b"secret-2", lambda ident, capture_func: capture_func())
        in_data = b"".join(pu._encode_message(b"secret", ['hi'], b'me'))
        self.assertRaises(pu.BadHmacValueError, r.feed, in_data)

    @mock.patch("socket.socket")
    def test_no_connect_channel(self, mock_socket_factory):
        mock_sock = mock.MagicMock()
        mock_socket_factory.return_value = mock_sock
        mock_sock.connect.side_effect = socket.error(errno.ECONNREFUSED,
                                                     'broken')
        c = pu.Channel(2222, b"me", b"secret")
        self.assertRaises(socket.error, c.send, "hi")
        self.assertTrue(c.dead)
        self.assertTrue(mock_sock.close.called)

    def test_send_and_dispatch(self):
        details_capture = []

        t = test_utils.DummyTask("rcver")
        t.notifier.register(
            task.EVENT_UPDATE_PROGRESS,
            lambda _event_type, details: details_capture.append(details))

        d = pu.Dispatcher({}, b'secret', b'server-josh')
        d.setup()
        d.targets[b'child-josh'] = t

        s = threading.Thread(target=asyncore.loop, kwargs={'map': d.map})
        s.start()
        self.addCleanup(s.join)

        c = pu.Channel(d.port, b'child-josh', b'secret')
        self.addCleanup(c.close)

        send_what = [
            {'progress': 0.1},
            {'progress': 0.2},
            {'progress': 0.3},
            {'progress': 0.4},
            {'progress': 0.5},
            {'progress': 0.6},
            {'progress': 0.7},
            {'progress': 0.8},
            {'progress': 0.9},
        ]
        e_s = pu.EventSender(c)
        for details in send_what:
            e_s(task.EVENT_UPDATE_PROGRESS, details)

        # This forces the thread to shutdown (since the asyncore loop
        # will exit when no more sockets exist to process...)
        d.close()

        self.assertEqual(len(send_what), len(details_capture))
        self.assertEqual(send_what, details_capture)
