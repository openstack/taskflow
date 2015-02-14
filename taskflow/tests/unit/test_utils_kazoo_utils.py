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

from kazoo import client
from kazoo import exceptions as k_exc
from six.moves import range as compat_range

from taskflow import test
from taskflow.test import mock
from taskflow.utils import kazoo_utils as ku

_FAKE_ZK_VER = (3, 4, 0)


def _iter_succeed_after(attempts):
    for i in compat_range(0, attempts):
        if i % 2 == 0:
            yield ValueError("Broken")
        else:
            yield AttributeError("Broken")
    yield _FAKE_ZK_VER


class KazooUtilTest(test.TestCase):
    def test_flakey_version_fetch_fail(self):
        m = mock.create_autospec(client.KazooClient, instance=True)
        m.server_version.side_effect = _iter_succeed_after(11)
        self.assertRaises(k_exc.KazooException,
                          ku.fetch_server_version, m, 10)
        self.assertEqual(10, m.server_version.call_count)

    def test_flakey_version_fetch_fail_truncated(self):
        m = mock.create_autospec(client.KazooClient, instance=True)
        m.server_version.side_effect = [None, [], "", [1]]
        self.assertRaises(k_exc.KazooException,
                          ku.fetch_server_version, m, 4)
        self.assertEqual(4, m.server_version.call_count)

    def test_flakey_version_fetch_pass(self):
        m = mock.create_autospec(client.KazooClient, instance=True)
        m.server_version.side_effect = _iter_succeed_after(4)
        self.assertEqual((3, 4, 0), ku.fetch_server_version(m, 5))
        self.assertEqual(5, m.server_version.call_count)
