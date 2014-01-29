# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2014 AT&T Labs All Rights Reserved.
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

import contextlib

from zake import fake_client

from taskflow.persistence import backends
from taskflow.persistence.backends import impl_zookeeper
from taskflow import test
from taskflow.tests.unit.persistence import base


class ZakePersistenceTest(test.TestCase, base.PersistenceTestMixin):
    def _get_connection(self):
        return self._backend.get_connection()

    def setUp(self):
        super(ZakePersistenceTest, self).setUp()
        conf = {
            "path": "/taskflow",
        }
        client = fake_client.FakeClient()
        client.start()
        self._backend = impl_zookeeper.ZkBackend(conf, client=client)
        conn = self._backend.get_connection()
        conn.upgrade()

    def test_zk_persistence_entry_point(self):
        conf = {'connection': 'zookeeper:'}
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_zookeeper.ZkBackend)
