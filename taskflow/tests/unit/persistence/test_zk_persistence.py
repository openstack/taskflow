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

import testtools

from taskflow.persistence.backends import impl_zookeeper
from taskflow import test
from taskflow.tests.unit.persistence import base


@testtools.skipIf(True, 'ZooKeeper is not available in Jenkins')
class ZkPersistenceTest(test.TestCase, base.PersistenceTestMixin):
    def _get_connection(self):
        return self._backend.get_connection()

    def setUp(self):
        super(ZkPersistenceTest, self).setUp()
        conf = {
            'hosts': "192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181",
            'path': "/taskflow",
        }
        self._backend = impl_zookeeper.ZkBackend(conf)
        conn = self._get_connection()
        conn.upgrade()

    def tearDown(self):
        super(ZkPersistenceTest, self).tearDown()
        conn = self._get_connection()
        conn.clear_all()
