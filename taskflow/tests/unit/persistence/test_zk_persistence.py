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

import testtools

from taskflow.openstack.common import uuidutils
from taskflow.persistence.backends import impl_zookeeper
from taskflow import test
from taskflow.tests.unit.persistence import base
from taskflow.utils import kazoo_utils

TEST_CONFIG = {
    'timeout': 1.0,
    'hosts': ["localhost:2181"],
}
TEST_PATH_TPL = '/taskflow/persistence-test/%s'


def _zookeeper_available():
    client = kazoo_utils.make_client(TEST_CONFIG)
    try:
        client.start()
        zk_ver = client.server_version()
        if zk_ver >= impl_zookeeper.MIN_ZK_VERSION:
            return True
        else:
            return False
    except Exception:
        return False
    finally:
        try:
            client.stop()
            client.close()
        except Exception:
            pass


@testtools.skipIf(not _zookeeper_available(), 'zookeeper is not available')
class ZkPersistenceTest(test.TestCase, base.PersistenceTestMixin):
    def _get_connection(self):
        return self.backend.get_connection()

    def _clear_all(self):
        with contextlib.closing(self._get_connection()) as conn:
            conn.clear_all()

    def setUp(self):
        super(ZkPersistenceTest, self).setUp()
        conf = TEST_CONFIG.copy()
        # Create a unique path just for this test (so that we don't overwrite
        # what other tests are doing).
        conf['path'] = TEST_PATH_TPL % (uuidutils.generate_uuid())
        try:
            self.backend = impl_zookeeper.ZkBackend(conf)
            self.addCleanup(self.backend.close)
        except Exception as e:
            self.skipTest("Failed creating backend created from configuration"
                          " %s due to %s" % (conf, e))
        with contextlib.closing(self._get_connection()) as conn:
            conn.upgrade()
            self.addCleanup(self._clear_all)
