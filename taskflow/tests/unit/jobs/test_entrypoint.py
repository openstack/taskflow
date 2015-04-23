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

import contextlib

from zake import fake_client

from taskflow.jobs import backends
from taskflow.jobs.backends import impl_redis
from taskflow.jobs.backends import impl_zookeeper
from taskflow import test


class BackendFetchingTest(test.TestCase):
    def test_zk_entry_point_text(self):
        conf = 'zookeeper'
        with contextlib.closing(backends.fetch('test', conf)) as be:
            self.assertIsInstance(be, impl_zookeeper.ZookeeperJobBoard)

    def test_zk_entry_point(self):
        conf = {
            'board': 'zookeeper',
        }
        with contextlib.closing(backends.fetch('test', conf)) as be:
            self.assertIsInstance(be, impl_zookeeper.ZookeeperJobBoard)

    def test_zk_entry_point_existing_client(self):
        existing_client = fake_client.FakeClient()
        conf = {
            'board': 'zookeeper',
        }
        kwargs = {
            'client': existing_client,
        }
        with contextlib.closing(backends.fetch('test', conf, **kwargs)) as be:
            self.assertIsInstance(be, impl_zookeeper.ZookeeperJobBoard)
            self.assertIs(existing_client, be._client)

    def test_redis_entry_point_text(self):
        conf = 'redis'
        with contextlib.closing(backends.fetch('test', conf)) as be:
            self.assertIsInstance(be, impl_redis.RedisJobBoard)

    def test_redis_entry_point(self):
        conf = {
            'board': 'redis',
        }
        with contextlib.closing(backends.fetch('test', conf)) as be:
            self.assertIsInstance(be, impl_redis.RedisJobBoard)
