# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

import time
from unittest import mock

from oslo_utils import uuidutils
import testtools

from taskflow import exceptions as excp
from taskflow.jobs.backends import impl_redis
from taskflow import states
from taskflow import test
from taskflow.tests.unit.jobs import base
from taskflow.tests import utils as test_utils
from taskflow.utils import persistence_utils as p_utils
from taskflow.utils import redis_utils as ru


REDIS_AVAILABLE = test_utils.redis_available(
    impl_redis.RedisJobBoard.MIN_REDIS_VERSION)
REDIS_PORT = test_utils.REDIS_PORT


@testtools.skipIf(not REDIS_AVAILABLE, 'redis is not available')
class RedisJobboardTest(test.TestCase, base.BoardTestMixin):
    def close_client(self, client):
        client.close()

    def create_board(self, persistence=None):
        namespace = uuidutils.generate_uuid()
        client = ru.RedisClient(port=REDIS_PORT)
        config = {
            'namespace': ("taskflow-%s" % namespace).encode('latin-1'),
        }
        kwargs = {
            'client': client,
            'persistence': persistence,
        }
        board = impl_redis.RedisJobBoard('test-board', config, **kwargs)
        self.addCleanup(board.close)
        self.addCleanup(self.close_client, client)
        return (client, board)

    def test_posting_claim_expiry(self):

        with base.connect_close(self.board):
            with self.flush(self.client):
                self.board.post('test', p_utils.temporary_log_book())

            self.assertEqual(1, self.board.job_count)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.assertEqual(states.UNCLAIMED, j.state)

            with self.flush(self.client):
                self.board.claim(j, self.board.name, expiry=0.5)

            self.assertEqual(self.board.name, self.board.find_owner(j))
            self.assertEqual(states.CLAIMED, j.state)

            time.sleep(0.6)
            self.assertEqual(states.UNCLAIMED, j.state)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))

    def test_posting_claim_same_owner(self):
        with base.connect_close(self.board):
            with self.flush(self.client):
                self.board.post('test', p_utils.temporary_log_book())

            self.assertEqual(1, self.board.job_count)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.assertEqual(states.UNCLAIMED, j.state)

            with self.flush(self.client):
                self.board.claim(j, self.board.name)

            possible_jobs = list(self.board.iterjobs())
            self.assertEqual(1, len(possible_jobs))
            with self.flush(self.client):
                self.assertRaises(excp.UnclaimableJob, self.board.claim,
                                  possible_jobs[0], self.board.name)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(0, len(possible_jobs))

    def setUp(self):
        super(RedisJobboardTest, self).setUp()
        self.client, self.board = self.create_board()

    def test__make_client(self):
        conf = {'host': '127.0.0.1',
                'port': 6379,
                'username': 'default',
                'password': 'secret',
                'namespace': 'test'
                }
        test_conf = {
            'host': '127.0.0.1',
            'port': 6379,
            'username': 'default',
            'password': 'secret',
        }
        with mock.patch('taskflow.utils.redis_utils.RedisClient') as mock_ru:
            impl_redis.RedisJobBoard('test-board', conf)
            mock_ru.assert_called_once_with(**test_conf)

    def test__make_client_sentinel(self):
        conf = {'host': '127.0.0.1',
                'port': 26379,
                'username': 'default',
                'password': 'secret',
                'namespace': 'test',
                'sentinel': 'mymaster',
                'sentinel_kwargs': {
                    'username': 'default',
                    'password': 'senitelsecret'
                }}
        with mock.patch('redis.sentinel.Sentinel') as mock_sentinel:
            impl_redis.RedisJobBoard('test-board', conf)
            test_conf = {
                'username': 'default',
                'password': 'secret',
            }
            mock_sentinel.assert_called_once_with(
                [('127.0.0.1', 26379)],
                sentinel_kwargs={
                    'username': 'default',
                    'password': 'senitelsecret'
                },
                **test_conf)
            mock_sentinel().master_for.assert_called_once_with('mymaster')

    def test__make_client_sentinel_fallbacks(self):
        conf = {'host': '127.0.0.1',
                'port': 26379,
                'username': 'default',
                'password': 'secret',
                'namespace': 'test',
                'sentinel': 'mymaster',
                'sentinel_fallbacks': [
                    '[::1]:26379', '127.0.0.2:26379', 'localhost:26379'
                ]}
        with mock.patch('redis.sentinel.Sentinel') as mock_sentinel:
            impl_redis.RedisJobBoard('test-board', conf)
            test_conf = {
                'username': 'default',
                'password': 'secret',
                'sentinel_kwargs': None,
            }
            mock_sentinel.assert_called_once_with(
                [('127.0.0.1', 26379), ('::1', 26379),
                 ('127.0.0.2', 26379), ('localhost', 26379)],
                **test_conf)
            mock_sentinel().master_for.assert_called_once_with('mymaster')

    def test__make_client_sentinel_ssl(self):
        conf = {'host': '127.0.0.1',
                'port': 26379,
                'username': 'default',
                'password': 'secret',
                'namespace': 'test',
                'sentinel': 'mymaster',
                'sentinel_kwargs': None,
                'ssl': True,
                'ssl_ca_certs': '/etc/ssl/certs'}
        with mock.patch('redis.sentinel.Sentinel') as mock_sentinel:
            impl_redis.RedisJobBoard('test-board', conf)
            test_conf = {
                'username': 'default',
                'password': 'secret',
                'ssl': True,
                'ssl_ca_certs': '/etc/ssl/certs',
            }
            mock_sentinel.assert_called_once_with(
                [('127.0.0.1', 26379)],
                sentinel_kwargs=None,
                **test_conf)
            mock_sentinel().master_for.assert_called_once_with('mymaster')
