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

from oslo_serialization import jsonutils
from oslo_utils import uuidutils
import six
import testtools
from zake import fake_client
from zake import utils as zake_utils

from taskflow.jobs.backends import impl_zookeeper
from taskflow import states
from taskflow import test
from taskflow.tests.unit.jobs import base
from taskflow.tests import utils as test_utils
from taskflow.utils import kazoo_utils
from taskflow.utils import misc
from taskflow.utils import persistence_utils as p_utils


TEST_PATH_TPL = '/taskflow/board-test/%s'
_ZOOKEEPER_AVAILABLE = test_utils.zookeeper_available(
    impl_zookeeper.MIN_ZK_VERSION)


@testtools.skipIf(not _ZOOKEEPER_AVAILABLE, 'zookeeper is not available')
class ZookeeperJobboardTest(test.TestCase, base.BoardTestMixin):
    def _create_board(self, persistence=None):

        def cleanup_path(client, path):
            if not client.connected:
                return
            client.delete(path, recursive=True)

        client = kazoo_utils.make_client(test_utils.ZK_TEST_CONFIG.copy())
        path = TEST_PATH_TPL % (uuidutils.generate_uuid())
        board = impl_zookeeper.ZookeeperJobBoard('test-board', {'path': path},
                                                 client=client,
                                                 persistence=persistence)
        self.addCleanup(kazoo_utils.finalize_client, client)
        self.addCleanup(cleanup_path, client, path)
        self.addCleanup(board.close)
        return (client, board)

    def setUp(self):
        super(ZookeeperJobboardTest, self).setUp()
        self.client, self.board = self._create_board()


class ZakeJobboardTest(test.TestCase, base.BoardTestMixin):
    def _create_board(self, persistence=None):
        client = fake_client.FakeClient()
        board = impl_zookeeper.ZookeeperJobBoard('test-board', {},
                                                 client=client,
                                                 persistence=persistence)
        self.addCleanup(board.close)
        self.addCleanup(kazoo_utils.finalize_client, client)
        return (client, board)

    def setUp(self):
        super(ZakeJobboardTest, self).setUp()
        self.client, self.board = self._create_board()
        self.bad_paths = [self.board.path]
        self.bad_paths.extend(zake_utils.partition_path(self.board.path))

    def test_posting_owner_lost(self):

        with base.connect_close(self.board):
            with base.flush(self.client):
                j = self.board.post('test', p_utils.temporary_log_book())
            self.assertEqual(states.UNCLAIMED, j.state)
            with base.flush(self.client):
                self.board.claim(j, self.board.name)
            self.assertEqual(states.CLAIMED, j.state)

            # Forcefully delete the owner from the backend storage to make
            # sure the job becomes unclaimed (this may happen if some admin
            # manually deletes the lock).
            paths = list(six.iteritems(self.client.storage.paths))
            for (path, value) in paths:
                if path in self.bad_paths:
                    continue
                if path.endswith('lock'):
                    value['data'] = misc.binary_encode(jsonutils.dumps({}))
            self.assertEqual(states.UNCLAIMED, j.state)

    def test_posting_state_lock_lost(self):

        with base.connect_close(self.board):
            with base.flush(self.client):
                j = self.board.post('test', p_utils.temporary_log_book())
            self.assertEqual(states.UNCLAIMED, j.state)
            with base.flush(self.client):
                self.board.claim(j, self.board.name)
            self.assertEqual(states.CLAIMED, j.state)

            # Forcefully delete the lock from the backend storage to make
            # sure the job becomes unclaimed (this may happen if some admin
            # manually deletes the lock).
            paths = list(six.iteritems(self.client.storage.paths))
            for (path, value) in paths:
                if path in self.bad_paths:
                    continue
                if path.endswith("lock"):
                    self.client.storage.pop(path)
            self.assertEqual(states.UNCLAIMED, j.state)

    def test_posting_received_raw(self):
        book = p_utils.temporary_log_book()

        with base.connect_close(self.board):
            self.assertTrue(self.board.connected)
            self.assertEqual(0, self.board.job_count)
            posted_job = self.board.post('test', book)

            self.assertEqual(self.board, posted_job.board)
            self.assertEqual(1, self.board.job_count)
            self.assertIn(posted_job.uuid, [j.uuid
                                            for j in self.board.iterjobs()])

        # Remove paths that got created due to the running process that we are
        # not interested in...
        paths = {}
        for (path, data) in six.iteritems(self.client.storage.paths):
            if path in self.bad_paths:
                continue
            paths[path] = data

        # Check the actual data that was posted.
        self.assertEqual(1, len(paths))
        path_key = list(six.iterkeys(paths))[0]
        self.assertTrue(len(paths[path_key]['data']) > 0)
        self.assertDictEqual({
            'uuid': posted_job.uuid,
            'name': posted_job.name,
            'book': {
                'name': book.name,
                'uuid': book.uuid,
            },
            'details': {},
        }, jsonutils.loads(misc.binary_decode(paths[path_key]['data'])))
