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

import contextlib
import threading

from kazoo.protocol import paths as k_paths
from kazoo.recipe import watchers
from oslo_serialization import jsonutils
from oslo_utils import uuidutils
import testtools
from zake import fake_client
from zake import utils as zake_utils

from taskflow import exceptions as excp
from taskflow.jobs.backends import impl_zookeeper
from taskflow import states
from taskflow import test
from taskflow.test import mock
from taskflow.tests.unit.jobs import base
from taskflow.tests import utils as test_utils
from taskflow.types import entity
from taskflow.utils import kazoo_utils
from taskflow.utils import misc
from taskflow.utils import persistence_utils as p_utils

FLUSH_PATH_TPL = '/taskflow/flush-test/%s'
TEST_PATH_TPL = '/taskflow/board-test/%s'
ZOOKEEPER_AVAILABLE = test_utils.zookeeper_available(
    impl_zookeeper.ZookeeperJobBoard.MIN_ZK_VERSION)
TRASH_FOLDER = impl_zookeeper.ZookeeperJobBoard.TRASH_FOLDER
LOCK_POSTFIX = impl_zookeeper.ZookeeperJobBoard.LOCK_POSTFIX


class ZookeeperBoardTestMixin(base.BoardTestMixin):
    def close_client(self, client):
        kazoo_utils.finalize_client(client)

    @contextlib.contextmanager
    def flush(self, client, path=None):
        # This uses the linearity guarantee of zookeeper (and associated
        # libraries) to create a temporary node, wait until a watcher notifies
        # it's created, then yield back for more work, and then at the end of
        # that work delete the created node. This ensures that the operations
        # done in the yield of this context manager will be applied and all
        # watchers will have fired before this context manager exits.
        if not path:
            path = FLUSH_PATH_TPL % uuidutils.generate_uuid()
        created = threading.Event()
        deleted = threading.Event()

        def on_created(data, stat):
            if stat is not None:
                created.set()
                return False  # cause this watcher to cease to exist

        def on_deleted(data, stat):
            if stat is None:
                deleted.set()
                return False  # cause this watcher to cease to exist

        watchers.DataWatch(client, path, func=on_created)
        client.create(path, makepath=True)
        if not created.wait(test_utils.WAIT_TIMEOUT):
            raise RuntimeError("Could not receive creation of %s in"
                               " the alloted timeout of %s seconds"
                               % (path, test_utils.WAIT_TIMEOUT))
        try:
            yield
        finally:
            watchers.DataWatch(client, path, func=on_deleted)
            client.delete(path, recursive=True)
            if not deleted.wait(test_utils.WAIT_TIMEOUT):
                raise RuntimeError("Could not receive deletion of %s in"
                                   " the alloted timeout of %s seconds"
                                   % (path, test_utils.WAIT_TIMEOUT))

    def test_posting_no_post(self):
        with base.connect_close(self.board):
            with mock.patch.object(self.client, 'create') as create_func:
                create_func.side_effect = IOError("Unable to post")
                self.assertRaises(IOError, self.board.post,
                                  'test', p_utils.temporary_log_book())
            self.assertEqual(0, self.board.job_count)

    def test_board_iter(self):
        with base.connect_close(self.board):
            it = self.board.iterjobs()
            self.assertEqual(self.board, it.board)
            self.assertFalse(it.only_unclaimed)
            self.assertFalse(it.ensure_fresh)

    @mock.patch("taskflow.jobs.backends.impl_zookeeper.misc."
                "millis_to_datetime")
    def test_posting_dates(self, mock_dt):
        epoch = misc.millis_to_datetime(0)
        mock_dt.return_value = epoch

        with base.connect_close(self.board):
            j = self.board.post('test', p_utils.temporary_log_book())
            self.assertEqual(epoch, j.created_on)
            self.assertEqual(epoch, j.last_modified)

        self.assertTrue(mock_dt.called)


@testtools.skipIf(not ZOOKEEPER_AVAILABLE, 'zookeeper is not available')
class ZookeeperJobboardTest(test.TestCase, ZookeeperBoardTestMixin):
    def create_board(self, persistence=None):

        def cleanup_path(client, path):
            if not client.connected:
                return
            client.delete(path, recursive=True)

        client = kazoo_utils.make_client(test_utils.ZK_TEST_CONFIG.copy())
        path = TEST_PATH_TPL % (uuidutils.generate_uuid())
        board = impl_zookeeper.ZookeeperJobBoard('test-board', {'path': path},
                                                 client=client,
                                                 persistence=persistence)
        self.addCleanup(self.close_client, client)
        self.addCleanup(cleanup_path, client, path)
        self.addCleanup(board.close)
        return (client, board)

    def setUp(self):
        super(ZookeeperJobboardTest, self).setUp()
        self.client, self.board = self.create_board()


class ZakeJobboardTest(test.TestCase, ZookeeperBoardTestMixin):
    def create_board(self, persistence=None):
        client = fake_client.FakeClient()
        board = impl_zookeeper.ZookeeperJobBoard('test-board', {},
                                                 client=client,
                                                 persistence=persistence)
        self.addCleanup(board.close)
        self.addCleanup(self.close_client, client)
        return (client, board)

    def setUp(self):
        super(ZakeJobboardTest, self).setUp()
        self.client, self.board = self.create_board()
        self.bad_paths = [self.board.path, self.board.trash_path]
        self.bad_paths.extend(zake_utils.partition_path(self.board.path))

    def test_posting_owner_lost(self):

        with base.connect_close(self.board):
            with self.flush(self.client):
                j = self.board.post('test', p_utils.temporary_log_book())
            self.assertEqual(states.UNCLAIMED, j.state)
            with self.flush(self.client):
                self.board.claim(j, self.board.name)
            self.assertEqual(states.CLAIMED, j.state)

            # Forcefully delete the owner from the backend storage to make
            # sure the job becomes unclaimed (this may happen if some admin
            # manually deletes the lock).
            paths = list(self.client.storage.paths.items())
            for (path, value) in paths:
                if path in self.bad_paths:
                    continue
                if path.endswith('lock'):
                    value['data'] = misc.binary_encode(jsonutils.dumps({}))
            self.assertEqual(states.UNCLAIMED, j.state)

    def test_posting_state_lock_lost(self):

        with base.connect_close(self.board):
            with self.flush(self.client):
                j = self.board.post('test', p_utils.temporary_log_book())
            self.assertEqual(states.UNCLAIMED, j.state)
            with self.flush(self.client):
                self.board.claim(j, self.board.name)
            self.assertEqual(states.CLAIMED, j.state)

            # Forcefully delete the lock from the backend storage to make
            # sure the job becomes unclaimed (this may happen if some admin
            # manually deletes the lock).
            paths = list(self.client.storage.paths.items())
            for (path, value) in paths:
                if path in self.bad_paths:
                    continue
                if path.endswith("lock"):
                    self.client.storage.pop(path)
            self.assertEqual(states.UNCLAIMED, j.state)

    def test_trashing_claimed_job(self):

        with base.connect_close(self.board):
            with self.flush(self.client):
                j = self.board.post('test', p_utils.temporary_log_book())
            self.assertEqual(states.UNCLAIMED, j.state)
            with self.flush(self.client):
                self.board.claim(j, self.board.name)
            self.assertEqual(states.CLAIMED, j.state)

            with self.flush(self.client):
                self.board.trash(j, self.board.name)

            trashed = []
            jobs = []
            paths = list(self.client.storage.paths.items())
            for (path, value) in paths:
                if path in self.bad_paths:
                    continue
                if path.find(TRASH_FOLDER) > -1:
                    trashed.append(path)
                elif (path.find(self.board._job_base) > -1
                        and not path.endswith(LOCK_POSTFIX)):
                    jobs.append(path)

            self.assertEqual(1, len(trashed))
            self.assertEqual(0, len(jobs))

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
        for (path, data) in self.client.storage.paths.items():
            if path in self.bad_paths:
                continue
            paths[path] = data

        # Check the actual data that was posted.
        self.assertEqual(1, len(paths))
        path_key = list(paths.keys())[0]
        self.assertTrue(len(paths[path_key]['data']) > 0)
        self.assertDictEqual({
            'uuid': posted_job.uuid,
            'name': posted_job.name,
            'book': {
                'name': book.name,
                'uuid': book.uuid,
            },
            'priority': 'NORMAL',
            'details': {},
        }, jsonutils.loads(misc.binary_decode(paths[path_key]['data'])))

    def test_register_entity(self):
        conductor_name = "conductor-abc@localhost:4123"
        entity_instance = entity.Entity("conductor",
                                        conductor_name,
                                        {})
        with base.connect_close(self.board):
            self.board.register_entity(entity_instance)
        # Check '.entity' node has been created
        self.assertIn(self.board.entity_path, self.client.storage.paths)

        conductor_entity_path = k_paths.join(self.board.entity_path,
                                             'conductor',
                                             conductor_name)
        self.assertIn(conductor_entity_path, self.client.storage.paths)
        conductor_data = (
            self.client.storage.paths[conductor_entity_path]['data'])
        self.assertTrue(len(conductor_data) > 0)
        self.assertDictEqual({
            'name': conductor_name,
            'kind': 'conductor',
            'metadata': {},
        }, jsonutils.loads(misc.binary_decode(conductor_data)))

        entity_instance_2 = entity.Entity("non-sense",
                                          "other_name",
                                          {})
        with base.connect_close(self.board):
            self.assertRaises(excp.NotImplementedError,
                              self.board.register_entity,
                              entity_instance_2)

    def test_connect_check_compatible(self):
        # Valid version
        client = fake_client.FakeClient()
        board = impl_zookeeper.ZookeeperJobBoard(
            'test-board', {'check_compatible': True},
            client=client)
        self.addCleanup(board.close)
        self.addCleanup(self.close_client, client)

        with base.connect_close(board):
            pass

        # Invalid version, no check
        client = fake_client.FakeClient(server_version=(3, 2, 0))
        board = impl_zookeeper.ZookeeperJobBoard(
            'test-board', {'check_compatible': False},
            client=client)
        self.addCleanup(board.close)
        self.addCleanup(self.close_client, client)

        with base.connect_close(board):
            pass

        # Invalid version, check_compatible=True
        client = fake_client.FakeClient(server_version=(3, 2, 0))
        board = impl_zookeeper.ZookeeperJobBoard(
            'test-board', {'check_compatible': True},
            client=client)
        self.addCleanup(board.close)
        self.addCleanup(self.close_client, client)

        self.assertRaises(excp.IncompatibleVersion, board.connect)

        # Invalid version, check_compatible='False'
        client = fake_client.FakeClient(server_version=(3, 2, 0))
        board = impl_zookeeper.ZookeeperJobBoard(
            'test-board', {'check_compatible': 'False'},
            client=client)
        self.addCleanup(board.close)
        self.addCleanup(self.close_client, client)

        with base.connect_close(board):
            pass
