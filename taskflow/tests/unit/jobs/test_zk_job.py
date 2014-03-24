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
import mock

import six

from zake import fake_client
from zake import utils as zake_utils

from taskflow import exceptions as excp
from taskflow.jobs.backends import impl_zookeeper
from taskflow import states
from taskflow import test

from taskflow.openstack.common import jsonutils
from taskflow.persistence.backends import impl_dir
from taskflow.utils import misc
from taskflow.utils import persistence_utils as p_utils


@contextlib.contextmanager
def connect_close(*args):
    try:
        for a in args:
            a.connect()
        yield
    finally:
        for a in args:
            a.close()


def create_board(**kwargs):
    client = fake_client.FakeClient()
    board = impl_zookeeper.ZookeeperJobBoard('test-board',
                                             conf=dict(kwargs),
                                             client=client)
    return (client, board)


class TestZookeeperJobs(test.TestCase):
    def setUp(self):
        super(TestZookeeperJobs, self).setUp()
        self.client, self.board = create_board()
        self.addCleanup(self.board.close)
        self.bad_paths = [self.board.path]
        self.bad_paths.extend(zake_utils.partition_path(self.board.path))

    def test_connect(self):
        self.assertFalse(self.board.connected)
        with connect_close(self.board):
            self.client.flush()
            self.assertTrue(self.board.connected)

    def test_posting_received_raw(self):
        book = p_utils.temporary_log_book()

        with connect_close(self.board):
            self.client.flush()
            self.assertTrue(self.board.connected)
            self.assertEqual(0, self.board.job_count)
            posted_job = self.board.post('test', book)
            self.client.flush()

            self.assertEqual(self.board, posted_job.board)
            self.assertTrue(1, self.board.job_count)
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
        self.assertIn(posted_job.uuid, path_key)
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

    def test_posting_claim(self):

        with connect_close(self.board):
            self.board.post('test', p_utils.temporary_log_book())
            self.client.flush()

            self.assertEqual(1, self.board.job_count)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.assertEqual(states.UNCLAIMED, j.state)

            self.board.claim(j, self.board.name)
            self.client.flush()
            self.assertEqual(self.board.name, self.board.find_owner(j))
            self.assertEqual(states.CLAIMED, j.state)

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(0, len(possible_jobs))

        self.assertRaisesAttrAccess(excp.NotFound, j, 'state')
        self.assertRaises(excp.NotFound,
                          self.board.consume, j, self.board.name)

    def test_posting_claim_consume(self):

        with connect_close(self.board):
            self.board.post('test', p_utils.temporary_log_book())
            self.client.flush()

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.board.claim(j, self.board.name)
            self.client.flush()

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(0, len(possible_jobs))
            self.board.consume(j, self.board.name)
            self.client.flush()

            self.assertEqual(0, len(list(self.board.iterjobs())))
            self.assertRaises(excp.NotFound,
                              self.board.consume, j, self.board.name)

    def test_posting_claim_abandon(self):

        with connect_close(self.board):
            self.board.post('test', p_utils.temporary_log_book())
            self.client.flush()

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.board.claim(j, self.board.name)
            self.client.flush()

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(0, len(possible_jobs))

            self.board.abandon(j, self.board.name)
            self.client.flush()

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))

    def test_posting_claim_diff_owner(self):

        with connect_close(self.board):
            self.board.post('test', p_utils.temporary_log_book())
            self.client.flush()

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            self.board.claim(possible_jobs[0], self.board.name)
            self.client.flush()

            possible_jobs = list(self.board.iterjobs())
            self.assertEqual(1, len(possible_jobs))
            self.assertRaises(excp.UnclaimableJob, self.board.claim,
                              possible_jobs[0], self.board.name + "-1")
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(0, len(possible_jobs))

    def test_posting_state_lock_lost(self):

        with connect_close(self.board):
            j = self.board.post('test', p_utils.temporary_log_book())
            self.client.flush()
            self.assertEqual(states.UNCLAIMED, j.state)
            self.board.claim(j, self.board.name)
            self.client.flush()
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

    def test_posting_no_post(self):

        def bad_format(job):
            raise UnicodeError("Could not format")

        with connect_close(self.board):
            with mock.patch.object(self.board, '_format_job', bad_format):
                self.assertRaises(UnicodeError, self.board.post,
                                  'test', p_utils.temporary_log_book())
            self.assertEqual(0, self.board.job_count)

    def test_posting_owner_lost(self):

        with connect_close(self.board):
            j = self.board.post('test', p_utils.temporary_log_book())
            self.client.flush()
            self.assertEqual(states.UNCLAIMED, j.state)
            self.board.claim(j, self.board.name)
            self.client.flush()
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

    def test_posting_with_book(self):
        backend = impl_dir.DirBackend(conf={
            'path': self.makeTmpDir(),
        })
        backend.get_connection().upgrade()
        book, flow_detail = p_utils.temporary_flow_detail(backend)
        self.assertEqual(1, len(book))

        client, board = create_board(persistence=backend)
        self.addCleanup(board.close)

        with connect_close(board):
            board.post('test', book)
            client.flush()

            possible_jobs = list(board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.assertEqual(1, len(j.book))
            self.assertEqual(book.name, j.book.name)
            self.assertEqual(book.uuid, j.book.uuid)

            flow_details = list(j.book)
            self.assertEqual(flow_detail.uuid, flow_details[0].uuid)
            self.assertEqual(flow_detail.name, flow_details[0].name)

    def test_posting_abandon_no_owner(self):

        with connect_close(self.board):
            self.board.post('test', p_utils.temporary_log_book())
            self.client.flush()

            self.assertEqual(1, self.board.job_count)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.assertRaises(excp.JobFailure, self.board.abandon, j, j.name)
