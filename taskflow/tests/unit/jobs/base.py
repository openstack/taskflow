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
import time

from kazoo.recipe import watchers
from oslo_utils import uuidutils

from taskflow import exceptions as excp
from taskflow.persistence.backends import impl_dir
from taskflow import states
from taskflow.test import mock
from taskflow.tests import utils as test_utils
from taskflow.utils import misc
from taskflow.utils import persistence_utils as p_utils
from taskflow.utils import threading_utils

FLUSH_PATH_TPL = '/taskflow/flush-test/%s'


@contextlib.contextmanager
def connect_close(*args):
    try:
        for a in args:
            a.connect()
        yield
    finally:
        for a in args:
            a.close()


@contextlib.contextmanager
def flush(client, path=None):
    # This uses the linearity guarantee of zookeeper (and associated libraries)
    # to create a temporary node, wait until a watcher notifies it's created,
    # then yield back for more work, and then at the end of that work delete
    # the created node. This ensures that the operations done in the yield
    # of this context manager will be applied and all watchers will have fired
    # before this context manager exits.
    if not path:
        path = FLUSH_PATH_TPL % uuidutils.generate_uuid()
    created = threading_utils.Event()
    deleted = threading_utils.Event()

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


class BoardTestMixin(object):
    def test_connect(self):
        self.assertFalse(self.board.connected)
        with connect_close(self.board):
            self.assertTrue(self.board.connected)

    @mock.patch("taskflow.jobs.backends.impl_zookeeper.misc."
                "millis_to_datetime")
    def test_posting_dates(self, mock_dt):
        epoch = misc.millis_to_datetime(0)
        mock_dt.return_value = epoch

        with connect_close(self.board):
            j = self.board.post('test', p_utils.temporary_log_book())
            self.assertEqual(epoch, j.created_on)
            self.assertEqual(epoch, j.last_modified)

        self.assertTrue(mock_dt.called)

    def test_board_iter(self):
        with connect_close(self.board):
            it = self.board.iterjobs()
            self.assertEqual(it.board, self.board)
            self.assertFalse(it.only_unclaimed)
            self.assertFalse(it.ensure_fresh)

    def test_board_iter_empty(self):
        with connect_close(self.board):
            jobs_found = list(self.board.iterjobs())
            self.assertEqual([], jobs_found)

    def test_fresh_iter(self):
        with connect_close(self.board):
            book = p_utils.temporary_log_book()
            self.board.post('test', book)
            jobs = list(self.board.iterjobs(ensure_fresh=True))
            self.assertEqual(1, len(jobs))

    def test_wait_timeout(self):
        with connect_close(self.board):
            self.assertRaises(excp.NotFound, self.board.wait, timeout=0.1)

    def test_wait_arrival(self):
        ev = threading_utils.Event()
        jobs = []

        def poster(wait_post=0.2):
            if not ev.wait(test_utils.WAIT_TIMEOUT):
                raise RuntimeError("Waiter did not appear ready"
                                   " in %s seconds" % test_utils.WAIT_TIMEOUT)
            time.sleep(wait_post)
            self.board.post('test', p_utils.temporary_log_book())

        def waiter():
            ev.set()
            it = self.board.wait()
            jobs.extend(it)

        with connect_close(self.board):
            t1 = threading_utils.daemon_thread(poster)
            t1.start()
            t2 = threading_utils.daemon_thread(waiter)
            t2.start()
            for t in (t1, t2):
                t.join()

        self.assertEqual(1, len(jobs))

    def test_posting_claim(self):

        with connect_close(self.board):
            with flush(self.client):
                self.board.post('test', p_utils.temporary_log_book())

            self.assertEqual(1, self.board.job_count)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.assertEqual(states.UNCLAIMED, j.state)

            with flush(self.client):
                self.board.claim(j, self.board.name)

            self.assertEqual(self.board.name, self.board.find_owner(j))
            self.assertEqual(states.CLAIMED, j.state)

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(0, len(possible_jobs))

        self.assertRaisesAttrAccess(excp.NotFound, j, 'state')
        self.assertRaises(excp.NotFound,
                          self.board.consume, j, self.board.name)

    def test_posting_claim_consume(self):

        with connect_close(self.board):
            with flush(self.client):
                self.board.post('test', p_utils.temporary_log_book())

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            with flush(self.client):
                self.board.claim(j, self.board.name)

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(0, len(possible_jobs))
            with flush(self.client):
                self.board.consume(j, self.board.name)

            self.assertEqual(0, len(list(self.board.iterjobs())))
            self.assertRaises(excp.NotFound,
                              self.board.consume, j, self.board.name)

    def test_posting_claim_abandon(self):

        with connect_close(self.board):
            with flush(self.client):
                self.board.post('test', p_utils.temporary_log_book())

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            with flush(self.client):
                self.board.claim(j, self.board.name)

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(0, len(possible_jobs))
            with flush(self.client):
                self.board.abandon(j, self.board.name)

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))

    def test_posting_claim_diff_owner(self):

        with connect_close(self.board):
            with flush(self.client):
                self.board.post('test', p_utils.temporary_log_book())

            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            with flush(self.client):
                self.board.claim(possible_jobs[0], self.board.name)

            possible_jobs = list(self.board.iterjobs())
            self.assertEqual(1, len(possible_jobs))
            self.assertRaises(excp.UnclaimableJob, self.board.claim,
                              possible_jobs[0], self.board.name + "-1")
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(0, len(possible_jobs))

    def test_posting_no_post(self):
        with connect_close(self.board):
            with mock.patch.object(self.client, 'create') as create_func:
                create_func.side_effect = IOError("Unable to post")
                self.assertRaises(IOError, self.board.post,
                                  'test', p_utils.temporary_log_book())
            self.assertEqual(0, self.board.job_count)

    def test_posting_with_book(self):
        backend = impl_dir.DirBackend(conf={
            'path': self.makeTmpDir(),
        })
        backend.get_connection().upgrade()
        book, flow_detail = p_utils.temporary_flow_detail(backend)
        self.assertEqual(1, len(book))

        client, board = self._create_board(persistence=backend)
        with connect_close(board):
            with flush(client):
                board.post('test', book)

            possible_jobs = list(board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.assertEqual(1, len(j.book))
            self.assertEqual(book.name, j.book.name)
            self.assertEqual(book.uuid, j.book.uuid)
            self.assertEqual(book.name, j.book_name)
            self.assertEqual(book.uuid, j.book_uuid)

            flow_details = list(j.book)
            self.assertEqual(flow_detail.uuid, flow_details[0].uuid)
            self.assertEqual(flow_detail.name, flow_details[0].name)

    def test_posting_abandon_no_owner(self):

        with connect_close(self.board):
            with flush(self.client):
                self.board.post('test', p_utils.temporary_log_book())

            self.assertEqual(1, self.board.job_count)
            possible_jobs = list(self.board.iterjobs(only_unclaimed=True))
            self.assertEqual(1, len(possible_jobs))
            j = possible_jobs[0]
            self.assertRaises(excp.JobFailure, self.board.abandon, j, j.name)
