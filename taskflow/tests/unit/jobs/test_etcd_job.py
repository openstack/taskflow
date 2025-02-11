#    Copyright (C) Red Hat
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

from unittest import mock

from oslo_serialization import jsonutils
from oslo_utils import uuidutils
import testtools

from taskflow import exceptions as exc
from taskflow.jobs.backends import impl_etcd
from taskflow.jobs import base as jobs_base
from taskflow import test
from taskflow.tests.unit.jobs import base
from taskflow.tests import utils as test_utils

ETCD_AVAILABLE = test_utils.etcd_available()
ETCD_PORT = test_utils.ETCD_PORT


class EtcdJobBoardMixin:
    def create_board(self, conf=None, persistence=None):
        self.path = f"test-{uuidutils.generate_uuid()}"
        board_conf = {
            "port": ETCD_PORT,
            "path": self.path,
        }
        if conf:
            board_conf.update(conf)
        board = impl_etcd.EtcdJobBoard("etcd", board_conf,
                                       persistence=persistence)
        return board._client, board


class MockedEtcdJobBoard(test.TestCase, EtcdJobBoardMixin):

    def test_create_board(self):
        _, jobboard = self.create_board()
        self.assertEqual(f"/taskflow/jobs/{self.path}", jobboard._root_path)

        _, jobboard = self.create_board({"path": "/testpath"})
        self.assertEqual("/taskflow/jobs/testpath", jobboard._root_path)

    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard.incr")
    @mock.patch("threading.Condition")
    @mock.patch("oslo_utils.uuidutils.generate_uuid")
    @mock.patch("oslo_utils.timeutils.utcnow")
    def test_post(self,
                  mock_utcnow: mock.Mock,
                  mock_generated_uuid: mock.Mock,
                  mock_cond: mock.Mock,
                  mock_incr: mock.Mock):
        mock_incr.return_value = 12
        mock_generated_uuid.return_value = "uuid1"
        mock_utcnow.return_value = "utcnow1"

        mock_book = mock.Mock()
        mock_book.name = "book1_name"
        mock_book.uuid = "book1_uuid"
        mock_details = mock.Mock()

        _, jobboard = self.create_board()
        jobboard._client = mock.Mock()
        job = jobboard.post("post1", book=mock_book,
                            details=mock_details,
                            priority=jobs_base.JobPriority.NORMAL)

        expected_key = (
            f"/taskflow/jobs/{self.path}/job12")
        expected_data_key = expected_key + jobboard.DATA_POSTFIX
        expected_book_data = {
            "name": "book1_name",
            "uuid": "book1_uuid"
        }
        expected_job_posting = {
            "uuid": "uuid1",
            "name": "post1",
            "priority": "NORMAL",
            "created_on": "utcnow1",
            "details": mock_details,
            "book": expected_book_data,
            "sequence": 12,
        }

        mock_incr.assert_called_with(f"/taskflow/jobs/{self.path}/sequence")

        jobboard._client.create.assert_called_with(
            expected_data_key, jsonutils.dumps(expected_job_posting))

        self.assertEqual("post1", job.name)
        self.assertEqual(expected_key, job.key)
        self.assertEqual(mock_details, job.details)
        self.assertEqual(mock_book, job.book)
        self.assertEqual(expected_book_data, job._book_data)
        self.assertEqual(jobs_base.JobPriority.NORMAL, job.priority)
        self.assertEqual(12, job.sequence)

        self.assertEqual(1, len(jobboard._job_cache))
        self.assertEqual(job, jobboard._job_cache[expected_key])

    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "set_last_modified")
    def test_claim(self, mock_set_last_modified):
        who = "owner1"
        lease_id = uuidutils.generate_uuid()

        _, jobboard = self.create_board(conf={"ttl": 37})
        jobboard._client = mock.Mock()

        mock_lease = mock.Mock(id=lease_id)
        jobboard._client.lease.return_value = mock_lease
        jobboard._client.create.return_value = True
        jobboard._client.get.return_value = [mock.Mock()]

        job = impl_etcd.EtcdJob(jobboard,
                                "job7",
                                jobboard._client,
                                f"/taskflow/jobs/{self.path}/job7",
                                uuid=uuidutils.generate_uuid(),
                                details=mock.Mock(),
                                backend="etcd",
                                book=mock.Mock(),
                                book_data=mock.Mock(),
                                priority=jobs_base.JobPriority.NORMAL,
                                sequence=7,
                                created_on="date")

        jobboard.claim(job, who)

        jobboard._client.lease.assert_called_once_with(ttl=37)

        jobboard._client.create.assert_called_once_with(
            f"{job.key}{jobboard.LOCK_POSTFIX}",
            jsonutils.dumps({"owner": who,
                             "lease_id": lease_id}),
            lease=mock_lease)

        jobboard._client.get.assert_called_once_with(
            job.key + jobboard.DATA_POSTFIX)
        mock_lease.revoke.assert_not_called()

        mock_set_last_modified.assert_called_once_with(job)

    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "set_last_modified")
    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "find_owner")
    def test_claim_already_claimed(self, mock_find_owner,
                                   mock_set_last_modified):
        who = "owner1"
        lease_id = uuidutils.generate_uuid()

        mock_find_owner.return_value = who

        _, jobboard = self.create_board({"ttl": 37})
        jobboard._client = mock.Mock()

        mock_lease = mock.Mock(id=lease_id)
        jobboard._client.lease.return_value = mock_lease
        jobboard._client.create.return_value = False
        jobboard._client.get.return_value = []

        job = impl_etcd.EtcdJob(jobboard,
                                "job7",
                                jobboard._client,
                                f"/taskflow/jobs/{self.path}/job7",
                                uuid=uuidutils.generate_uuid(),
                                details=mock.Mock(),
                                backend="etcd",
                                book=mock.Mock(),
                                book_data=mock.Mock(),
                                priority=jobs_base.JobPriority.NORMAL,
                                sequence=7,
                                created_on="date")

        self.assertRaisesRegex(exc.UnclaimableJob, "already claimed by",
                               jobboard.claim, job, who)

        jobboard._client.lease.assert_called_once_with(ttl=37)

        jobboard._client.create.assert_called_once_with(
            f"{job.key}{jobboard.LOCK_POSTFIX}",
            jsonutils.dumps({"owner": who,
                             "lease_id": lease_id}),
            lease=mock_lease)

        mock_lease.revoke.assert_called_once()

        mock_set_last_modified.assert_not_called()

    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "set_last_modified")
    def test_claim_deleted(self, mock_set_last_modified):
        who = "owner1"
        lease_id = uuidutils.generate_uuid()

        _, jobboard = self.create_board({"ttl": 37})
        jobboard._client = mock.Mock()

        mock_lease = mock.Mock(id=lease_id)
        jobboard._client.lease.return_value = mock_lease
        jobboard._client.create.return_value = True
        jobboard._client.get.return_value = []

        job = impl_etcd.EtcdJob(jobboard,
                                "job7",
                                jobboard._client,
                                f"/taskflow/jobs/{self.path}/job7",
                                uuid=uuidutils.generate_uuid(),
                                details=mock.Mock(),
                                backend="etcd",
                                book=mock.Mock(),
                                book_data=mock.Mock(),
                                priority=jobs_base.JobPriority.NORMAL,
                                sequence=7,
                                created_on="date")

        self.assertRaisesRegex(exc.UnclaimableJob, "already deleted",
                               jobboard.claim, job, who)

        jobboard._client.lease.assert_called_once_with(ttl=37)

        jobboard._client.create.assert_called_once_with(
            f"{job.key}{jobboard.LOCK_POSTFIX}",
            jsonutils.dumps({"owner": who,
                             "lease_id": lease_id}),
            lease=mock_lease)

        jobboard._client.get.assert_called_once_with(
            job.key + jobboard.DATA_POSTFIX)
        mock_lease.revoke.assert_called_once()

        mock_set_last_modified.assert_not_called()

    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "get_owner_and_data")
    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "_remove_job_from_cache")
    def test_consume(self, mock__remove_job_from_cache,
                     mock_get_owner_and_data):
        mock_get_owner_and_data.return_value = ["owner1", mock.Mock()]

        _, jobboard = self.create_board()
        jobboard._client = mock.Mock()

        job = impl_etcd.EtcdJob(jobboard,
                                "job7",
                                jobboard._client,
                                f"/taskflow/jobs/{self.path}/job7")
        jobboard.consume(job, "owner1")

        jobboard._client.delete_prefix.assert_called_once_with(job.key + ".")
        mock__remove_job_from_cache.assert_called_once_with(job.key)

    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "get_owner_and_data")
    def test_consume_bad_owner(self, mock_get_owner_and_data):
        mock_get_owner_and_data.return_value = ["owner2", mock.Mock()]

        _, jobboard = self.create_board()
        jobboard._client = mock.Mock()

        job = impl_etcd.EtcdJob(jobboard,
                                "job7",
                                jobboard._client,
                                f"/taskflow/jobs/{self.path}/job7")
        self.assertRaisesRegex(exc.JobFailure, "which is not owned",
                               jobboard.consume, job, "owner1")

        jobboard._client.delete_prefix.assert_not_called()

    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "get_owner_and_data")
    def test_abandon(self, mock_get_owner_and_data):
        mock_get_owner_and_data.return_value = ["owner1", mock.Mock()]

        _, jobboard = self.create_board()
        jobboard._client = mock.Mock()

        job = impl_etcd.EtcdJob(jobboard,
                                "job7",
                                jobboard._client,
                                f"/taskflow/jobs/{self.path}/job7")
        jobboard.abandon(job, "owner1")

        jobboard._client.delete.assert_called_once_with(
            f"{job.key}{jobboard.LOCK_POSTFIX}")

    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "get_owner_and_data")
    def test_abandon_bad_owner(self, mock_get_owner_and_data):
        mock_get_owner_and_data.return_value = ["owner2", mock.Mock()]

        _, jobboard = self.create_board()
        jobboard._client = mock.Mock()

        job = impl_etcd.EtcdJob(jobboard,
                                "job7",
                                jobboard._client,
                                f"/taskflow/jobs/{self.path}/job7")
        self.assertRaisesRegex(exc.JobFailure, "which is not owned",
                               jobboard.abandon, job, "owner1")

        jobboard._client.delete.assert_not_called()

    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "get_owner_and_data")
    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "_remove_job_from_cache")
    def test_trash(self, mock__remove_job_from_cache,
                   mock_get_owner_and_data):
        mock_get_owner_and_data.return_value = ["owner1", mock.Mock()]

        _, jobboard = self.create_board()
        jobboard._client = mock.Mock()

        job = impl_etcd.EtcdJob(jobboard,
                                "job7",
                                jobboard._client,
                                f"/taskflow/jobs/{self.path}/job7")
        jobboard.trash(job, "owner1")

        jobboard._client.create.assert_called_once_with(
            f"/taskflow/.trash/{self.path}/job7", mock.ANY)
        jobboard._client.delete_prefix.assert_called_once_with(job.key + ".")
        mock__remove_job_from_cache.assert_called_once_with(job.key)

    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "get_owner_and_data")
    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "_remove_job_from_cache")
    def test_trash_bad_owner(self, mock__remove_job_from_cache,
                             mock_get_owner_and_data):
        mock_get_owner_and_data.return_value = ["owner2", mock.Mock()]

        _, jobboard = self.create_board()
        jobboard._client = mock.Mock()

        job = impl_etcd.EtcdJob(jobboard,
                                "job7",
                                jobboard._client,
                                f"/taskflow/jobs/{self.path}/job7")
        self.assertRaisesRegex(exc.JobFailure, "which is not owned",
                               jobboard.trash, job, "owner1")

        jobboard._client.create.assert_not_called()
        jobboard._client.delete_prefix.assert_not_called()
        mock__remove_job_from_cache.assert_not_called()

    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "get_owner_and_data")
    @mock.patch("taskflow.jobs.backends.impl_etcd.EtcdJobBoard."
                "_remove_job_from_cache")
    def test_trash_deleted_job(self, mock__remove_job_from_cache,
                               mock_get_owner_and_data):
        mock_get_owner_and_data.return_value = ["owner1", None]

        _, jobboard = self.create_board()
        jobboard._client = mock.Mock()

        job = impl_etcd.EtcdJob(jobboard,
                                "job7",
                                jobboard._client,
                                f"/taskflow/jobs/{self.path}/job7")
        self.assertRaisesRegex(exc.NotFound, "Cannot find job",
                               jobboard.trash, job, "owner1")

        jobboard._client.create.assert_not_called()
        jobboard._client.delete_prefix.assert_not_called()
        mock__remove_job_from_cache.assert_not_called()


@testtools.skipIf(not ETCD_AVAILABLE, 'Etcd is not available')
class EtcdJobBoardTest(test.TestCase, base.BoardTestMixin, EtcdJobBoardMixin):
    def setUp(self):
        super().setUp()
        self.client, self.board = self.create_board()

    def test__incr(self):
        key = uuidutils.generate_uuid()

        self.board.connect()
        self.addCleanup(self.board.close)
        self.addCleanup(self.board._client.delete, key)

        self.assertEqual(1, self.board.incr(key))
        self.assertEqual(2, self.board.incr(key))
        self.assertEqual(3, self.board.incr(key))

        self.assertEqual(b'3', self.board.get_one(key))
        self.board.close()

    def test_get_one(self):
        key1 = uuidutils.generate_uuid()

        self.board.connect()
        self.addCleanup(self.board._client.delete, key1)

        # put data and get it
        self.board._client.put(key1, "testset1")
        self.assertEqual(b"testset1", self.board.get_one(key1))

        # delete data and check that it's not found
        self.board._client.delete(key1)
        self.assertIsNone(self.board.get_one(key1))

        # get a non-existant data
        key2 = uuidutils.generate_uuid()
        # (ensure it doesn't exist)
        self.board._client.delete(key2)
        self.assertIsNone(self.board.get_one(key2))
        self.board.close()
