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

from redis import exceptions as redis_exceptions

from taskflow import test
from taskflow.utils import redis_utils


class TestIsServerNewEnough(test.TestCase):
    def test_is_server_new_enough(self):
        client = mock.Mock()
        client.info.return_value = {'redis_version': '2.6.1'}
        self.assertEqual(
            (True, '2.6.1'),
            redis_utils.is_server_new_enough(client, (2, 6)),
        )
        client.info.assert_called_once_with()

        client.reset_mock()
        self.assertEqual(
            (False, '2.6.1'),
            redis_utils.is_server_new_enough(client, (2, 7)),
        )
        client.info.assert_called_once_with()

    def test_is_server_new_enough_empty(self):
        client = mock.Mock()
        client.info.return_value = {}
        self.assertEqual(
            (False, ''),
            redis_utils.is_server_new_enough(client, (2, 6)),
        )
        client.info.assert_called_once_with()

        client.reset_mock()
        self.assertEqual(
            (True, ''),
            redis_utils.is_server_new_enough(client, (2, 6), True),
        )
        client.info.assert_called_once_with()

        client.reset_mock()
        self.assertEqual(
            (
                False,
                '',
            ),
            redis_utils.is_server_new_enough(client, (2, 6), False),
        )
        client.assert_not_called()

    def test_is_server_new_enough_fail(self):
        client = mock.Mock()
        client.info.side_effect = redis_exceptions.ResponseError()
        self.assertEqual(
            (False, ''),
            redis_utils.is_server_new_enough(client, (2, 6)),
        )
        client.info.assert_called_once_with()

        client.reset_mock()
        self.assertEqual(
            (True, ''),
            redis_utils.is_server_new_enough(client, (2, 6), True),
        )
        client.info.assert_called_once_with()

        client.reset_mock()
        self.assertEqual(
            (False, ''),
            redis_utils.is_server_new_enough(client, (2, 7), False),
        )
        client.info.assert_called_once_with()


class TestGetExpiry(test.TestCase):
    def test_get_expiry(self):
        client = mock.Mock()
        client.info.return_value = {'redis_version': '2.6.1'}
        client.pttl.return_value = 10.0
        self.assertEqual(0.01, redis_utils.get_expiry(client, 'foo'))
        client.pttl.assert_called_once_with('foo')
        client.ttl.assert_not_called()

    def test_get_expiry_does_not_expire(self):
        client = mock.Mock()
        client.info.return_value = {'redis_version': '2.6.1'}
        client.pttl.return_value = -1
        self.assertEqual(
            redis_utils.DOES_NOT_EXPIRE, redis_utils.get_expiry(client, 'foo')
        )
        client.pttl.assert_called_once_with('foo')
        client.ttl.assert_not_called()

    def test_get_expiry_key_not_found(self):
        client = mock.Mock()
        client.info.return_value = {'redis_version': '2.6.1'}
        client.pttl.return_value = -2
        self.assertEqual(
            redis_utils.KEY_NOT_FOUND, redis_utils.get_expiry(client, 'foo')
        )
        client.pttl.assert_called_once_with('foo')
        client.ttl.assert_not_called()

    def test_get_expiry_legacy(self):
        client = mock.Mock()
        client.info.return_value = {'redis_version': '2.5.1'}
        client.ttl.return_value = 10
        self.assertEqual(10.0, redis_utils.get_expiry(client, 'foo'))
        client.ttl.assert_called_once_with('foo')
        client.pttl.assert_not_called()

    def test_get_expiry_legacy_does_not_expire(self):
        client = mock.Mock()
        client.info.return_value = {'redis_version': '2.5.1'}
        client.ttl.return_value = -1
        self.assertEqual(
            redis_utils.DOES_NOT_EXPIRE, redis_utils.get_expiry(client, 'foo')
        )
        client.ttl.assert_called_once_with('foo')
        client.pttl.assert_not_called()

    def test_get_expiry_legacy_key_not_found(self):
        client = mock.Mock()
        client.info.return_value = {'redis_version': '2.5.1'}
        client.ttl.return_value = -2
        self.assertEqual(
            redis_utils.KEY_NOT_FOUND, redis_utils.get_expiry(client, 'foo')
        )
        client.ttl.assert_called_once_with('foo')
        client.pttl.assert_not_called()


class TestApplyExpiry(test.TestCase):
    def test_apply_expiry(self):
        client = mock.Mock()
        client.info.return_value = {'redis_version': '2.6.1'}
        client.pexpire.return_value = 1
        self.assertIs(True, redis_utils.apply_expiry(client, 'foo', 10))
        client.pexpire.assert_called_once_with('foo', 10000.0)
        client.expire.assert_not_called()

    def test_apply_expiry_negative(self):
        client = mock.Mock()
        client.info.return_value = {'redis_version': '2.6.1'}
        client.pexpire.return_value = 1
        self.assertIs(True, redis_utils.apply_expiry(client, 'foo', -10))
        client.pexpire.assert_called_once_with('foo', 0.0)
        client.expire.assert_not_called()

    def test_apply_expiry_legacy(self):
        client = mock.Mock()
        client.info.return_value = {'redis_version': '2.5.1'}
        client.expire.return_value = 1
        self.assertIs(True, redis_utils.apply_expiry(client, 'foo', 10))
        client.expire.assert_called_once_with('foo', 10)
        client.pexpire.assert_not_called()

    def test_apply_expiry_legacy_negative(self):
        client = mock.Mock()
        client.info.return_value = {'redis_version': '2.5.1'}
        client.expire.return_value = 1
        self.assertIs(True, redis_utils.apply_expiry(client, 'foo', -10))
        client.expire.assert_called_once_with('foo', 0)
        client.pexpire.assert_not_called()
