# -*- coding: utf-8 -*-

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

from taskflow import test
from taskflow.utils import kazoo_utils


class MakeClientTest(test.TestCase):

    @mock.patch("kazoo.client.KazooClient")
    def test_make_client_config(self, mock_kazoo_client):
        conf = {}
        expected = {
            'hosts': 'localhost:2181',
            'logger': mock.ANY,
            'read_only': False,
            'randomize_hosts': False,
            'keyfile': None,
            'keyfile_password': None,
            'certfile': None,
            'use_ssl': False,
            'verify_certs': True
        }

        kazoo_utils.make_client(conf)

        mock_kazoo_client.assert_called_once_with(**expected)

        mock_kazoo_client.reset_mock()

        # With boolean passed as strings
        conf = {
            'use_ssl': 'True',
            'verify_certs': 'False'
        }
        expected = {
            'hosts': 'localhost:2181',
            'logger': mock.ANY,
            'read_only': False,
            'randomize_hosts': False,
            'keyfile': None,
            'keyfile_password': None,
            'certfile': None,
            'use_ssl': True,
            'verify_certs': False
        }

        kazoo_utils.make_client(conf)

        mock_kazoo_client.assert_called_once_with(**expected)

        mock_kazoo_client.reset_mock()
