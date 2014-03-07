# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Rackspace Hosting All Rights Reserved.
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
import os
import shutil
import tempfile

from taskflow.persistence import backends
from taskflow.persistence.backends import impl_dir
from taskflow import test
from taskflow.tests.unit.persistence import base


class DirPersistenceTest(test.TestCase, base.PersistenceTestMixin):
    def _get_connection(self):
        conf = {
            'path': self.path,
        }
        return impl_dir.DirBackend(conf).get_connection()

    def setUp(self):
        super(DirPersistenceTest, self).setUp()
        self.path = tempfile.mkdtemp()
        conn = self._get_connection()
        conn.upgrade()

    def tearDown(self):
        super(DirPersistenceTest, self).tearDown()
        conn = self._get_connection()
        conn.clear_all()
        if self.path and os.path.isdir(self.path):
            shutil.rmtree(self.path)
        self.path = None

    def _check_backend(self, conf):
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_dir.DirBackend)

    def test_dir_backend_entry_point(self):
        self._check_backend(dict(connection='dir:', path=self.path))

    def test_dir_backend_name(self):
        self._check_backend(dict(connection='dir',  # no colon
                                 path=self.path))

    def test_file_backend_entry_point(self):
        self._check_backend(dict(connection='file:', path=self.path))
