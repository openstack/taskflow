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

from oslo_utils import uuidutils
import testscenarios

from taskflow import exceptions as exc
from taskflow.persistence import backends
from taskflow.persistence.backends import impl_dir
from taskflow.persistence import models
from taskflow import test
from taskflow.tests.unit.persistence import base


class DirPersistenceTest(testscenarios.TestWithScenarios,
                         test.TestCase, base.PersistenceTestMixin):

    scenarios = [
        ('no_cache', {'max_cache_size': None}),
        ('one', {'max_cache_size': 1}),
        ('tiny', {'max_cache_size': 256}),
        ('medimum', {'max_cache_size': 512}),
        ('large', {'max_cache_size': 1024}),
    ]

    def _get_connection(self):
        return self.backend.get_connection()

    def setUp(self):
        super(DirPersistenceTest, self).setUp()
        self.path = tempfile.mkdtemp()
        self.backend = impl_dir.DirBackend({
            'path': self.path,
            'max_cache_size': self.max_cache_size,
        })
        with contextlib.closing(self._get_connection()) as conn:
            conn.upgrade()

    def tearDown(self):
        super(DirPersistenceTest, self).tearDown()
        if self.path and os.path.isdir(self.path):
            shutil.rmtree(self.path)
        self.path = None
        self.backend = None

    def _check_backend(self, conf):
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_dir.DirBackend)

    def test_dir_backend_invalid_cache_size(self):
        for invalid_size in [-1024, 0, -1]:
            conf = {
                'path': self.path,
                'max_cache_size': invalid_size,
            }
            self.assertRaises(ValueError, impl_dir.DirBackend, conf)

    def test_dir_backend_cache_overfill(self):
        if self.max_cache_size is not None:
            # Ensure cache never goes past the desired max size...
            books_ids_made = []
            with contextlib.closing(self._get_connection()) as conn:
                for i in range(0, int(1.5 * self.max_cache_size)):
                    lb_name = 'book-%s' % (i)
                    lb_id = uuidutils.generate_uuid()
                    lb = models.LogBook(name=lb_name, uuid=lb_id)
                    self.assertRaises(exc.NotFound, conn.get_logbook, lb_id)
                    conn.save_logbook(lb)
                    books_ids_made.append(lb_id)
                    self.assertLessEqual(self.backend.file_cache.currsize,
                                         self.max_cache_size)
            # Also ensure that we can still read all created books...
            with contextlib.closing(self._get_connection()) as conn:
                for lb_id in books_ids_made:
                    lb = conn.get_logbook(lb_id)
                    self.assertIsNotNone(lb)

    def test_dir_backend_entry_point(self):
        self._check_backend(dict(connection='dir:', path=self.path))

    def test_dir_backend_name(self):
        self._check_backend(dict(connection='dir',  # no colon
                                 path=self.path))

    def test_file_backend_entry_point(self):
        self._check_backend(dict(connection='file:', path=self.path))
