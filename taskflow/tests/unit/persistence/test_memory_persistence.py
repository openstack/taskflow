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

from taskflow.persistence import backends
from taskflow.persistence.backends import impl_memory
from taskflow import test
from taskflow.tests.unit.persistence import base


class MemoryPersistenceTest(test.TestCase, base.PersistenceTestMixin):
    def setUp(self):
        super(MemoryPersistenceTest, self).setUp()
        self._backend = impl_memory.MemoryBackend({})

    def _get_connection(self):
        return self._backend.get_connection()

    def tearDown(self):
        conn = self._get_connection()
        conn.clear_all()
        self._backend = None
        super(MemoryPersistenceTest, self).tearDown()

    def test_memory_backend_entry_point(self):
        conf = {'connection': 'memory:'}
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_memory.MemoryBackend)

    def test_memory_backend_fetch_by_name(self):
        conf = {'connection': 'memory'}  # note no colon
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_memory.MemoryBackend)
