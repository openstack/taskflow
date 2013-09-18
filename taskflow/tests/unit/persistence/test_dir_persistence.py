# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import os
import shutil
import tempfile

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
