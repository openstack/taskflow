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
import tempfile

from taskflow.openstack.common.db.sqlalchemy import session
from taskflow.persistence.backends import api as b_api
from taskflow.persistence.backends.sqlalchemy import migration
from taskflow import test
from taskflow.tests.unit.persistence import base


class SqlPersistenceTest(test.TestCase, base.PersistenceTestMixin):
    """Inherits from the base test and sets up a sqlite temporary db."""
    def _get_backend(self):
        return 'sqlalchemy'

    def setupDatabase(self):
        _handle, db_location = tempfile.mkstemp()
        db_uri = "sqlite:///%s" % (db_location)
        session.set_defaults(db_uri, db_location)
        migration.db_sync()
        return db_location

    def setUp(self):
        super(SqlPersistenceTest, self).setUp()
        self.db_location = self.setupDatabase()

    def tearDown(self):
        b_api.fetch(self._get_backend()).clear_all()
        super(SqlPersistenceTest, self).tearDown()
        if self.db_location and os.path.isfile(self.db_location):
            os.unlink(self.db_location)
            self.db_location = None
