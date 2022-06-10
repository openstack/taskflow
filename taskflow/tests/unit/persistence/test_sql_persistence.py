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

import abc
import contextlib
import os
import random
import tempfile

import testtools


# NOTE(harlowja): by default this will test against sqlite using a temporary
# sqlite file (this is done instead of in-memory to ensure thread safety,
# in-memory sqlite is not thread safe).
#
# There are also "opportunistic" tests for both mysql and postgresql in here,
# which allows testing against all 3 databases (sqlite, mysql, postgres) in
# a properly configured unit test environment. For the opportunistic testing
# you need to set up a db user 'openstack_citest' with password
# 'openstack_citest' that has the permissions to create databases on
# localhost.

USER = "openstack_citest"
PASSWD = "openstack_citest"
DATABASE = "tftest_" + ''.join(random.choice('0123456789')
                               for _ in range(12))

import sqlalchemy as sa

from taskflow.persistence import backends
from taskflow.persistence.backends import impl_sqlalchemy
from taskflow import test
from taskflow.tests.unit.persistence import base


def _get_connect_string(backend, user, passwd, database=None, variant=None):
    """Forms a sqlalchemy database uri string for the given values."""
    if backend == "postgres":
        if not variant:
            variant = 'psycopg2'
        backend = "postgresql+%s" % (variant)
    elif backend == "mysql":
        if not variant:
            variant = 'pymysql'
        backend = "mysql+%s" % (variant)
    else:
        raise Exception("Unrecognized backend: '%s'" % backend)
    if not database:
        database = ''
    return "%s://%s:%s@localhost/%s" % (backend, user, passwd, database)


def _mysql_exists():
    engine = None
    try:
        db_uri = _get_connect_string('mysql', USER, PASSWD)
        engine = sa.create_engine(db_uri)
        with contextlib.closing(engine.connect()):
            return True
    except Exception:
        pass
    finally:
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                pass
    return False


def _postgres_exists():
    engine = None
    try:
        db_uri = _get_connect_string('postgres', USER, PASSWD, 'postgres')
        engine = sa.create_engine(db_uri)
        with contextlib.closing(engine.connect()):
            return True
    except Exception:
        return False
    finally:
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                pass


class SqlitePersistenceTest(test.TestCase, base.PersistenceTestMixin):
    """Inherits from the base test and sets up a sqlite temporary db."""
    def _get_connection(self):
        conf = {
            'connection': self.db_uri,
        }
        return impl_sqlalchemy.SQLAlchemyBackend(conf).get_connection()

    def setUp(self):
        super(SqlitePersistenceTest, self).setUp()
        self.db_location = tempfile.mktemp(suffix='.db')
        self.db_uri = "sqlite:///%s" % (self.db_location)
        # Ensure upgraded to the right schema
        with contextlib.closing(self._get_connection()) as conn:
            conn.upgrade()

    def tearDown(self):
        super(SqlitePersistenceTest, self).tearDown()
        if self.db_location and os.path.isfile(self.db_location):
            os.unlink(self.db_location)
            self.db_location = None


class BackendPersistenceTestMixin(base.PersistenceTestMixin,
                                  metaclass=abc.ABCMeta):
    """Specifies a backend type and does required setup and teardown."""

    def _get_connection(self):
        return self.backend.get_connection()

    def test_entrypoint(self):
        # Test that the entrypoint fetching also works (even with dialects)
        # using the same configuration we used in setUp() but not using
        # the impl_sqlalchemy SQLAlchemyBackend class directly...
        with contextlib.closing(backends.fetch(self.db_conf)) as backend:
            with contextlib.closing(backend.get_connection()):
                pass

    @abc.abstractmethod
    def _init_db(self):
        """Sets up the database, and returns the uri to that database."""

    @abc.abstractmethod
    def _remove_db(self):
        """Cleans up by removing the database once the tests are done."""

    def setUp(self):
        super(BackendPersistenceTestMixin, self).setUp()
        self.backend = None
        try:
            self.db_uri = self._init_db()
            self.db_conf = {
                'connection': self.db_uri
            }
            # Since we are using random database names, we need to make sure
            # and remove our random database when we are done testing.
            self.addCleanup(self._remove_db)
        except Exception as e:
            self.skipTest("Failed to create temporary database;"
                          " testing being skipped due to: %s" % (e))
        else:
            self.backend = impl_sqlalchemy.SQLAlchemyBackend(self.db_conf)
            self.addCleanup(self.backend.close)
            with contextlib.closing(self._get_connection()) as conn:
                conn.upgrade()


@testtools.skipIf(not _mysql_exists(), 'mysql is not available')
class MysqlPersistenceTest(BackendPersistenceTestMixin, test.TestCase):

    def _init_db(self):
        engine = None
        try:
            db_uri = _get_connect_string('mysql', USER, PASSWD)
            engine = sa.create_engine(db_uri)
            with contextlib.closing(engine.connect()) as conn:
                conn.execute("CREATE DATABASE %s" % DATABASE)
        except Exception as e:
            raise Exception('Failed to initialize MySQL db: %s' % (e))
        finally:
            if engine is not None:
                try:
                    engine.dispose()
                except Exception:
                    pass
        return _get_connect_string('mysql', USER, PASSWD,
                                   database=DATABASE)

    def _remove_db(self):
        engine = None
        try:
            engine = sa.create_engine(self.db_uri)
            with contextlib.closing(engine.connect()) as conn:
                conn.execute("DROP DATABASE IF EXISTS %s" % DATABASE)
        except Exception as e:
            raise Exception('Failed to remove temporary database: %s' % (e))
        finally:
            if engine is not None:
                try:
                    engine.dispose()
                except Exception:
                    pass


@testtools.skipIf(not _postgres_exists(), 'postgres is not available')
class PostgresPersistenceTest(BackendPersistenceTestMixin, test.TestCase):

    def _init_db(self):
        engine = None
        try:
            # Postgres can't operate on the database it's connected to, that's
            # why we connect to the database 'postgres' and then create the
            # desired database.
            db_uri = _get_connect_string('postgres', USER, PASSWD,
                                         database='postgres')
            engine = sa.create_engine(db_uri)
            with contextlib.closing(engine.connect()) as conn:
                conn.connection.set_isolation_level(0)
                conn.execute("CREATE DATABASE %s" % DATABASE)
                conn.connection.set_isolation_level(1)
        except Exception as e:
            raise Exception('Failed to initialize PostgreSQL db: %s' % (e))
        finally:
            if engine is not None:
                try:
                    engine.dispose()
                except Exception:
                    pass
        return _get_connect_string('postgres', USER, PASSWD,
                                   database=DATABASE)

    def _remove_db(self):
        engine = None
        try:
            # Postgres can't operate on the database it's connected to, that's
            # why we connect to the 'postgres' database and then drop the
            # database.
            db_uri = _get_connect_string('postgres', USER, PASSWD,
                                         database='postgres')
            engine = sa.create_engine(db_uri)
            with contextlib.closing(engine.connect()) as conn:
                conn.connection.set_isolation_level(0)
                conn.execute("DROP DATABASE IF EXISTS %s" % DATABASE)
                conn.connection.set_isolation_level(1)
        except Exception as e:
            raise Exception('Failed to remove temporary database: %s' % (e))
        finally:
            if engine is not None:
                try:
                    engine.dispose()
                except Exception:
                    pass


class SQLBackendFetchingTest(test.TestCase):

    def test_sqlite_persistence_entry_point(self):
        conf = {'connection': 'sqlite:///'}
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_sqlalchemy.SQLAlchemyBackend)

    @testtools.skipIf(not _mysql_exists(), 'mysql is not available')
    def test_mysql_persistence_entry_point(self):
        uri = _get_connect_string('mysql', USER, PASSWD, database=DATABASE)
        conf = {'connection': uri}
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_sqlalchemy.SQLAlchemyBackend)

    @testtools.skipIf(not _postgres_exists(), 'postgres is not available')
    def test_postgres_persistence_entry_point(self):
        uri = _get_connect_string('postgres', USER, PASSWD, database=DATABASE)
        conf = {'connection': uri}
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_sqlalchemy.SQLAlchemyBackend)
