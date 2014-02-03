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

import contextlib
import os
import tempfile
import threading

import testtools


# NOTE(harlowja): by default this will test against sqlite using a temporary
# sqlite file (this is done instead of in-memory to ensure thread safety,
# in-memory sqlite is not thread safe).
#
# There are also "opportunistic" tests for both mysql and postgresql in here,
# which allows testing against all 3 databases (sqlite, mysql, postgres) in
# a properly configured unit test environment. For the opportunistic testing
# you need to set up a db named 'openstack_citest' with user 'openstack_citest'
# and password 'openstack_citest' on localhost.

USER = "openstack_citest"
PASSWD = "openstack_citest"
DATABASE = "openstack_citest"

try:
    from taskflow.persistence.backends import impl_sqlalchemy

    import sqlalchemy as sa
    SQLALCHEMY_AVAILABLE = True
except Exception:
    SQLALCHEMY_AVAILABLE = False

# Testing will try to run against these two mysql library variants.
MYSQL_VARIANTS = ('mysqldb', 'pymysql')

from taskflow.persistence import backends
from taskflow import test
from taskflow.tests.unit.persistence import base
from taskflow.utils import lock_utils


def _get_connect_string(backend, user, passwd, database=None, variant=None):
    """Try to get a connection with a very specific set of values, if we get
    these then we'll run the tests, otherwise they are skipped.
    """
    if backend == "postgres":
        if not variant:
            variant = 'psycopg2'
        backend = "postgresql+%s" % (variant)
    elif backend == "mysql":
        if not variant:
            variant = 'mysqldb'
        backend = "mysql+%s" % (variant)
    else:
        raise Exception("Unrecognized backend: '%s'" % backend)
    if not database:
        database = ''
    return "%s://%s:%s@localhost/%s" % (backend, user, passwd, database)


def _mysql_exists():
    if not SQLALCHEMY_AVAILABLE:
        return False
    for variant in MYSQL_VARIANTS:
        engine = None
        try:
            db_uri = _get_connect_string('mysql', USER, PASSWD,
                                         variant=variant)
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
    if not SQLALCHEMY_AVAILABLE:
        return False
    engine = None
    try:
        db_uri = _get_connect_string('postgres', USER, PASSWD, 'template1')
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


@testtools.skipIf(not SQLALCHEMY_AVAILABLE, 'sqlalchemy is not available')
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


class BackendPersistenceTestMixin(base.PersistenceTestMixin):
    """Specifies a backend type and does required setup and teardown."""
    LOCK_NAME = None

    def _get_connection(self):
        return self.backend.get_connection()

    def _reset_database(self):
        """Resets the database, and returns the uri to that database.

        Called *only* after locking succeeds.
        """
        raise NotImplementedError()

    def setUp(self):
        super(BackendPersistenceTestMixin, self).setUp()
        self.backend = None
        self.big_lock.acquire()
        self.addCleanup(self.big_lock.release)
        try:
            conf = {
                'connection': self._reset_database(),
            }
        except Exception as e:
            self.skipTest("Failed to reset your database;"
                          " testing being skipped due to: %s" % (e))
        try:
            self.backend = impl_sqlalchemy.SQLAlchemyBackend(conf)
            self.addCleanup(self.backend.close)
            with contextlib.closing(self._get_connection()) as conn:
                conn.upgrade()
        except Exception as e:
            self.skipTest("Failed to setup your database;"
                          " testing being skipped due to: %s" % (e))


@testtools.skipIf(not SQLALCHEMY_AVAILABLE, 'sqlalchemy is not available')
@testtools.skipIf(not _mysql_exists(), 'mysql is not available')
class MysqlPersistenceTest(BackendPersistenceTestMixin, test.TestCase):
    LOCK_NAME = 'mysql_persistence_test'

    def __init__(self, *args, **kwargs):
        test.TestCase.__init__(self, *args, **kwargs)
        # We need to make sure that each test goes through a set of locks
        # to ensure that multiple tests are not modifying the database,
        # dropping it, creating it at the same time. To accomplish this we use
        # a lock that ensures multiple parallel processes can't run at the
        # same time as well as a in-process lock to ensure that multiple
        # threads can't run at the same time.
        lock_path = os.path.join(tempfile.gettempdir(),
                                 'taskflow-%s.lock' % (self.LOCK_NAME))
        locks = [
            lock_utils.InterProcessLock(lock_path),
            threading.RLock(),
        ]
        self.big_lock = lock_utils.MultiLock(locks)

    def _reset_database(self):
        working_variant = None
        for variant in MYSQL_VARIANTS:
            engine = None
            try:
                db_uri = _get_connect_string('mysql', USER, PASSWD,
                                             variant=variant)
                engine = sa.create_engine(db_uri)
                with contextlib.closing(engine.connect()) as conn:
                    conn.execute("DROP DATABASE IF EXISTS %s" % DATABASE)
                    conn.execute("CREATE DATABASE %s" % DATABASE)
                    working_variant = variant
            except Exception:
                pass
            finally:
                if engine is not None:
                    try:
                        engine.dispose()
                    except Exception:
                        pass
            if working_variant:
                break
        if not working_variant:
            variants = ", ".join(MYSQL_VARIANTS)
            self.skipTest("Failed to find a mysql variant"
                          " (tried %s) that works; mysql testing"
                          " being skipped" % (variants))
        else:
            return _get_connect_string('mysql', USER, PASSWD,
                                       database=DATABASE,
                                       variant=working_variant)


@testtools.skipIf(not SQLALCHEMY_AVAILABLE, 'sqlalchemy is not available')
@testtools.skipIf(not _postgres_exists(), 'postgres is not available')
class PostgresPersistenceTest(BackendPersistenceTestMixin, test.TestCase):
    LOCK_NAME = 'postgres_persistence_test'

    def __init__(self, *args, **kwargs):
        test.TestCase.__init__(self, *args, **kwargs)
        # We need to make sure that each test goes through a set of locks
        # to ensure that multiple tests are not modifying the database,
        # dropping it, creating it at the same time. To accomplish this we use
        # a lock that ensures multiple parallel processes can't run at the
        # same time as well as a in-process lock to ensure that multiple
        # threads can't run at the same time.
        lock_path = os.path.join(tempfile.gettempdir(),
                                 'taskflow-%s.lock' % (self.LOCK_NAME))
        locks = [
            lock_utils.InterProcessLock(lock_path),
            threading.RLock(),
        ]
        self.big_lock = lock_utils.MultiLock(locks)

    def _reset_database(self):
        engine = None
        try:
            # Postgres can't operate on the database its connected to, thats
            # why we connect to the default template database 'template1' and
            # then drop and create the desired database.
            db_uri = _get_connect_string('postgres', USER, PASSWD,
                                         database='template1')
            engine = sa.create_engine(db_uri)
            with contextlib.closing(engine.connect()) as conn:
                conn.connection.set_isolation_level(0)
                conn.execute("DROP DATABASE IF EXISTS %s" % DATABASE)
                conn.connection.set_isolation_level(1)
            with contextlib.closing(engine.connect()) as conn:
                conn.connection.set_isolation_level(0)
                conn.execute("CREATE DATABASE %s" % DATABASE)
                conn.connection.set_isolation_level(1)
        finally:
            if engine is not None:
                try:
                    engine.dispose()
                except Exception:
                    pass
        return _get_connect_string('postgres', USER, PASSWD, database=DATABASE)


@testtools.skipIf(not SQLALCHEMY_AVAILABLE, 'sqlalchemy is not available')
class SQLBackendFetchingTest(test.TestCase):

    def test_sqlite_persistence_entry_point(self):
        conf = {'connection': 'sqlite:///'}
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_sqlalchemy.SQLAlchemyBackend)

    @testtools.skipIf(not _postgres_exists(), 'postgres is not available')
    def test_mysql_persistence_entry_point(self):
        uri = "mysql://%s:%s@localhost/%s" % (USER, PASSWD, DATABASE)
        conf = {'connection': uri}
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_sqlalchemy.SQLAlchemyBackend)

    @testtools.skipIf(not _mysql_exists(), 'mysql is not available')
    def test_postgres_persistence_entry_point(self):
        uri = "postgresql://%s:%s@localhost/%s" % (USER, PASSWD, DATABASE)
        conf = {'connection': uri}
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_sqlalchemy.SQLAlchemyBackend)
