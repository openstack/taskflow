# -*- coding: utf-8 -*-

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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

from __future__ import absolute_import

import contextlib
import copy
import functools
import threading
import time

from oslo_utils import strutils
import retrying
import six
import sqlalchemy as sa
from sqlalchemy import exc as sa_exc
from sqlalchemy import pool as sa_pool
from sqlalchemy import sql

from taskflow import exceptions as exc
from taskflow import logging
from taskflow.persistence.backends.sqlalchemy import migration
from taskflow.persistence.backends.sqlalchemy import tables
from taskflow.persistence import base
from taskflow.persistence import models
from taskflow.utils import eventlet_utils
from taskflow.utils import misc


LOG = logging.getLogger(__name__)

# NOTE(harlowja): This is all very similar to what oslo-incubator uses but is
# not based on using oslo.cfg and its global configuration (which should not be
# used in libraries such as taskflow).
#
# TODO(harlowja): once oslo.db appears we should be able to use that instead
# since it's not supposed to have any usage of oslo.cfg in it when it
# materializes as a library.

# See: http://dev.mysql.com/doc/refman/5.0/en/error-messages-client.html
MY_SQL_CONN_ERRORS = (
    # Lost connection to MySQL server at '%s', system error: %d
    '2006',
    # Can't connect to MySQL server on '%s' (%d)
    '2003',
    # Can't connect to local MySQL server through socket '%s' (%d)
    '2002',
)
MY_SQL_GONE_WAY_AWAY_ERRORS = (
    # Lost connection to MySQL server at '%s', system error: %d
    '2006',
    # Lost connection to MySQL server during query
    '2013',
    # Commands out of sync; you can't run this command now
    '2014',
    # Can't open shared memory; no answer from server (%lu)
    '2045',
    # Lost connection to MySQL server at '%s', system error: %d
    '2055',
)

# See: http://www.postgresql.org/docs/9.1/static/errcodes-appendix.html
POSTGRES_CONN_ERRORS = (
    # connection_exception
    '08000',
    # connection_does_not_exist
    '08003',
    # connection_failure
    '08006',
    # sqlclient_unable_to_establish_sqlconnection
    '08001',
    # sqlserver_rejected_establishment_of_sqlconnection
    '08004',
    # Just couldn't connect (postgres errors are pretty weird)
    'could not connect to server',
)
POSTGRES_GONE_WAY_AWAY_ERRORS = (
    # Server terminated while in progress (postgres errors are pretty weird).
    'server closed the connection unexpectedly',
    'terminating connection due to administrator command',
)

# These connection urls mean sqlite is being used as an in-memory DB.
SQLITE_IN_MEMORY = ('sqlite://', 'sqlite:///', 'sqlite:///:memory:')

# Transacation isolation levels that will be automatically applied, we prefer
# strong read committed isolation levels to avoid merging and using dirty
# data...
#
# See: http://en.wikipedia.org/wiki/Isolation_(database_systems)
DEFAULT_TXN_ISOLATION_LEVELS = {
    'mysql': 'READ COMMITTED',
    'postgresql': 'READ COMMITTED',
    'postgres': 'READ COMMITTED',
}


def _log_statements(log_level, conn, cursor, statement, parameters, *args):
    if LOG.isEnabledFor(log_level):
        LOG.log(log_level, "Running statement '%s' with parameters %s",
                statement, parameters)


def _in_any(reason, err_haystack):
    """Checks if any elements of the haystack are in the given reason."""
    for err in err_haystack:
        if reason.find(six.text_type(err)) != -1:
            return True
    return False


def _is_db_connection_error(reason):
    return _in_any(reason, list(MY_SQL_CONN_ERRORS + POSTGRES_CONN_ERRORS))


def _as_bool(value):
    if isinstance(value, bool):
        return value
    # This is different than strutils, but imho is an acceptable difference.
    if value is None:
        return False
    # NOTE(harlowja): prefer strictness to avoid users getting accustomed
    # to passing bad values in and this *just working* (which imho is a bad
    # habit to encourage).
    return strutils.bool_from_string(value, strict=True)


def _thread_yield(dbapi_con, con_record):
    """Ensure other greenthreads get a chance to be executed.

    If we use eventlet.monkey_patch(), eventlet.greenthread.sleep(0) will
    execute instead of time.sleep(0).

    Force a context switch. With common database backends (eg MySQLdb and
    sqlite), there is no implicit yield caused by network I/O since they are
    implemented by C libraries that eventlet cannot monkey patch.
    """
    time.sleep(0)


def _set_sql_mode(sql_mode, dbapi_con, connection_rec):
    """Set the sql_mode session variable.

    MySQL supports several server modes. The default is None, but sessions
    may choose to enable server modes like TRADITIONAL, ANSI,
    several STRICT_* modes and others.

    Note: passing in '' (empty string) for sql_mode clears
    the SQL mode for the session, overriding a potentially set
    server default.
    """
    cursor = dbapi_con.cursor()
    cursor.execute("SET SESSION sql_mode = %s", [sql_mode])


def _ping_listener(dbapi_conn, connection_rec, connection_proxy):
    """Ensures that MySQL connections checked out of the pool are alive.

    Modified + borrowed from: http://bit.ly/14BYaW6.
    """
    try:
        dbapi_conn.cursor().execute('select 1')
    except dbapi_conn.OperationalError as ex:
        if _in_any(six.text_type(ex.args[0]), MY_SQL_GONE_WAY_AWAY_ERRORS):
            LOG.warn('Got mysql server has gone away', exc_info=True)
            raise sa_exc.DisconnectionError("Database server went away")
        elif _in_any(six.text_type(ex.args[0]), POSTGRES_GONE_WAY_AWAY_ERRORS):
            LOG.warn('Got postgres server has gone away', exc_info=True)
            raise sa_exc.DisconnectionError("Database server went away")
        else:
            raise


class _Alchemist(object):
    """Internal <-> external row <-> objects + other helper functions.

    NOTE(harlowja): for internal usage only.
    """
    def __init__(self, tables):
        self._tables = tables

    @staticmethod
    def convert_flow_detail(row):
        return models.FlowDetail.from_dict(dict(row.items()))

    @staticmethod
    def convert_book(row):
        return models.LogBook.from_dict(dict(row.items()))

    @staticmethod
    def convert_atom_detail(row):
        row = dict(row.items())
        atom_cls = models.atom_detail_class(row.pop('atom_type'))
        return atom_cls.from_dict(row)

    def atom_query_iter(self, conn, parent_uuid):
        q = (sql.select([self._tables.atomdetails]).
             where(self._tables.atomdetails.c.parent_uuid == parent_uuid))
        for row in conn.execute(q):
            yield self.convert_atom_detail(row)

    def flow_query_iter(self, conn, parent_uuid):
        q = (sql.select([self._tables.flowdetails]).
             where(self._tables.flowdetails.c.parent_uuid == parent_uuid))
        for row in conn.execute(q):
            yield self.convert_flow_detail(row)

    def populate_book(self, conn, book):
        for fd in self.flow_query_iter(conn, book.uuid):
            book.add(fd)
            self.populate_flow_detail(conn, fd)

    def populate_flow_detail(self, conn, fd):
        for ad in self.atom_query_iter(conn, fd.uuid):
            fd.add(ad)


class SQLAlchemyBackend(base.Backend):
    """A sqlalchemy backend.

    Example configuration::

        conf = {
            "connection": "sqlite:////tmp/test.db",
        }
    """
    def __init__(self, conf, engine=None):
        super(SQLAlchemyBackend, self).__init__(conf)
        if engine is not None:
            self._engine = engine
            self._owns_engine = False
        else:
            self._engine = self._create_engine(self._conf)
            self._owns_engine = True
        self._validated = False
        self._upgrade_lock = threading.Lock()
        try:
            self._max_retries = misc.as_int(self._conf.get('max_retries'))
        except TypeError:
            self._max_retries = 0

    @staticmethod
    def _create_engine(conf):
        # NOTE(harlowja): copy the internal one so that we don't modify it via
        # all the popping that will happen below.
        conf = copy.deepcopy(conf)
        engine_args = {
            'echo': _as_bool(conf.pop('echo', False)),
            'convert_unicode': _as_bool(conf.pop('convert_unicode', True)),
            'pool_recycle': 3600,
        }
        if 'idle_timeout' in conf:
            idle_timeout = misc.as_int(conf.pop('idle_timeout'))
            engine_args['pool_recycle'] = idle_timeout
        sql_connection = conf.pop('connection')
        e_url = sa.engine.url.make_url(sql_connection)
        if 'sqlite' in e_url.drivername:
            engine_args["poolclass"] = sa_pool.NullPool

            # Adjustments for in-memory sqlite usage.
            if sql_connection.lower().strip() in SQLITE_IN_MEMORY:
                engine_args["poolclass"] = sa_pool.StaticPool
                engine_args["connect_args"] = {'check_same_thread': False}
        else:
            for (k, lookup_key) in [('pool_size', 'max_pool_size'),
                                    ('max_overflow', 'max_overflow'),
                                    ('pool_timeout', 'pool_timeout')]:
                if lookup_key in conf:
                    engine_args[k] = misc.as_int(conf.pop(lookup_key))
        if 'isolation_level' not in conf:
            # Check driver name exact matches first, then try driver name
            # partial matches...
            txn_isolation_levels = conf.pop('isolation_levels',
                                            DEFAULT_TXN_ISOLATION_LEVELS)
            level_applied = False
            for (driver, level) in six.iteritems(txn_isolation_levels):
                if driver == e_url.drivername:
                    engine_args['isolation_level'] = level
                    level_applied = True
                    break
            if not level_applied:
                for (driver, level) in six.iteritems(txn_isolation_levels):
                    if e_url.drivername.find(driver) != -1:
                        engine_args['isolation_level'] = level
                        break
        else:
            engine_args['isolation_level'] = conf.pop('isolation_level')
        # If the configuration dict specifies any additional engine args
        # or engine arg overrides make sure we merge them in.
        engine_args.update(conf.pop('engine_args', {}))
        engine = sa.create_engine(sql_connection, **engine_args)
        log_statements = conf.pop('log_statements', False)
        if _as_bool(log_statements):
            log_statements_level = conf.pop("log_statements_level",
                                            logging.TRACE)
            sa.event.listen(engine, "before_cursor_execute",
                            functools.partial(_log_statements,
                                              log_statements_level))
        checkin_yield = conf.pop('checkin_yield',
                                 eventlet_utils.EVENTLET_AVAILABLE)
        if _as_bool(checkin_yield):
            sa.event.listen(engine, 'checkin', _thread_yield)
        if 'mysql' in e_url.drivername:
            if _as_bool(conf.pop('checkout_ping', True)):
                sa.event.listen(engine, 'checkout', _ping_listener)
            mode = None
            if 'mysql_sql_mode' in conf:
                mode = conf.pop('mysql_sql_mode')
            if mode is not None:
                sa.event.listen(engine, 'connect',
                                functools.partial(_set_sql_mode, mode))
        return engine

    @property
    def engine(self):
        return self._engine

    def get_connection(self):
        conn = Connection(self, upgrade_lock=self._upgrade_lock)
        if not self._validated:
            conn.validate(max_retries=self._max_retries)
            self._validated = True
        return conn

    def close(self):
        # NOTE(harlowja): Only dispose of the engine if we actually own the
        # engine in the first place. If the user passed in their own engine
        # we should not be disposing it on their behalf...
        if self._owns_engine:
            self._engine.dispose()
        self._validated = False


class Connection(base.Connection):
    def __init__(self, backend, upgrade_lock):
        self._backend = backend
        self._upgrade_lock = upgrade_lock
        self._engine = backend.engine
        self._metadata = sa.MetaData()
        self._tables = tables.fetch(self._metadata)
        self._converter = _Alchemist(self._tables)

    @property
    def backend(self):
        return self._backend

    def validate(self, max_retries=0):
        """Performs basic **connection** validation of a sqlalchemy engine."""

        def _retry_on_exception(exc):
            LOG.warn("Engine connection (validate) failed due to '%s'", exc)
            if isinstance(exc, sa_exc.OperationalError) and \
               _is_db_connection_error(six.text_type(exc.args[0])):
                # We may be able to fix this by retrying...
                return True
            if isinstance(exc, (sa_exc.TimeoutError,
                                sa_exc.ResourceClosedError,
                                sa_exc.DisconnectionError)):
                # We may be able to fix this by retrying...
                return True
            # Other failures we likely can't fix by retrying...
            return False

        @retrying.retry(stop_max_attempt_number=max(0, int(max_retries)),
                        # Ensure that the 2 ** retry number
                        # is converted into milliseconds (thus why this
                        # multiplies by 1000.0) because thats what retrying
                        # lib. uses internally for whatever reason.
                        wait_exponential_multiplier=1000.0,
                        wrap_exception=False,
                        retry_on_exception=_retry_on_exception)
        def _try_connect(engine):
            # See if we can make a connection happen.
            #
            # NOTE(harlowja): note that even though we are connecting
            # once it does not mean that we will be able to connect in
            # the future, so this is more of a sanity test and is not
            # complete connection insurance.
            with contextlib.closing(engine.connect()):
                pass

        _try_connect(self._engine)

    def upgrade(self):
        try:
            with self._upgrade_lock:
                with contextlib.closing(self._engine.connect()) as conn:
                    # NOTE(imelnikov): Alembic does not support SQLite,
                    # and we don't recommend to use SQLite in production
                    # deployments, so migrations are rarely needed
                    # for SQLite. So we don't bother about working around
                    # SQLite limitations, and create the database directly
                    # from the tables when it is in use...
                    if 'sqlite' in self._engine.url.drivername:
                        self._metadata.create_all(bind=conn)
                    else:
                        migration.db_sync(conn)
        except sa_exc.SQLAlchemyError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Failed upgrading database version")

    def clear_all(self):
        try:
            logbooks = self._tables.logbooks
            with self._engine.begin() as conn:
                conn.execute(logbooks.delete())
        except sa_exc.DBAPIError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Failed clearing all entries")

    def update_atom_details(self, atom_detail):
        try:
            atomdetails = self._tables.atomdetails
            with self._engine.begin() as conn:
                q = (sql.select([atomdetails]).
                     where(atomdetails.c.uuid == atom_detail.uuid))
                row = conn.execute(q).first()
                if not row:
                    raise exc.NotFound("No atom details found with uuid"
                                       " '%s'" % atom_detail.uuid)
                e_ad = self._converter.convert_atom_detail(row)
                self._update_atom_details(conn, atom_detail, e_ad)
            return e_ad
        except sa_exc.SQLAlchemyError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Failed updating atom details"
                                 " with uuid '%s'" % atom_detail.uuid)

    def _insert_flow_details(self, conn, fd, parent_uuid):
        value = fd.to_dict()
        value['parent_uuid'] = parent_uuid
        conn.execute(sql.insert(self._tables.flowdetails, value))
        for ad in fd:
            self._insert_atom_details(conn, ad, fd.uuid)

    def _insert_atom_details(self, conn, ad, parent_uuid):
        value = ad.to_dict()
        value['parent_uuid'] = parent_uuid
        value['atom_type'] = models.atom_detail_type(ad)
        conn.execute(sql.insert(self._tables.atomdetails, value))

    def _update_atom_details(self, conn, ad, e_ad):
        e_ad.merge(ad)
        conn.execute(sql.update(self._tables.atomdetails)
                     .where(self._tables.atomdetails.c.uuid == e_ad.uuid)
                     .values(e_ad.to_dict()))

    def _update_flow_details(self, conn, fd, e_fd):
        e_fd.merge(fd)
        conn.execute(sql.update(self._tables.flowdetails)
                     .where(self._tables.flowdetails.c.uuid == e_fd.uuid)
                     .values(e_fd.to_dict()))
        for ad in fd:
            e_ad = e_fd.find(ad.uuid)
            if e_ad is None:
                e_fd.add(ad)
                self._insert_atom_details(conn, ad, fd.uuid)
            else:
                self._update_atom_details(conn, ad, e_ad)

    def update_flow_details(self, flow_detail):
        try:
            flowdetails = self._tables.flowdetails
            with self._engine.begin() as conn:
                q = (sql.select([flowdetails]).
                     where(flowdetails.c.uuid == flow_detail.uuid))
                row = conn.execute(q).first()
                if not row:
                    raise exc.NotFound("No flow details found with"
                                       " uuid '%s'" % flow_detail.uuid)
                e_fd = self._converter.convert_flow_detail(row)
                self._converter.populate_flow_detail(conn, e_fd)
                self._update_flow_details(conn, flow_detail, e_fd)
            return e_fd
        except sa_exc.SQLAlchemyError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Failed updating flow details with"
                                 " uuid '%s'" % flow_detail.uuid)

    def destroy_logbook(self, book_uuid):
        try:
            logbooks = self._tables.logbooks
            with self._engine.begin() as conn:
                q = logbooks.delete().where(logbooks.c.uuid == book_uuid)
                r = conn.execute(q)
                if r.rowcount == 0:
                    raise exc.NotFound("No logbook found with"
                                       " uuid '%s'" % book_uuid)
        except sa_exc.DBAPIError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Failed destroying logbook '%s'" % book_uuid)

    def save_logbook(self, book):
        try:
            logbooks = self._tables.logbooks
            with self._engine.begin() as conn:
                q = (sql.select([logbooks]).
                     where(logbooks.c.uuid == book.uuid))
                row = conn.execute(q).first()
                if row:
                    e_lb = self._converter.convert_book(row)
                    self._converter.populate_book(conn, e_lb)
                    e_lb.merge(book)
                    conn.execute(sql.update(logbooks)
                                 .where(logbooks.c.uuid == e_lb.uuid)
                                 .values(e_lb.to_dict()))
                    for fd in book:
                        e_fd = e_lb.find(fd.uuid)
                        if e_fd is None:
                            e_lb.add(fd)
                            self._insert_flow_details(conn, fd, e_lb.uuid)
                        else:
                            self._update_flow_details(conn, fd, e_fd)
                    return e_lb
                else:
                    conn.execute(sql.insert(logbooks, book.to_dict()))
                    for fd in book:
                        self._insert_flow_details(conn, fd, book.uuid)
                    return book
        except sa_exc.DBAPIError:
            exc.raise_with_cause(
                exc.StorageFailure,
                "Failed saving logbook '%s'" % book.uuid)

    def get_logbook(self, book_uuid, lazy=False):
        try:
            logbooks = self._tables.logbooks
            with contextlib.closing(self._engine.connect()) as conn:
                q = (sql.select([logbooks]).
                     where(logbooks.c.uuid == book_uuid))
                row = conn.execute(q).first()
                if not row:
                    raise exc.NotFound("No logbook found with"
                                       " uuid '%s'" % book_uuid)
                book = self._converter.convert_book(row)
                if not lazy:
                    self._converter.populate_book(conn, book)
                return book
        except sa_exc.DBAPIError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Failed getting logbook '%s'" % book_uuid)

    def get_logbooks(self, lazy=False):
        gathered = []
        try:
            with contextlib.closing(self._engine.connect()) as conn:
                q = sql.select([self._tables.logbooks])
                for row in conn.execute(q):
                    book = self._converter.convert_book(row)
                    if not lazy:
                        self._converter.populate_book(conn, book)
                    gathered.append(book)
        except sa_exc.DBAPIError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Failed getting logbooks")
        for book in gathered:
            yield book

    def get_flows_for_book(self, book_uuid, lazy=False):
        gathered = []
        try:
            with contextlib.closing(self._engine.connect()) as conn:
                for fd in self._converter.flow_query_iter(conn, book_uuid):
                    if not lazy:
                        self._converter.populate_flow_detail(conn, fd)
                    gathered.append(fd)
        except sa_exc.DBAPIError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Failed getting flow details in"
                                 " logbook '%s'" % book_uuid)
        for flow_details in gathered:
            yield flow_details

    def get_flow_details(self, fd_uuid, lazy=False):
        try:
            flowdetails = self._tables.flowdetails
            with self._engine.begin() as conn:
                q = (sql.select([flowdetails]).
                     where(flowdetails.c.uuid == fd_uuid))
                row = conn.execute(q).first()
                if not row:
                    raise exc.NotFound("No flow details found with uuid"
                                       " '%s'" % fd_uuid)
                fd = self._converter.convert_flow_detail(row)
                if not lazy:
                    self._converter.populate_flow_detail(conn, fd)
                return fd
        except sa_exc.SQLAlchemyError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Failed getting flow details with"
                                 " uuid '%s'" % fd_uuid)

    def get_atom_details(self, ad_uuid):
        try:
            atomdetails = self._tables.atomdetails
            with self._engine.begin() as conn:
                q = (sql.select([atomdetails]).
                     where(atomdetails.c.uuid == ad_uuid))
                row = conn.execute(q).first()
                if not row:
                    raise exc.NotFound("No atom details found with uuid"
                                       " '%s'" % ad_uuid)
                return self._converter.convert_atom_detail(row)
        except sa_exc.SQLAlchemyError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Failed getting atom details with"
                                 " uuid '%s'" % ad_uuid)

    def get_atoms_for_flow(self, fd_uuid):
        gathered = []
        try:
            with contextlib.closing(self._engine.connect()) as conn:
                for ad in self._converter.atom_query_iter(conn, fd_uuid):
                    gathered.append(ad)
        except sa_exc.DBAPIError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Failed getting atom details in flow"
                                 " detail '%s'" % fd_uuid)
        for atom_details in gathered:
            yield atom_details

    def close(self):
        pass
