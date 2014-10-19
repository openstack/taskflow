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

"""Implementation of a SQLAlchemy storage backend."""

from __future__ import absolute_import

import contextlib
import copy
import functools
import logging
import time

from oslo.utils import strutils
import six
import sqlalchemy as sa
from sqlalchemy import exc as sa_exc
from sqlalchemy import orm as sa_orm
from sqlalchemy import pool as sa_pool

from taskflow import exceptions as exc
from taskflow.persistence.backends import base
from taskflow.persistence.backends.sqlalchemy import migration
from taskflow.persistence.backends.sqlalchemy import models
from taskflow.persistence import logbook
from taskflow.types import failure
from taskflow.utils import async_utils
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


class SQLAlchemyBackend(base.Backend):
    """A sqlalchemy backend.

    Example conf:

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
            self._engine = None
            self._owns_engine = True
        self._session_maker = None
        self._validated = False

    def _create_engine(self):
        # NOTE(harlowja): copy the internal one so that we don't modify it via
        # all the popping that will happen below.
        conf = copy.deepcopy(self._conf)
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
        checkin_yield = conf.pop('checkin_yield',
                                 async_utils.EVENTLET_AVAILABLE)
        if _as_bool(checkin_yield):
            sa.event.listen(engine, 'checkin', _thread_yield)
        if 'mysql' in e_url.drivername:
            if _as_bool(conf.pop('checkout_ping', True)):
                sa.event.listen(engine, 'checkout', _ping_listener)
            mode = None
            if _as_bool(conf.pop('mysql_traditional_mode', True)):
                mode = 'TRADITIONAL'
            if 'mysql_sql_mode' in conf:
                mode = conf.pop('mysql_sql_mode')
            if mode is not None:
                sa.event.listen(engine, 'connect',
                                functools.partial(_set_sql_mode, mode))
        return engine

    @property
    def engine(self):
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    def _get_session_maker(self):
        if self._session_maker is None:
            self._session_maker = sa_orm.sessionmaker(bind=self.engine,
                                                      autocommit=True)
        return self._session_maker

    def get_connection(self):
        conn = Connection(self, self._get_session_maker())
        if not self._validated:
            try:
                max_retries = misc.as_int(self._conf.get('max_retries', None))
            except TypeError:
                max_retries = 0
            conn.validate(max_retries=max_retries)
            self._validated = True
        return conn

    def close(self):
        if self._session_maker is not None:
            self._session_maker.close_all()
            self._session_maker = None
        if self._engine is not None and self._owns_engine:
            # NOTE(harlowja): Only dispose of the engine and clear it from
            # our local state if we actually own the engine in the first
            # place. If the user passed in their own engine we should not
            # be disposing it on their behalf (and we shouldn't be clearing
            # our local engine either, since then we would just recreate a
            # new engine if the engine property is accessed).
            self._engine.dispose()
            self._engine = None
        self._validated = False


class Connection(base.Connection):
    def __init__(self, backend, session_maker):
        self._backend = backend
        self._session_maker = session_maker
        self._engine = backend.engine

    @property
    def backend(self):
        return self._backend

    def validate(self, max_retries=0):

        def test_connect(failures):
            try:
                # See if we can make a connection happen.
                #
                # NOTE(harlowja): note that even though we are connecting
                # once it does not mean that we will be able to connect in
                # the future, so this is more of a sanity test and is not
                # complete connection insurance.
                with contextlib.closing(self._engine.connect()):
                    pass
            except sa_exc.OperationalError as ex:
                if _is_db_connection_error(six.text_type(ex.args[0])):
                    failures.append(failure.Failure())
                    return False
            return True

        failures = []
        if test_connect(failures):
            return

        # Sorry it didn't work out...
        if max_retries <= 0:
            failures[-1].reraise()

        # Go through the exponential backoff loop and see if we can connect
        # after a given number of backoffs (with a backoff sleeping period
        # between each attempt)...
        attempts_left = max_retries
        for sleepy_secs in misc.ExponentialBackoff(max_retries):
            LOG.warn("SQL connection failed due to '%s', %s attempts left.",
                     failures[-1].exc, attempts_left)
            LOG.info("Attempting to test the connection again in %s seconds.",
                     sleepy_secs)
            time.sleep(sleepy_secs)
            if test_connect(failures):
                return
            attempts_left -= 1

        # Sorry it didn't work out...
        failures[-1].reraise()

    def _run_in_session(self, functor, *args, **kwargs):
        """Runs a callback in a session.

        This function proxy will create a session, and then call the callback
        with that session (along with the provided args and kwargs). It ensures
        that the session is opened & closed and makes sure that sqlalchemy
        exceptions aren't emitted from the callback or sessions actions (as
        that would expose the underlying sqlalchemy exception model).
        """
        try:
            session = self._make_session()
            with session.begin():
                return functor(session, *args, **kwargs)
        except sa_exc.SQLAlchemyError as e:
            LOG.exception("Failed running '%s' within a database session",
                          functor.__name__)
            raise exc.StorageFailure("Storage backend internal error, failed"
                                     " running '%s' within a database"
                                     " session" % functor.__name__, e)

    def _make_session(self):
        try:
            return self._session_maker()
        except sa_exc.SQLAlchemyError as e:
            LOG.exception('Failed creating database session')
            raise exc.StorageFailure("Failed creating database session", e)

    def upgrade(self):
        try:
            with contextlib.closing(self._engine.connect()) as conn:
                # NOTE(imelnikov): Alembic does not support SQLite,
                # and we don't recommend to use SQLite in production
                # deployments, so migrations are rarely needed
                # for SQLite. So we don't bother about working around
                # SQLite limitations, and create database from models
                # when it is in use.
                if 'sqlite' in self._engine.url.drivername:
                    models.BASE.metadata.create_all(conn)
                else:
                    migration.db_sync(conn)
        except sa_exc.SQLAlchemyError as e:
            LOG.exception('Failed upgrading database version')
            raise exc.StorageFailure("Failed upgrading database version", e)

    def _clear_all(self, session):
        # NOTE(harlowja): due to how we have our relationship setup and
        # cascading deletes are enabled, this will cause all associated
        # task details and flow details to automatically be purged.
        try:
            return session.query(models.LogBook).delete()
        except sa_exc.DBAPIError as e:
            LOG.exception('Failed clearing all entries')
            raise exc.StorageFailure("Failed clearing all entries", e)

    def clear_all(self):
        return self._run_in_session(self._clear_all)

    def _update_atom_details(self, session, ad):
        # Must already exist since a atoms details has a strong connection to
        # a flow details, and atom details can not be saved on there own since
        # they *must* have a connection to an existing flow detail.
        ad_m = _atom_details_get_model(ad.uuid, session=session)
        ad_m = _atomdetails_merge(ad_m, ad)
        ad_m = session.merge(ad_m)
        return _convert_ad_to_external(ad_m)

    def update_atom_details(self, atom_detail):
        return self._run_in_session(self._update_atom_details, ad=atom_detail)

    def _update_flow_details(self, session, fd):
        # Must already exist since a flow details has a strong connection to
        # a logbook, and flow details can not be saved on there own since they
        # *must* have a connection to an existing logbook.
        fd_m = _flow_details_get_model(fd.uuid, session=session)
        fd_m = _flowdetails_merge(fd_m, fd)
        fd_m = session.merge(fd_m)
        return _convert_fd_to_external(fd_m)

    def update_flow_details(self, flow_detail):
        return self._run_in_session(self._update_flow_details, fd=flow_detail)

    def _destroy_logbook(self, session, lb_id):
        try:
            lb = _logbook_get_model(lb_id, session=session)
            session.delete(lb)
        except sa_exc.DBAPIError as e:
            LOG.exception('Failed destroying logbook')
            raise exc.StorageFailure("Failed destroying logbook %s" % lb_id, e)

    def destroy_logbook(self, book_uuid):
        return self._run_in_session(self._destroy_logbook, lb_id=book_uuid)

    def _save_logbook(self, session, lb):
        try:
            lb_m = _logbook_get_model(lb.uuid, session=session)
            lb_m = _logbook_merge(lb_m, lb)
        except exc.NotFound:
            lb_m = _convert_lb_to_internal(lb)
        try:
            lb_m = session.merge(lb_m)
            return _convert_lb_to_external(lb_m)
        except sa_exc.DBAPIError as e:
            LOG.exception('Failed saving logbook')
            raise exc.StorageFailure("Failed saving logbook %s" % lb.uuid, e)

    def save_logbook(self, book):
        return self._run_in_session(self._save_logbook, lb=book)

    def get_logbook(self, book_uuid):
        session = self._make_session()
        try:
            lb = _logbook_get_model(book_uuid, session=session)
            return _convert_lb_to_external(lb)
        except sa_exc.DBAPIError as e:
            LOG.exception('Failed getting logbook')
            raise exc.StorageFailure("Failed getting logbook %s" % book_uuid,
                                     e)

    def get_logbooks(self):
        session = self._make_session()
        try:
            raw_books = session.query(models.LogBook).all()
            books = [_convert_lb_to_external(lb) for lb in raw_books]
        except sa_exc.DBAPIError as e:
            LOG.exception('Failed getting logbooks')
            raise exc.StorageFailure("Failed getting logbooks", e)
        for lb in books:
            yield lb

    def close(self):
        pass

###
# Internal <-> external model + merging + other helper functions.
###


def _atomdetails_merge(ad_m, ad):
    atom_type = logbook.atom_detail_type(ad)
    if atom_type != ad_m.atom_type:
        raise exc.StorageFailure("Can not merge differing atom types "
                                 "(%s != %s)" % (atom_type, ad_m.atom_type))
    ad_d = ad.to_dict()
    ad_m.state = ad_d['state']
    ad_m.intention = ad_d['intention']
    ad_m.results = ad_d['results']
    ad_m.version = ad_d['version']
    ad_m.failure = ad_d['failure']
    ad_m.meta = ad_d['meta']
    ad_m.name = ad_d['name']
    return ad_m


def _flowdetails_merge(fd_m, fd):
    fd_d = fd.to_dict()
    fd_m.state = fd_d['state']
    fd_m.name = fd_d['name']
    fd_m.meta = fd_d['meta']
    for ad in fd:
        existing_ad = False
        for ad_m in fd_m.atomdetails:
            if ad_m.uuid == ad.uuid:
                existing_ad = True
                ad_m = _atomdetails_merge(ad_m, ad)
                break
        if not existing_ad:
            ad_m = _convert_ad_to_internal(ad, fd_m.uuid)
            fd_m.atomdetails.append(ad_m)
    return fd_m


def _logbook_merge(lb_m, lb):
    lb_d = lb.to_dict()
    lb_m.meta = lb_d['meta']
    lb_m.name = lb_d['name']
    lb_m.created_at = lb_d['created_at']
    lb_m.updated_at = lb_d['updated_at']
    for fd in lb:
        existing_fd = False
        for fd_m in lb_m.flowdetails:
            if fd_m.uuid == fd.uuid:
                existing_fd = True
                fd_m = _flowdetails_merge(fd_m, fd)
        if not existing_fd:
            lb_m.flowdetails.append(_convert_fd_to_internal(fd, lb_m.uuid))
    return lb_m


def _convert_fd_to_external(fd):
    fd_c = logbook.FlowDetail(fd.name, uuid=fd.uuid)
    fd_c.meta = fd.meta
    fd_c.state = fd.state
    for ad_m in fd.atomdetails:
        fd_c.add(_convert_ad_to_external(ad_m))
    return fd_c


def _convert_fd_to_internal(fd, parent_uuid):
    fd_m = models.FlowDetail(name=fd.name, uuid=fd.uuid,
                             parent_uuid=parent_uuid, meta=fd.meta,
                             state=fd.state)
    fd_m.atomdetails = []
    for ad in fd:
        fd_m.atomdetails.append(_convert_ad_to_internal(ad, fd_m.uuid))
    return fd_m


def _convert_ad_to_internal(ad, parent_uuid):
    converted = ad.to_dict()
    converted['atom_type'] = logbook.atom_detail_type(ad)
    converted['parent_uuid'] = parent_uuid
    return models.AtomDetail(**converted)


def _convert_ad_to_external(ad):
    # Convert from sqlalchemy model -> external model, this allows us
    # to change the internal sqlalchemy model easily by forcing a defined
    # interface (that isn't the sqlalchemy model itself).
    atom_cls = logbook.atom_detail_class(ad.atom_type)
    return atom_cls.from_dict({
        'state': ad.state,
        'intention': ad.intention,
        'results': ad.results,
        'failure': ad.failure,
        'meta': ad.meta,
        'version': ad.version,
        'name': ad.name,
        'uuid': ad.uuid,
    })


def _convert_lb_to_external(lb_m):
    lb_c = logbook.LogBook(lb_m.name, lb_m.uuid)
    lb_c.updated_at = lb_m.updated_at
    lb_c.created_at = lb_m.created_at
    lb_c.meta = lb_m.meta
    for fd_m in lb_m.flowdetails:
        lb_c.add(_convert_fd_to_external(fd_m))
    return lb_c


def _convert_lb_to_internal(lb_c):
    lb_m = models.LogBook(uuid=lb_c.uuid, meta=lb_c.meta, name=lb_c.name)
    lb_m.flowdetails = []
    for fd_c in lb_c:
        lb_m.flowdetails.append(_convert_fd_to_internal(fd_c, lb_c.uuid))
    return lb_m


def _logbook_get_model(lb_id, session):
    entry = session.query(models.LogBook).filter_by(uuid=lb_id).first()
    if entry is None:
        raise exc.NotFound("No logbook found with id: %s" % lb_id)
    return entry


def _flow_details_get_model(flow_id, session):
    entry = session.query(models.FlowDetail).filter_by(uuid=flow_id).first()
    if entry is None:
        raise exc.NotFound("No flow details found with id: %s" % flow_id)
    return entry


def _atom_details_get_model(atom_id, session):
    entry = session.query(models.AtomDetail).filter_by(uuid=atom_id).first()
    if entry is None:
        raise exc.NotFound("No atom details found with id: %s" % atom_id)
    return entry
