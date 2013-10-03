# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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
import logging
import time

import sqlalchemy as sa
from sqlalchemy import exceptions as sa_exc
from sqlalchemy import orm as sa_orm
from sqlalchemy import pool as sa_pool

from taskflow import exceptions as exc
from taskflow.persistence.backends import base
from taskflow.persistence.backends.sqlalchemy import migration
from taskflow.persistence.backends.sqlalchemy import models
from taskflow.persistence import logbook
from taskflow.utils import misc
from taskflow.utils import persistence_utils


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
    # Server terminated while in progress (postgres errors are pretty weird)
    'server closed the connection unexpectedly',
    'terminating connection due to administrator command',
)

# These connection urls mean sqlite is being used as an in-memory DB
SQLITE_IN_MEMORY = ('sqlite://', 'sqlite:///', 'sqlite:///:memory:')


def _in_any(reason, err_haystack):
    """Checks if any elements of the haystack are in the given reason"""
    for err in err_haystack:
        if reason.find(str(err)) != -1:
            return True
    return False


def _is_db_connection_error(reason):
    return _in_any(reason, list(MY_SQL_CONN_ERRORS + POSTGRES_CONN_ERRORS))


def _thread_yield(dbapi_con, con_record):
    """Ensure other greenthreads get a chance to be executed.

    If we use eventlet.monkey_patch(), eventlet.greenthread.sleep(0) will
    execute instead of time.sleep(0).

    Force a context switch. With common database backends (eg MySQLdb and
    sqlite), there is no implicit yield caused by network I/O since they are
    implemented by C libraries that eventlet cannot monkey patch.
    """
    time.sleep(0)


def _ping_listener(dbapi_conn, connection_rec, connection_proxy):
    """Ensures that MySQL connections checked out of the pool are alive.

    Modified + borrowed from: http://bit.ly/14BYaW6
    """
    try:
        dbapi_conn.cursor().execute('select 1')
    except dbapi_conn.OperationalError as ex:
        if _in_any(str(ex.args[0]), MY_SQL_GONE_WAY_AWAY_ERRORS):
            LOG.warn('Got mysql server has gone away: %s', ex)
            raise sa_exc.DisconnectionError("Database server went away")
        elif _in_any(str(ex.args[0]), POSTGRES_GONE_WAY_AWAY_ERRORS):
            LOG.warn('Got postgres server has gone away: %s', ex)
            raise sa_exc.DisconnectionError("Database server went away")
        else:
            raise


class SQLAlchemyBackend(base.Backend):
    def __init__(self, conf):
        super(SQLAlchemyBackend, self).__init__(conf)
        self._engine = None
        self._session_maker = None

    def _test_connected(self, engine, max_retries=0):

        def test_connect(failures):
            try:
                # See if we can make a connection happen.
                #
                # NOTE(harlowja): note that even though we are connecting
                # once it does not mean that we will be able to connect in
                # the future, so this is more of a sanity test and is not
                # complete connection insurance.
                with contextlib.closing(engine.connect()):
                    pass
            except sa_exc.OperationalError as ex:
                if _is_db_connection_error(str(ex.args[0])):
                    failures.append(misc.Failure())
                    return False
            return True

        failures = []
        if test_connect(failures):
            return engine

        # Sorry it didn't work out...
        if max_retries <= 0:
            failures[-1].reraise()

        # Go through the exponential backoff loop and see if we can connect
        # after a given number of backoffs (with a backoff sleeping period
        # between each attempt)...
        attempts_left = max_retries
        for sleepy_secs in misc.ExponentialBackoff(attempts=max_retries):
            LOG.warn("SQL connection failed due to '%s', %s attempts left.",
                     failures[-1].exc, attempts_left)
            LOG.info("Attempting to test the connection again in %s seconds.",
                     sleepy_secs)
            time.sleep(sleepy_secs)
            if test_connect(failures):
                return engine
            attempts_left -= 1

        # Sorry it didn't work out...
        failures[-1].reraise()

    def _create_engine(self):
        # NOTE(harlowja): copy the internal one so that we don't modify it via
        # all the popping that will happen below.
        conf = copy.deepcopy(self._conf)
        engine_args = {
            'echo': misc.as_bool(conf.pop('echo', False)),
            'convert_unicode': misc.as_bool(conf.pop('convert_unicode', True)),
            'pool_recycle': 3600,
        }
        try:
            idle_timeout = misc.as_int(conf.pop('idle_timeout', None))
            engine_args['pool_recycle'] = idle_timeout
        except TypeError:
            pass
        sql_connection = conf.pop('connection')
        e_url = sa.engine.url.make_url(sql_connection)
        if 'sqlite' in e_url.drivername:
            engine_args["poolclass"] = sa_pool.NullPool

            # Adjustments for in-memory sqlite usage
            if sql_connection.lower().strip() in SQLITE_IN_MEMORY:
                engine_args["poolclass"] = sa_pool.StaticPool
                engine_args["connect_args"] = {'check_same_thread': False}
        else:
            for (k, lookup_key) in [('pool_size', 'max_pool_size'),
                                    ('max_overflow', 'max_overflow'),
                                    ('pool_timeout', 'pool_timeout')]:
                try:
                    engine_args[k] = misc.as_int(conf.pop(lookup_key, None))
                except TypeError:
                    pass
        # If the configuration dict specifies any additional engine args
        # or engine arg overrides make sure we merge them in.
        engine_args.update(conf.pop('engine_args', {}))
        engine = sa.create_engine(sql_connection, **engine_args)
        if misc.as_bool(conf.pop('checkin_yield', True)):
            sa.event.listen(engine, 'checkin', _thread_yield)
        if 'mysql' in e_url.drivername:
            if misc.as_bool(conf.pop('checkout_ping', True)):
                sa.event.listen(engine, 'checkout', _ping_listener)
        try:
            max_retries = misc.as_int(conf.pop('max_retries', None))
        except TypeError:
            max_retries = 0
        return self._test_connected(engine, max_retries=max_retries)

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
        return Connection(self, self._get_session_maker())

    def close(self):
        if self._session_maker is not None:
            self._session_maker.close_all()
        if self._engine is not None:
            self._engine.dispose()
        self._engine = None
        self._session_maker = None


class Connection(base.Connection):
    def __init__(self, backend, session_maker):
        self._backend = backend
        self._session_maker = session_maker
        self._engine = backend.engine

    @property
    def backend(self):
        return self._backend

    def _run_in_session(self, functor, *args, **kwargs):
        """Runs a function in a session and makes sure that sqlalchemy
        exceptions aren't emitted from that sessions actions (as that would
        expose the underlying backends exception model).
        """
        try:
            session = self._make_session()
            with session.begin():
                return functor(session, *args, **kwargs)
        except sa_exc.SQLAlchemyError as e:
            LOG.exception('Failed running database session')
            raise exc.StorageError("Failed running database session: %s" % e,
                                   e)

    def _make_session(self):
        try:
            return self._session_maker()
        except sa_exc.SQLAlchemyError as e:
            LOG.exception('Failed creating database session')
            raise exc.StorageError("Failed creating database session: %s"
                                   % e, e)

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
            raise exc.StorageError("Failed upgrading database version: %s" % e,
                                   e)

    def _clear_all(self, session):
        # NOTE(harlowja): due to how we have our relationship setup and
        # cascading deletes are enabled, this will cause all associated
        # task details and flow details to automatically be purged.
        try:
            return session.query(models.LogBook).delete()
        except sa_exc.DBAPIError as e:
            LOG.exception('Failed clearing all entries')
            raise exc.StorageError("Failed clearing all entries: %s" % e, e)

    def clear_all(self):
        return self._run_in_session(self._clear_all)

    def _update_task_details(self, session, td):
        # Must already exist since a tasks details has a strong connection to
        # a flow details, and tasks details can not be saved on there own since
        # they *must* have a connection to an existing flow details.
        td_m = _task_details_get_model(td.uuid, session=session)
        td_m = _taskdetails_merge(td_m, td)
        td_m = session.merge(td_m)
        return _convert_td_to_external(td_m)

    def update_task_details(self, task_detail):
        return self._run_in_session(self._update_task_details, td=task_detail)

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
            raise exc.StorageError("Failed destroying"
                                   " logbook %s: %s" % (lb_id, e), e)

    def destroy_logbook(self, book_uuid):
        return self._run_in_session(self._destroy_logbook, lb_id=book_uuid)

    def _save_logbook(self, session, lb):
        try:
            lb_m = _logbook_get_model(lb.uuid, session=session)
            # NOTE(harlowja): Merge them (note that this doesn't provide
            # 100% correct update semantics due to how databases have
            # MVCC). This is where a stored procedure or a better backing
            # store would handle this better by allowing this merge logic
            # to exist in the database itself.
            lb_m = _logbook_merge(lb_m, lb)
        except exc.NotFound:
            lb_m = _convert_lb_to_internal(lb)
        try:
            lb_m = session.merge(lb_m)
            return _convert_lb_to_external(lb_m)
        except sa_exc.DBAPIError as e:
            LOG.exception('Failed saving logbook')
            raise exc.StorageError("Failed saving logbook %s: %s" %
                                   (lb.uuid, e), e)

    def save_logbook(self, book):
        return self._run_in_session(self._save_logbook, lb=book)

    def get_logbook(self, book_uuid):
        session = self._make_session()
        try:
            lb = _logbook_get_model(book_uuid, session=session)
            return _convert_lb_to_external(lb)
        except sa_exc.DBAPIError as e:
            LOG.exception('Failed getting logbook')
            raise exc.StorageError("Failed getting logbook %s: %s"
                                   % (book_uuid, e), e)

    def get_logbooks(self):
        session = self._make_session()
        try:
            raw_books = session.query(models.LogBook).all()
            books = [_convert_lb_to_external(lb) for lb in raw_books]
        except sa_exc.DBAPIError as e:
            LOG.exception('Failed getting logbooks')
            raise exc.StorageError("Failed getting logbooks: %s" % e, e)
        for lb in books:
            yield lb

    def close(self):
        pass

###
# Internal <-> external model + merging + other helper functions.
###


def _convert_fd_to_external(fd):
    fd_c = logbook.FlowDetail(fd.name, uuid=fd.uuid)
    fd_c.meta = fd.meta
    fd_c.state = fd.state
    for td in fd.taskdetails:
        fd_c.add(_convert_td_to_external(td))
    return fd_c


def _convert_fd_to_internal(fd, parent_uuid):
    fd_m = models.FlowDetail(name=fd.name, uuid=fd.uuid,
                             parent_uuid=parent_uuid, meta=fd.meta,
                             state=fd.state)
    fd_m.taskdetails = []
    for td in fd:
        fd_m.taskdetails.append(_convert_td_to_internal(td, fd_m.uuid))
    return fd_m


def _convert_td_to_internal(td, parent_uuid):
    return models.TaskDetail(name=td.name, uuid=td.uuid,
                             state=td.state, results=td.results,
                             failure=td.failure, meta=td.meta,
                             version=td.version, parent_uuid=parent_uuid)


def _convert_td_to_external(td):
    # Convert from sqlalchemy model -> external model, this allows us
    # to change the internal sqlalchemy model easily by forcing a defined
    # interface (that isn't the sqlalchemy model itself).
    td_c = logbook.TaskDetail(td.name, uuid=td.uuid)
    td_c.state = td.state
    td_c.results = td.results
    td_c.failure = td.failure
    td_c.meta = td.meta
    td_c.version = td.version
    return td_c


def _convert_lb_to_external(lb_m):
    """Don't expose the internal sqlalchemy ORM model to the external api."""
    lb_c = logbook.LogBook(lb_m.name, lb_m.uuid,
                           updated_at=lb_m.updated_at,
                           created_at=lb_m.created_at)
    lb_c.meta = lb_m.meta
    for fd_m in lb_m.flowdetails:
        lb_c.add(_convert_fd_to_external(fd_m))
    return lb_c


def _convert_lb_to_internal(lb_c):
    """Don't expose the external model to the sqlalchemy ORM model."""
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


def _flow_details_get_model(f_id, session):
    entry = session.query(models.FlowDetail).filter_by(uuid=f_id).first()
    if entry is None:
        raise exc.NotFound("No flow details found with id: %s" % f_id)
    return entry


def _task_details_get_model(t_id, session):
    entry = session.query(models.TaskDetail).filter_by(uuid=t_id).first()
    if entry is None:
        raise exc.NotFound("No task details found with id: %s" % t_id)
    return entry


def _logbook_merge(lb_m, lb):
    lb_m = persistence_utils.logbook_merge(lb_m, lb)
    for fd in lb:
        existing_fd = False
        for fd_m in lb_m.flowdetails:
            if fd_m.uuid == fd.uuid:
                existing_fd = True
                fd_m = _flowdetails_merge(fd_m, fd)
        if not existing_fd:
            lb_m.flowdetails.append(_convert_fd_to_internal(fd, lb_m.uuid))
    return lb_m


def _flowdetails_merge(fd_m, fd):
    fd_m = persistence_utils.flow_details_merge(fd_m, fd)
    for td in fd:
        existing_td = False
        for td_m in fd_m.taskdetails:
            if td_m.uuid == td.uuid:
                existing_td = True
                td_m = _taskdetails_merge(td_m, td)
                break
        if not existing_td:
            td_m = _convert_td_to_internal(td, fd_m.uuid)
            fd_m.taskdetails.append(td_m)
    return fd_m


def _taskdetails_merge(td_m, td):
    return persistence_utils.task_details_merge(td_m, td)
