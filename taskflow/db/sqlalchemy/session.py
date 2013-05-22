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

"""Session Handling for SQLAlchemy backend."""

import logging

import sqlalchemy.orm
import sqlalchemy.engine
import sqlalchemy.interfaces
import sqlalchemy
from sqlalchemy.pool import NullPool

from taskflow.db import api as db_api

LOG = logging.getLogger(__name__)

_ENGINE = None
_MAKER = None


def get_session(autocommit=True, expire_on_commit=True):
    """Return a SQLAlchemy session."""
    global _MAKER

    if _MAKER is None:
        _MAKER = get_maker(get_engine(), autocommit, expire_on_commit)
    return _MAKER()


def synchronous_switch_listener(dbapi_conn, connection_rec):
    """Switch sqlite connections to non-synchronous mode"""
    dbapi_conn.execute("PRAGMA synchronous = OFF")


def ping_listener(dbapi_conn, connection_rec, connection_proxy):
    """
Ensures that MySQL connections checked out of the
pool are alive.

Borrowed from:
http://groups.google.com/group/sqlalchemy/msg/a4ce563d802c929f
"""
    try:
        dbapi_conn.cursor().execute('select 1')
    except dbapi_conn.OperationalError, ex:
        if ex.args[0] in (2006, 2013, 2014, 2045, 2055):
            LOG.warn(_('Got mysql server has gone away: %s'), ex)
            raise DisconnectionError("Database server went away")
        else:
            raise


def get_engine():
    """Return a SQLAlchemy engine."""
    global _ENGINE
    if _ENGINE is None:
        connection_dict = sqlalchemy.engine.url.make_url(_get_sql_connection())
        engine_args = {
            "pool_recycle": _get_sql_idle_timeout(),
            "echo": False,
            "convert_unicode": True
        }

        if "sqlite" in connection_dict.drivername:
            engine_args['poolclass'] = NullPool

        _ENGINE = sqlalchemy.create_engine(_get_sql_connection(),
                                           **engine_args)

        if 'mysql' in connection_dict.drivername:
            sqlalchemy.event.listen(_ENGINE, 'checkout', ping_listener)
        if 'sqlite' in connection_dict.drivername:
            sqlalchemy.event.listen(_ENGINE, 'connect',
                                    synchronous_switch_listener)

    #TODO: Check to make sure engine connected

    return _ENGINE


def get_maker(engine, autocommit=True, expire_on_commit=False):
    "Return a SQLAlchemy sessionmaker using the given engine."""
    return sqlalchemy.orm.sessionmaker(bind=engine,
                                       autocommit=autocommit,
                                       expire_on_commit=expire_on_commit)

def _get_sql_connection():
    return db_api.SQL_CONNECTION


def _get_sql_idle_timeout():
    return db_api.SQL_IDLE_TIMEOUT
