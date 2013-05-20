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

"""Implementation of SQLAlchemy Backend"""

from oslo.config import cfg

from taskflow.common import config
from taskflow import utils

SQL_CONNECTION = 'sqlite://'
db_opts = [
    cfg.StrOpt('db_backend',
               default='sqlalchemy',
               help='The backend to use for db')]

CONF = cfg.CONF
CONF.register_opts(db_opts)

IMPL = utils.LazyPluggable('db_backend',
                           sqlalchemy='taskflow.db.sqlalchemy.api')

def configure():
    global SQL_CONNECTION
    global SQL_IDLE_TIMEOUT
    config.register_db_opts()
    SQL_CONNECTION = cfg.CONF.sql_connection
    SQL_IDLE_TIMEOUT = cfg.CONF.sql_idle_timeout
