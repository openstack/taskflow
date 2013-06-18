# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
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

"""
Routines for configuring TaskFlow
"""

import os

from oslo.config import cfg

db_opts = [
    cfg.StrOpt('sql_connection',
               default='sqlite:///' +
                       os.path.abspath(os.path.join(os.path.dirname(__file__),
                       '../', '$sqlite_db')),
               help='The SQLAlchemy connection string used to connect to the '
                    'database'),
    cfg.StrOpt('sqlite_db',
               default='taskflow.sqlite',
               help='The filename to use with sqlite'),
    cfg.IntOpt('sql_idle_timeout',
               default=3600,
               help='timeout before idle sql connections are reaped')]

celery_opts = [
    cfg.StrOpt('celery_backend',
               default='mysql://task:flow@localhost/taskflow',
               help='The SQLAlchemy connection string used to connect to the '
                    'celery backend'),
    cfg.StrOpt('celery_MQ',
               default='mongodb://task:flow@localhost:27017/taskflow',
               help='The MongoDB connection string used to connect to the '
                    'celery message queue')]


def register_db_opts():
    cfg.CONF.register_opts(db_opts)


def register_celery_opts():
    cfg.CONF.register_opts(celery_opts)
