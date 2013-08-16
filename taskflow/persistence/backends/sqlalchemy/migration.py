# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
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

"""Database setup and migration commands."""

import os

from oslo.config import cfg

import alembic
from alembic import config as alembic_config

CONF = cfg.CONF
CONF.import_opt('connection',
                'taskflow.openstack.common.db.sqlalchemy.session',
                group='database')


def _alembic_config():
    path = os.path.join(os.path.dirname(__file__), 'alembic', 'alembic.ini')
    config = alembic_config.Config(path)
    if not config.get_main_option('url'):
        config.set_main_option('sqlalchemy.url', CONF.database.connection)
    return config


def db_sync():
    config = _alembic_config()
    alembic.command.upgrade(config, "head")
