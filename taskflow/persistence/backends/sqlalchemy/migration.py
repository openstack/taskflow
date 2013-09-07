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

from alembic import config as a_config
from alembic import environment as a_env
from alembic import script as a_script


def _alembic_config():
    path = os.path.join(os.path.dirname(__file__), 'alembic', 'alembic.ini')
    return a_config.Config(path)


def db_sync(connection, revision='head'):
    script = a_script.ScriptDirectory.from_config(_alembic_config())

    def upgrade(rev, context):
        return script._upgrade_revs(revision, rev)

    config = _alembic_config()
    with a_env.EnvironmentContext(config, script, fn=upgrade, as_sql=False,
                                  starting_rev=None, destination_rev=revision,
                                  tag=None) as context:
        context.configure(connection=connection)
        context.run_migrations()
