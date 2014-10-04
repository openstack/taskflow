# -*- coding: utf-8 -*-

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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

"""initial_logbook_details_tables

Revision ID: 1cea328f0f65
Revises: None
Create Date: 2013-08-23 11:41:49.207087

"""

# revision identifiers, used by Alembic.
revision = '1cea328f0f65'
down_revision = None

import logging

from alembic import op
import sqlalchemy as sa

from taskflow.persistence.backends.sqlalchemy import tables

LOG = logging.getLogger(__name__)


def _get_indexes():
    # Ensure all uuids are indexed since they are what is typically looked
    # up and fetched, so attempt to ensure that that is done quickly.
    indexes = [
        {
            'name': 'logbook_uuid_idx',
            'table_name': 'logbooks',
            'columns': ['uuid'],
        },
        {
            'name': 'flowdetails_uuid_idx',
            'table_name': 'flowdetails',
            'columns': ['uuid'],
        },
        {
            'name': 'taskdetails_uuid_idx',
            'table_name': 'taskdetails',
            'columns': ['uuid'],
        },
    ]
    return indexes


def _get_foreign_keys():
    f_keys = [
        # Flow details uuid -> logbook parent uuid
        {
            'name': 'flowdetails_ibfk_1',
            'source': 'flowdetails',
            'referent': 'logbooks',
            'local_cols': ['parent_uuid'],
            'remote_cols': ['uuid'],
            'ondelete': 'CASCADE',
        },
        # Task details uuid -> flow details parent uuid
        {
            'name': 'taskdetails_ibfk_1',
            'source': 'taskdetails',
            'referent': 'flowdetails',
            'local_cols': ['parent_uuid'],
            'remote_cols': ['uuid'],
            'ondelete': 'CASCADE',
        },
    ]
    return f_keys


def upgrade():
    op.create_table('logbooks',
                    sa.Column('created_at', sa.DateTime),
                    sa.Column('updated_at', sa.DateTime),
                    sa.Column('meta', sa.Text(), nullable=True),
                    sa.Column('name', sa.String(length=tables.NAME_LENGTH),
                              nullable=True),
                    sa.Column('uuid', sa.String(length=tables.UUID_LENGTH),
                              primary_key=True, nullable=False),
                    mysql_engine='InnoDB',
                    mysql_charset='utf8')
    op.create_table('flowdetails',
                    sa.Column('created_at', sa.DateTime),
                    sa.Column('updated_at', sa.DateTime),
                    sa.Column('parent_uuid',
                              sa.String(length=tables.UUID_LENGTH)),
                    sa.Column('meta', sa.Text(), nullable=True),
                    sa.Column('state', sa.String(length=tables.STATE_LENGTH),
                              nullable=True),
                    sa.Column('name', sa.String(length=tables.NAME_LENGTH),
                              nullable=True),
                    sa.Column('uuid', sa.String(length=tables.UUID_LENGTH),
                              primary_key=True, nullable=False),
                    mysql_engine='InnoDB',
                    mysql_charset='utf8')
    op.create_table('taskdetails',
                    sa.Column('created_at', sa.DateTime),
                    sa.Column('updated_at', sa.DateTime),
                    sa.Column('parent_uuid',
                              sa.String(length=tables.UUID_LENGTH)),
                    sa.Column('meta', sa.Text(), nullable=True),
                    sa.Column('name', sa.String(length=tables.NAME_LENGTH),
                              nullable=True),
                    sa.Column('results', sa.Text(), nullable=True),
                    sa.Column('version',
                              sa.String(length=tables.VERSION_LENGTH),
                              nullable=True),
                    sa.Column('stacktrace', sa.Text(), nullable=True),
                    sa.Column('exception', sa.Text(), nullable=True),
                    sa.Column('state', sa.String(length=tables.STATE_LENGTH),
                              nullable=True),
                    sa.Column('uuid', sa.String(length=tables.UUID_LENGTH),
                              primary_key=True, nullable=False),
                    mysql_engine='InnoDB',
                    mysql_charset='utf8')
    try:
        for fkey_descriptor in _get_foreign_keys():
            op.create_foreign_key(**fkey_descriptor)
    except NotImplementedError as e:
        LOG.warn("Foreign keys are not supported: %s", e)
    try:
        for index_descriptor in _get_indexes():
            op.create_index(**index_descriptor)
    except NotImplementedError as e:
        LOG.warn("Indexes are not supported: %s", e)


def downgrade():
    for table in ['logbooks', 'flowdetails', 'taskdetails']:
        op.drop_table(table)
