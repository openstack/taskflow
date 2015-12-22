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

"""Switch postgres to json native type.

Revision ID: 2ad4984f2864
Revises: 3162c0f3f8e4
Create Date: 2015-06-04 13:08:36.667948

"""

# revision identifiers, used by Alembic.
revision = '2ad4984f2864'
down_revision = '3162c0f3f8e4'

from alembic import op


_ALTER_TO_JSON_TPL = 'ALTER TABLE %s ALTER COLUMN %s TYPE JSON USING %s::JSON'
_TABLES_COLS = tuple([
    ('logbooks', 'meta'),
    ('flowdetails', 'meta'),
    ('atomdetails', 'meta'),
    ('atomdetails', 'failure'),
    ('atomdetails', 'revert_failure'),
    ('atomdetails', 'results'),
    ('atomdetails', 'revert_results'),
])
_ALTER_TO_TEXT_TPL = 'ALTER TABLE %s ALTER COLUMN %s TYPE TEXT'


def upgrade():
    b = op.get_bind()
    if b.dialect.name.startswith('postgresql'):
        for (table_name, col_name) in _TABLES_COLS:
            q = _ALTER_TO_JSON_TPL % (table_name, col_name, col_name)
            op.execute(q)


def downgrade():
    b = op.get_bind()
    if b.dialect.name.startswith('postgresql'):
        for (table_name, col_name) in _TABLES_COLS:
            q = _ALTER_TO_TEXT_TPL % (table_name, col_name)
            op.execute(q)
