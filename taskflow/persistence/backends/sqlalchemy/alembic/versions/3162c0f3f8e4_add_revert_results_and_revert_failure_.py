# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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

"""Add 'revert_results' and 'revert_failure' atom detail column.

Revision ID: 3162c0f3f8e4
Revises: 589dccdf2b6e
Create Date: 2015-06-17 15:52:56.575245

"""

# revision identifiers, used by Alembic.
revision = '3162c0f3f8e4'
down_revision = '589dccdf2b6e'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('atomdetails',
                  sa.Column('revert_results', sa.Text(), nullable=True))
    op.add_column('atomdetails',
                  sa.Column('revert_failure', sa.Text(), nullable=True))


def downgrade():
    op.drop_column('atomdetails', 'revert_results')
    op.drop_column('atomdetails', 'revert_failure')
