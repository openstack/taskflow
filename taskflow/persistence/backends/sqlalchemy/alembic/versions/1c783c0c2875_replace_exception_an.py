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

"""Replace exception and stacktrace with failure column

Revision ID: 1c783c0c2875
Revises: 1cea328f0f65
Create Date: 2013-09-26 12:33:30.970122

"""

# revision identifiers, used by Alembic.
revision = '1c783c0c2875'
down_revision = '1cea328f0f65'

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('taskdetails',
                  sa.Column('failure', sa.Text(), nullable=True))
    op.drop_column('taskdetails', 'exception')
    op.drop_column('taskdetails', 'stacktrace')


def downgrade():
    op.drop_column('taskdetails', 'failure')
    op.add_column('taskdetails',
                  sa.Column('stacktrace', sa.Text(), nullable=True))
    op.add_column('taskdetails',
                  sa.Column('exception', sa.Text(), nullable=True))
