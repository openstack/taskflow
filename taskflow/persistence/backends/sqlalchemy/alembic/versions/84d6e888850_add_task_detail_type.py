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

"""Add task detail type

Revision ID: 84d6e888850
Revises: 1c783c0c2875
Create Date: 2014-01-20 18:12:42.503267

"""

# revision identifiers, used by Alembic.
revision = '84d6e888850'
down_revision = '1c783c0c2875'

from alembic import op
import sqlalchemy as sa

from taskflow.persistence import models


def upgrade():
    atom_types = sa.Enum(*models.ATOM_TYPES, name='atom_types')
    column = sa.Column('atom_type', atom_types)
    bind = op.get_bind()
    impl = atom_types.dialect_impl(bind.dialect)
    impl.create(bind, checkfirst=True)
    op.add_column('taskdetails', column)


def downgrade():
    op.drop_column('taskdetails', 'atom_type')
