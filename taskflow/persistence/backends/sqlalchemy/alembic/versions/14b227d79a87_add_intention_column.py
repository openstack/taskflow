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

# revision identifiers, used by Alembic.
revision = '14b227d79a87'
down_revision = '84d6e888850'

from alembic import op
import sqlalchemy as sa

from taskflow import states


def upgrade():
    bind = op.get_bind()
    intention_type = sa.Enum(*states.INTENTIONS, name='intention_type')
    column = sa.Column('intention', intention_type,
                       server_default=states.EXECUTE)
    impl = intention_type.dialect_impl(bind.dialect)
    impl.create(bind, checkfirst=True)
    op.add_column('taskdetails', column)


def downgrade():
    op.drop_column('taskdetails', 'intention')
