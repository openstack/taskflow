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

"""set_result_meduimtext_type

Revision ID: 0bc3e1a3c135
Revises: 2ad4984f2864
Create Date: 2019-08-08 16:11:36.221164

"""

# revision identifiers, used by Alembic.
revision = '0bc3e1a3c135'
down_revision = '2ad4984f2864'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


def upgrade():
    bind = op.get_bind()
    engine = bind.engine
    if engine.name == 'mysql':
        op.alter_column('atomdetails', 'results', type_=mysql.LONGTEXT,
                        existing_nullable=True)


def downgrade():
    bind = op.get_bind()
    engine = bind.engine
    if engine.name == 'mysql':
        op.alter_column('atomdetails', 'results', type_=sa.Text(),
                        existing_nullable=True)
