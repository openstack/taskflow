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

"""fix atomdetails failure size

Revision ID: 40fc8c914bd2
Revises: 6df9422fcb43
Create Date: 2022-01-27 18:10:06.176006

"""

# revision identifiers, used by Alembic.
revision = '40fc8c914bd2'
down_revision = '6df9422fcb43'

from alembic import op
from sqlalchemy.dialects import mysql


def upgrade():
    bind = op.get_bind()
    engine = bind.engine
    if engine.name == 'mysql':
        op.alter_column('atomdetails', 'failure', type_=mysql.LONGTEXT,
                        existing_nullable=True)
        op.alter_column('atomdetails', 'revert_failure', type_=mysql.LONGTEXT,
                        existing_nullable=True)
