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

"""fix flowdetails meta size

Revision ID: 6df9422fcb43
Revises: 0bc3e1a3c135
Create Date: 2021-04-27 14:51:53.618249

"""

# revision identifiers, used by Alembic.
revision = '6df9422fcb43'
down_revision = '0bc3e1a3c135'

from alembic import op
from sqlalchemy.dialects import mysql


def upgrade():
    bind = op.get_bind()
    engine = bind.engine
    if engine.name == 'mysql':
        op.alter_column('flowdetails', 'meta', type_=mysql.LONGTEXT,
                        existing_nullable=True)
