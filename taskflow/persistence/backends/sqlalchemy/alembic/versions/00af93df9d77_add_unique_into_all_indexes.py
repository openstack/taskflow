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

"""Add unique into all indexes

Revision ID: 00af93df9d77
Revises: 40fc8c914bd2
Create Date: 2025-02-28 15:44:37.066720

"""

# revision identifiers, used by Alembic.
revision = '00af93df9d77'
down_revision = '40fc8c914bd2'

from alembic import op


def upgrade():
    bind = op.get_bind()
    engine = bind.engine
    if engine.name == 'mysql':
        with op.batch_alter_table("logbooks") as batch_op:
            batch_op.drop_index("logbook_uuid_idx")
            batch_op.create_index(
                index_name="logbook_uuid_idx",
                columns=['uuid'],
                unique=True)

        with op.batch_alter_table("flowdetails") as batch_op:
            batch_op.drop_index("flowdetails_uuid_idx")
            batch_op.create_index(
                index_name="flowdetails_uuid_idx",
                columns=['uuid'],
                unique=True)

        with op.batch_alter_table("atomdetails") as batch_op:
            batch_op.drop_index("taskdetails_uuid_idx")
            batch_op.create_index(
                index_name="taskdetails_uuid_idx",
                columns=['uuid'],
                unique=True)


def downgrade():
    bind = op.get_bind()
    engine = bind.engine
    if engine.name == 'mysql':
        with op.batch_alter_table("logbooks") as batch_op:
            batch_op.drop_index("logbook_uuid_idx")
            batch_op.create_index(
                index_name="logbook_uuid_idx",
                columns=['uuid'])

        with op.batch_alter_table("flowdetails") as batch_op:
            batch_op.drop_index("flowdetails_uuid_idx")
            batch_op.create_index(
                index_name="flowdetails_uuid_idx",
                columns=['uuid'])

        with op.batch_alter_table("atomdetails") as batch_op:
            batch_op.drop_index("taskdetails_uuid_idx")
            batch_op.create_index(
                index_name="taskdetails_uuid_idx",
                columns=['uuid'])
