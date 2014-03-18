# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

"""Rename taskdetails to atomdetails

Revision ID: 589dccdf2b6e
Revises: 14b227d79a87
Create Date: 2014-03-19 11:49:16.533227

"""

# revision identifiers, used by Alembic.
revision = '589dccdf2b6e'
down_revision = '14b227d79a87'

from alembic import op


def upgrade():
    op.rename_table("taskdetails", "atomdetails")


def downgrade():
    op.rename_table("atomdetails", "taskdetails")
