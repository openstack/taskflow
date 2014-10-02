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

import collections

from oslo.serialization import jsonutils
from oslo.utils import timeutils
from oslo.utils import uuidutils
from sqlalchemy import Table, Column, String, ForeignKey, DateTime, Enum
from sqlalchemy import types

from taskflow.persistence import logbook
from taskflow import states

Tables = collections.namedtuple('Tables',
                                ['logbooks', 'flowdetails', 'atomdetails'])

# Column length limits...
NAME_LENGTH = 255
UUID_LENGTH = 64
STATE_LENGTH = 255
VERSION_LENGTH = 64


class Json(types.TypeDecorator):
    impl = types.Text

    def process_bind_param(self, value, dialect):
        if not value:
            return None
        return jsonutils.dumps(value)

    def process_result_value(self, value, dialect):
        if not value:
            return None
        return jsonutils.loads(value)


def fetch(metadata):
    """Returns the master set of table objects (which is also there schema)."""
    logbooks = Table('logbooks', metadata,
                     Column('created_at', DateTime,
                            default=timeutils.utcnow),
                     Column('updated_at', DateTime,
                            default=timeutils.utcnow),
                     Column('meta', Json),
                     Column('name', String(length=NAME_LENGTH)),
                     Column('uuid', String(length=UUID_LENGTH),
                            primary_key=True, nullable=False, unique=True,
                            default=uuidutils.generate_uuid))
    flowdetails = Table('flowdetails', metadata,
                        Column('created_at', DateTime,
                               default=timeutils.utcnow),
                        Column('updated_at', DateTime,
                               default=timeutils.utcnow),
                        Column('parent_uuid', String(length=UUID_LENGTH),
                               ForeignKey('logbooks.uuid',
                                          ondelete='CASCADE')),
                        Column('meta', Json),
                        Column('name', String(length=NAME_LENGTH)),
                        Column('state', String(length=STATE_LENGTH)),
                        Column('uuid', String(length=UUID_LENGTH),
                               primary_key=True, nullable=False, unique=True,
                               default=uuidutils.generate_uuid))
    atomdetails = Table('atomdetails', metadata,
                        Column('created_at', DateTime,
                               default=timeutils.utcnow),
                        Column('updated_at', DateTime,
                               default=timeutils.utcnow),
                        Column('meta', Json),
                        Column('parent_uuid', String(length=UUID_LENGTH),
                               ForeignKey('flowdetails.uuid',
                                          ondelete='CASCADE')),
                        Column('name', String(length=NAME_LENGTH)),
                        Column('version', String(length=VERSION_LENGTH)),
                        Column('state', String(length=STATE_LENGTH)),
                        Column('uuid', String(length=UUID_LENGTH),
                               primary_key=True, nullable=False, unique=True,
                               default=uuidutils.generate_uuid),
                        Column('failure', Json),
                        Column('results', Json),
                        Column('atom_type', Enum(*logbook.ATOM_TYPES,
                                                 name='atom_types')),
                        Column('intention', Enum(*states.INTENTIONS,
                                                 name='intentions')))
    return Tables(logbooks, flowdetails, atomdetails)
