# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
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

from sqlalchemy import Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship
from sqlalchemy import types as types

from taskflow.openstack.common import jsonutils
from taskflow.openstack.common import timeutils
from taskflow.openstack.common import uuidutils

from taskflow.utils import persistence_utils

BASE = declarative_base()


# TODO(harlowja): remove when oslo.db exists
class TimestampMixin(object):
    created_at = Column(DateTime, default=timeutils.utcnow)
    updated_at = Column(DateTime, onupdate=timeutils.utcnow)


class Json(types.TypeDecorator, types.MutableType):
    impl = types.Text

    def process_bind_param(self, value, dialect):
        return jsonutils.dumps(value)

    def process_result_value(self, value, dialect):
        return jsonutils.loads(value)


class Failure(types.TypeDecorator, types.MutableType):
    """Put misc.Failure object into database column.

    We convert Failure object to dict, serialize that dict into
    JSON and save it. None is stored as NULL.

    The conversion is lossy since we cannot save exc_info.
    """
    impl = types.Text

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return jsonutils.dumps(persistence_utils.failure_to_dict(value))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return persistence_utils.failure_from_dict(jsonutils.loads(value))


class ModelBase(TimestampMixin):
    """Base model for all taskflow objects"""
    uuid = Column(String, default=uuidutils.generate_uuid,
                  primary_key=True, nullable=False, unique=True)
    name = Column(String, nullable=True)
    meta = Column(Json, nullable=True)


class LogBook(BASE, ModelBase):
    """Represents a logbook for a set of flows"""
    __tablename__ = 'logbooks'

    # Relationships
    flowdetails = relationship("FlowDetail",
                               single_parent=True,
                               backref=backref("logbooks",
                                               cascade="save-update, delete, "
                                                       "merge"))


class FlowDetail(BASE, ModelBase):
    __tablename__ = 'flowdetails'

    # Member variables
    state = Column(String)

    # Relationships
    parent_uuid = Column(String, ForeignKey('logbooks.uuid'))
    taskdetails = relationship("TaskDetail",
                               single_parent=True,
                               backref=backref("flowdetails",
                                               cascade="save-update, delete, "
                                                       "merge"))


class TaskDetail(BASE, ModelBase):
    __tablename__ = 'taskdetails'

    # Member variables
    state = Column(String)
    results = Column(Json)
    failure = Column(Failure)
    version = Column(String)

    # Relationships
    parent_uuid = Column(String, ForeignKey('flowdetails.uuid'))
