# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#    Copyright (C) 2013 Rackspace Hosting All Rights Reserved.
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
"""
SQLAlchemy models for taskflow data.
"""
import json
from oslo.config import cfg

from sqlalchemy import Column, Integer, String, Table
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import object_mapper, relationship
from sqlalchemy import DateTime, ForeignKey
from sqlalchemy import types as types

from taskflow.db.sqlalchemy import session as sql_session
from taskflow.openstack.common import exception
from taskflow.openstack.common import timeutils
from taskflow.openstack.common import uuidutils

CONF = cfg.CONF
BASE = declarative_base()


class Json(types.TypeDecorator, types.MutableType):
    impl = types.Text

    def process_bind_param(self, value, dialect):
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        return json.loads(value)


class TaskFlowBase(object):
    """Base class for TaskFlow Models."""
    __table_args__ = {'mysql_engine': 'InnoDB'}
    __table_initialized = False
    created_at = Column(DateTime, default=timeutils.utcnow)
    updated_at = Column(DateTime, default=timeutils.utcnow)

    def save(self, session=None):
        """Save this object."""
        if not session:
            session = sql_session.get_session()
        session.add(self)
        try:
            session.flush()
        except IntegrityError, e:
            if str(e).endswith('is not unique'):
                raise exception.Duplicate(str(e))
            else:
                raise

    def delete(self, session=None):
        """Delete this object."""
        self.deleted = True
        self.deleted_at = timeutils.utcnow()
        if not session:
            session = sql_session.get_session()
        session.delete(self)
        session.flush()

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def __iter__(self):
        self._i = iter(object_mapper(self).columns)
        return self

    def next(self):
        n = self._i.next().name
        return n, getattr(self, n)

    def update(self, values):
        """Make the model object behave like a dict"""
        for k, v in values.iteritems():
            setattr(self, k, v)

    def iteritems(self):
        """Make the model object behave like a dict

           Includes attributes from joins."""
        local = dict(self)
        joined = dict([k, v] for k, v in self.__dict__.iteritems()
                      if not k[0] == '_')
        local.update(joined)
        return local.iteritems()


workflow_logbook_assoc = Table(
    'wf_lb_assoc', BASE.metadata,
    Column('workflow_id', Integer, ForeignKey('workflow.id')),
    Column('logbook_id', Integer, ForeignKey('logbook.id')),
    Column('id', Integer, primary_key=True)
)


workflow_job_assoc = Table(
    'wf_job_assoc', BASE.metadata,
    Column('workflow_id', Integer, ForeignKey('workflow.id')),
    Column('job_id', Integer, ForeignKey('job.id')),
    Column('id', Integer, primary_key=True)
)


class LogBook(BASE, TaskFlowBase):
    """Represents a logbook for a set of workflows"""

    __tablename__ = 'logbook'

    id = Column(Integer, primary_key=True)
    logbook_id = Column(String, default=uuidutils.generate_uuid,
                        unique=True)
    name = Column(String)
    workflows = relationship("Workflow",
                             secondary=workflow_logbook_assoc)
    job = relationship("Job", uselist=False, backref="logbook")


class Job(BASE, TaskFlowBase):
    """Represents a Job"""

    __tablename__ = 'job'

    id = Column(Integer, primary_key=True)
    job_id = Column(String, default=uuidutils.generate_uuid,
                    unique=True)
    name = Column(String)
    owner = Column(String)
    state = Column(String)
    workflows = relationship("Workflow",
                             secondary=workflow_job_assoc)
    logbook_id = Column(String, ForeignKey('logbook.logbook_id'))


class Workflow(BASE, TaskFlowBase):
    """Represents Workflow detail objects"""

    __tablename__ = 'workflow'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    tasks = relationship("Task", backref="workflow")


class Task(BASE, TaskFlowBase):
    """Represents Task detail objects"""

    __tablename__ = 'task'

    id = Column(Integer, primary_key=True)
    task_id = Column(String, default=uuidutils.generate_uuid)
    name = Column(String)
    results = Column(Json)
    exception = Column(String)
    stacktrace = Column(String)
    workflow_id = Column(String, ForeignKey('workflow.id'))


def create_tables():
    BASE.metadata.create_all(sql_session.get_engine())
