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

"""Implementation of a SQLAlchemy storage backend."""

import logging
import sys

from sqlalchemy import exceptions as sql_exc

from taskflow import exceptions as exc
from taskflow.openstack.common.db.sqlalchemy import session as db_session
from taskflow.persistence.backends.sqlalchemy import models
from taskflow.persistence import flowdetail
from taskflow.persistence import logbook
from taskflow.persistence import taskdetail


LOG = logging.getLogger(__name__)


def get_backend():
    """The backend is this module itself."""
    return sys.modules[__name__]


def _convert_fd_to_external(fd):
    fd_c = flowdetail.FlowDetail(fd.name, uuid=fd.uuid, backend='sqlalchemy')
    fd_c.meta = fd.meta
    fd_c.state = fd.state
    for td in fd.taskdetails:
        fd_c.add(_convert_td_to_external(td))
    return fd_c


def _convert_fd_to_internal(fd, lb_uuid):
    fd_m = models.FlowDetail(name=fd.name, uuid=fd.uuid, parent_uuid=lb_uuid,
                             meta=fd.meta, state=fd.state)
    fd_m.taskdetails = []
    for td in fd:
        fd_m.taskdetails.append(_convert_td_to_internal(td, fd_m.uuid))
    return fd_m


def _convert_td_to_internal(td, parent_uuid):
    return models.TaskDetail(name=td.name, uuid=td.uuid,
                             state=td.state, results=td.results,
                             exception=td.exception, meta=td.meta,
                             stacktrace=td.stacktrace,
                             version=td.version, parent_uuid=parent_uuid)


def _convert_td_to_external(td):
    # Convert from sqlalchemy model -> external model, this allows us
    # to change the internal sqlalchemy model easily by forcing a defined
    # interface (that isn't the sqlalchemy model itself).
    td_c = taskdetail.TaskDetail(td.name, uuid=td.uuid, backend='sqlalchemy')
    td_c.state = td.state
    td_c.results = td.results
    td_c.exception = td.exception
    td_c.stacktrace = td.stacktrace
    td_c.meta = td.meta
    td_c.version = td.version
    return td_c


def _convert_lb_to_external(lb_m):
    """Don't expose the internal sqlalchemy ORM model to the external api."""
    lb_c = logbook.LogBook(lb_m.name, lb_m.uuid,
                           updated_at=lb_m.updated_at,
                           created_at=lb_m.created_at,
                           backend='sqlalchemy')
    lb_c.meta = lb_m.meta
    for fd_m in lb_m.flowdetails:
        lb_c.add(_convert_fd_to_external(fd_m))
    return lb_c


def _convert_lb_to_internal(lb_c):
    """Don't expose the external model to the sqlalchemy ORM model."""
    lb_m = models.LogBook(uuid=lb_c.uuid, meta=lb_c.meta, name=lb_c.name)
    lb_m.flowdetails = []
    for fd_c in lb_c:
        lb_m.flowdetails.append(_convert_fd_to_internal(fd_c, lb_c.uuid))
    return lb_m


def _logbook_get_model(lb_id, session):
    entry = session.query(models.LogBook).filter_by(uuid=lb_id).first()
    if entry is None:
        raise exc.NotFound("No logbook found with id: %s" % lb_id)
    return entry


def _flow_details_get_model(f_id, session):
    entry = session.query(models.FlowDetail).filter_by(uuid=f_id).first()
    if entry is None:
        raise exc.NotFound("No flow details found with id: %s" % f_id)
    return entry


def _task_details_get_model(t_id, session):
    entry = session.query(models.TaskDetail).filter_by(uuid=t_id).first()
    if entry is None:
        raise exc.NotFound("No task details found with id: %s" % t_id)
    return entry


def _taskdetails_merge(td_m, td):
    if td_m.state != td.state:
        td_m.state = td.state
    if td_m.results != td.results:
        td_m.results = td.results
    if td_m.exception != td.exception:
        td_m.exception = td.exception
    if td_m.stacktrace != td.stacktrace:
        td_m.stacktrace = td.stacktrace
    if td_m.meta != td.meta:
        td_m.meta = td.meta
    return td_m


def clear_all():
    session = db_session.get_session()
    with session.begin():
        # NOTE(harlowja): due to how we have our relationship setup and
        # cascading deletes are enabled, this will cause all associated task
        # details and flow details to automatically be purged.
        try:
            return session.query(models.LogBook).delete()
        except sql_exc.DBAPIError as e:
            raise exc.StorageError("Failed clearing all entries: %s" % e, e)


def taskdetails_save(td):
    # Must already exist since a tasks details has a strong connection to
    # a flow details, and tasks details can not be saved on there own since
    # they *must* have a connection to an existing flow details.
    session = db_session.get_session()
    with session.begin():
        td_m = _task_details_get_model(td.uuid, session=session)
        td_m = _taskdetails_merge(td_m, td)
        td_m = session.merge(td_m)
        return _convert_td_to_external(td_m)


def flowdetails_save(fd):
    # Must already exist since a flow details has a strong connection to
    # a logbook, and flow details can not be saved on there own since they
    # *must* have a connection to an existing logbook.
    session = db_session.get_session()
    with session.begin():
        fd_m = _flow_details_get_model(fd.uuid, session=session)
        if fd_m.meta != fd.meta:
            fd_m.meta = fd.meta
        if fd_m.state != fd.state:
            fd_m.state = fd.state
        for td in fd:
            updated = False
            for td_m in fd_m.taskdetails:
                if td_m.uuid == td.uuid:
                    updated = True
                    td_m = _taskdetails_merge(td_m, td)
                    break
            if not updated:
                fd_m.taskdetails.append(_convert_td_to_internal(td, fd_m.uuid))
        fd_m = session.merge(fd_m)
        return _convert_fd_to_external(fd_m)


def logbook_destroy(lb_id):
    session = db_session.get_session()
    with session.begin():
        try:
            lb = _logbook_get_model(lb_id, session=session)
            session.delete(lb)
        except sql_exc.DBAPIError as e:
            raise exc.StorageError("Failed destroying"
                                   " logbook %s: %s" % (lb_id, e), e)


def logbook_save(lb):
    session = db_session.get_session()
    with session.begin():
        try:
            lb_m = _logbook_get_model(lb.uuid, session=session)
            # NOTE(harlowja): Merge them (note that this doesn't provide 100%
            # correct update semantics due to how databases have MVCC). This
            # is where a stored procedure or a better backing store would
            # handle this better (something more suited to this type of data).
            for fd in lb:
                existing_fd = False
                for fd_m in lb_m.flowdetails:
                    if fd_m.uuid == fd.uuid:
                        existing_fd = True
                        if fd_m.meta != fd.meta:
                            fd_m.meta = fd.meta
                        if fd_m.state != fd.state:
                            fd_m.state = fd.state
                        for td in fd:
                            existing_td = False
                            for td_m in fd_m.taskdetails:
                                if td_m.uuid == td.uuid:
                                    existing_td = True
                                    td_m = _taskdetails_merge(td_m, td)
                                    break
                            if not existing_td:
                                td_m = _convert_td_to_internal(td, fd_m.uuid)
                                fd_m.taskdetails.append(td_m)
                if not existing_fd:
                    lb_m.flowdetails.append(_convert_fd_to_internal(fd,
                                                                    lb_m.uuid))
        except exc.NotFound:
            lb_m = _convert_lb_to_internal(lb)
        try:
            lb_m = session.merge(lb_m)
            return _convert_lb_to_external(lb_m)
        except sql_exc.DBAPIError as e:
            raise exc.StorageError("Failed saving"
                                   " logbook %s: %s" % (lb.uuid, e), e)


def logbook_get(lb_id):
    session = db_session.get_session()
    try:
        lb_m = _logbook_get_model(lb_id, session=session)
        return _convert_lb_to_external(lb_m)
    except sql_exc.DBAPIError as e:
        raise exc.StorageError("Failed getting"
                               " logbook %s: %s" % (lb_id, e), e)
