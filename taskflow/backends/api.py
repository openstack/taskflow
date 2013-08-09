# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

"""Backend persistence API"""

from oslo.config import cfg

from taskflow.common import config
from taskflow import utils

db_opts = [
    cfg.StrOpt('db_backend',
               default='sqlalchemy',
               help='The backend to use for db')]
mem_opts = [
    cfg.StrOpt('mem_backend',
               default='memory',
               help='The backend to use for in-memory')]


CONF = cfg.CONF
CONF.register_opts(db_opts)
CONF.register_opts(mem_opts)

IMPL = utils.LazyPluggable('mem_backend',
                           memory='taskflow.backends.memory.api',
                           sqlalchemy='taskflow.backends.sqlalchemy.api')


def configure(pivot='mem_backend'):
    IMPL.set_pivot(pivot)

    if pivot == 'db_backend':
        global SQL_CONNECTION
        global SQL_IDLE_TIMEOUT
        config.register_db_opts()
        SQL_CONNECTION = cfg.CONF.sql_connection
        SQL_IDLE_TIMEOUT = cfg.CONF.sql_idle_timeout


"""
LOGBOOK
"""


def logbook_create(name, lb_id=None):
    """Create a logbook

    This method creates a logbook representation in the currently selected
    backend to be persisted.

    PARAMETERS:
        name: str

            Specifies the name of the logbook represented.

        lb_id: str

            Specifies the uuid of the logbook represented. If this element is
            left as None, the table will generate a uuid for the logbook. This
            field must be unique for all logbooks stored in the backend. In the
            event that a logbook with this uuid already exists, the backend
            will raise an exception.
    """
    return IMPL.logbook_create(name, lb_id)


def logbook_destroy(lb_id):
    """Destroy a logbook

    This method will destroy a logbook representation with a uuid matching
    lb_id from the currently selected backend.

    PARAMETERS:
        lb_id: str

            Specifies the uuid of the logbook representation to be destroyed
            from the currently selected backend. If a logbook with matching
            uuid is not present, the backend will raise an exception.
    """
    return IMPL.logbook_destroy(lb_id)


def logbook_save(lb):
    """Save a logbook

    This method creates a logbook representation in the currently selected
    backend if one does not already exists and updates it with attributes from
    the generic LogBook passed in as a parameter.

    PARAMETERS:
        lb: taskflow.generics.LogBook

            The generic type LogBook to be persisted in the backend. If a
            representation does not yet exist, one will be created. The
            representation will be updated to match the attributes of this
            object.
    """
    return IMPL.logbook_save(lb)


def logbook_delete(lb):
    """Delete a logbook

    This method destroys a logbook representation in the curretly selected
    backend.

    PARAMETERS:
        lb: taskflow.generics.LogBook

            The generic type LogBook whose representation is to be destroyed
            from the backend. If a representation does not exist, the backend
            will raise an exception.
    """
    return IMPL.logbook_delete(lb)


def logbook_get(lb_id):
    """Get a logbook

    This method returns a generic type LogBook based on its representation in
    the currently selected backend.

    PARAMETERS:
        lb_id: str

            Specifies the uuid of the logbook representation to be used. If a
            logbook with this uuid does not exist, the backend will raise an
            exception.

    RETURNS:
        a generic type LogBook that reflects what is stored by a logbook
        representation in the backend.
    """
    return IMPL.logbook_get(lb_id)


def logbook_add_flow_detail(lb_id, fd_id):
    """Add a flowdetail

    This method adds a flowdetail in the backend to the list of flowdetails
    contained by the specified logbook representation.

    PARAMETERS:
        lb_id: str

            Specifies the uuid of the logbook representation to which the
            flowdetail will be added. If a logbook with this uuid does not
            exist, the backend will raise an exception.

        fd_id: str

            Specifies the uuid of the flowdetail representation to be added
            to the specified logbook representation. If a flowdetail with this
            uuid does not exist, the backend will raise an exception.
    """
    return IMPL.logbook_add_flow_detail(lb_id, fd_id)


def logbook_remove_flowdetail(lb_id, fd_id):
    """Remove a flowdetail

    This method removes a flowdetail from the list of flowdetails contained by
    the specified logbook representation.

    PARAMETERS:
        lb_id: str

            Specifies the uuid of the logbook representation from which the
            flowdetail will be removed. If a logbook with this uuid does not
            exist, the backend will raise an exception.

        fd_id: str
            Specifies the uuid of the flowdetail representation to be removed
            from the specified logbook representation.
    """
    return IMPL.logbook_remove_flowdetail(lb_id, fd_id)


def logbook_get_ids_names():
    """Get the ids and names of all logbooks

    This method returns a dict of uuids and names of all logbook
    representations stored in the backend.

    RETURNS:
        a dict of uuids and names of all logbook representations
    """
    return IMPL.logbook_get_ids_names()


"""
FLOWDETAIL
"""


def flowdetail_create(name, wf, fd_id=None):
    """Create a flowdetail

    This method creates a flowdetail representation in the currently selected
    backend to be persisted.

    PARAMETERS:
        name: str

            Specifies the name of the flowdetail represented.

        wf: taskflow.generics.Flow

            The workflow object this flowdetail is to represent.

        fd_id: str

            Specifies the uuid of the flowdetail represented. If this element
            is left as None, the table will generate a uuid for the flowdetail.
            This field must be unique for all flowdetails stored in the
            backend. In the event that a flowdetail with this uuid already
            exists, the backend will raise an exception.
    """
    return IMPL.flowdetail_create(name, wf, fd_id)


def flowdetail_destroy(fd_id):
    """Destroy a flowdetail

    This method will destroy a flowdetail representation with a uuid matching
    fd_id from the currently selected backend.

    PARAMETERS:
        fd_id: str

            Specifices the uuid of the flowdetail representation to be
            destroyed from the currently selected backend. If a flowdetail with
            matching uuid is not present, the backend will raise an exception.
    """
    return IMPL.flowdetail_destroy(fd_id)


def flowdetail_save(fd):
    """Save a flowdetail

    This method creates a flowdetail representation in the currently selected
    backend if one does not already exist and updates it with attributes from
    the generic FlowDetail passed in as a parameter.

    PARAMETERS:
        fd: taskflow.generics.FlowDetail

            The generic type FlowDetail to be persisted in the backend. If a
            representation does not yet exist, one will be created. The
            representation will be updated to match the attributes of this
            object.
    """
    return IMPL.flowdetail_save(fd)


def flowdetail_delete(fd):
    """Delete a flowdetail

    This method destroys a flowdetail representation in the currently selected
    backend.

    PARAMETERS:
        fd: taskflow.generics.FlowDetail

            The generic type FlowDetail whose representation is to be destroyed
            from the backend. If a representation does not exist, the backend
            will raise an exception.
    """
    return IMPL.flowdetail_delete(fd)


def flowdetail_get(fd_id):
    """Get a flowdetail

    This method returns a generic type FlowDetail based on its representation
    in the currently selected backend.

    PARAMETERS:
        fd_id: str

            Specifies the uuid of the flowdetail representation to be used. If
            a flowdetail with this uuid does not exist, the backend will raise
            an exception.

    RETURNS:
        a generic type FlowDetail that reflects what is stored by a flowdetail
        representation in the backend.
    """
    return IMPL.flowdetail_get(fd_id)


def flowdetail_add_task_detail(fd_id, td_id):
    """Add a taskdetail

    This method adds a taskdetail in the backend to the list of taskdetails
    contained by the specified flowdetail representation.

    PARAMETERS:
        fd_id: str

            Specifies the uuid of the flowdetail representation to which the
            taskdetail will be added. If a flowdetail with this uuid does not
            exist, the backend will raise an exception.

        td_id: str

            Specifies the uuid of the taskdetail representation to be added
            to the specified flowdetail representation. If a flowdetail with
            this uuid does not exist, the backend will raise an exception.
    """
    return IMPL.flowdetail_add_task_detail(fd_id, td_id)


def flowdetail_remove_taskdetail(fd_id, td_id):
    """Remove a taskdetail

    This method removes a taskdetail from the list of taskdetails contained by
    the specified flowdetail representation.

    PARAMETERS:
        fd_id: str

            Specifies the uuid of the flowdetail representation from which the
            taskdetail will be removed. If a flowdetail with this uuid does not
            exist, the backend will raise an exception.

        td_id: str
            Specifies the uuid of the taskdetail representation to be removed
            from the specified flowdetail representation.
    """
    return IMPL.flowdetail_remove_taskdetail(fd_id, td_id)


def flowdetail_get_ids_names():
    """Get the ids and names of all flowdetails

    This method returns a dict of the uuids and names of all flowdetail
    representations stored in the backend.

    RETURNS:
        a dict of uuids and names of all flowdetail representations
    """
    return IMPL.flowdetail_get_ids_names()


"""
TASKDETAIL
"""


def taskdetail_create(name, tsk, td_id=None):
    """Create a taskdetail

    This method creates a taskdetail representation in the current selected
    backend to be persisted.

    PARAMETERS:
        name: str

            Specifies the name of the taskdetail represented.

        tsk: taskflow.generics.Task

            The task object this taskdetail is to represent.

        td_id: str

            Specifies the uuid of the taskdetail represented. If this element
            is left as None, the table will generate a uuid for the taskdetail.
            This field must be unique for all taskdetails stored in the
            backend. In the event that a taskdetail with this uuid already
            exists, the backend will raise an exception.
    """
    return IMPL.taskdetail_create(name, tsk, td_id)


def taskdetail_destroy(td_id):
    """Destroy a taskdetail

    This method will destroy a taskdetail representation with a uuid matching
    td_id from the currently selected backend.

    PARAMETERS:
        td_id: str

            Specifies the uuid of the taskdetail representation to be
            destroyed from the currently selected backend. If a taskdetail with
            matching uuid is not present, the backend will raise an exception.
    """
    return IMPL.taskdetail_destroy(td_id)


def taskdetail_save(td):
    """Save a taskdetail

    This method creates a taskdetail representation in the currently selected
    backend if one does not already exist and updates it with attributes from
    the generic TaskDetail passed in as a parameter.

    PARAMETERS:
        td: taskflow.generics.TaskDetail

            The generic type TaskDetail to be persisted in the backend. If a
            representation does not yet exist, one will be created. The
            representation will be updated to match the attributes of this
            object.
    """
    return IMPL.taskdetail_save(td)


def taskdetail_delete(td):
    """Delete a taskdetail

    This method destroys a taskdetail representation in the currently selected
    backend.

    PARAMETERS:
        td: taskdetail.generics.TaskDetail

            The generic type TaskDetail whose representation is to be destroyed
            from the backend. If a representation does not exist, the backend
            will raise an exception.
    """
    return IMPL.taskdetail_delete(td)


def taskdetail_get(td_id):
    """Get a taskdetail

    This method returns a generic type TaskDetail based on its representation
    in the currently selected backend.

    PARAMETERS:
        td_id: str

            Specifies the uuid of the taskdetail representation to be used. If
            a taskdetail with this uuid does not exist, the backend will raise
            an exception.

    RETURNS:
        a generic type TaskDetail that reflects what is stored by a taskdetail
        representation in the backend.
    """
    return IMPL.taskdetail_get(td_id)


def taskdetail_update(td_id, values):
    """Update a taskdetail

    This method updates the attributes of a taskdetail representation in the
    currently selected backend.

    PARAMETERS:
        td_id: str

            Specifies the uuid of the taskdetail representation to be updated.
            If a taskdetail with this uuid does not exist, the backend will
            raise an execption.

        values: dict

            Specifies the values to be updated and the values to which they are
            to be updated.
    """
    return IMPL.taskdetail_update(td_id, values)


def taskdetail_get_ids_names():
    """Gets the ids and names of all taskdetails

    This method returns a dict of the uuids and names of all taskdetail
    representations stored in the backend.

    RETURNS:
        a dict of uuids and names of all taskdetail representations
    """
    return IMPL.taskdetail_get_ids_names()
