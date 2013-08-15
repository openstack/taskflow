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

"""Implementation of SQLAlchemy backend."""

import logging

from sqlalchemy import exc
from taskflow import exceptions as exception
from taskflow.persistence.backends.sqlalchemy import models
from taskflow.persistence.backends.sqlalchemy import session as sql_session
from taskflow.persistence import flowdetail
from taskflow.persistence import logbook
from taskflow.persistence import taskdetail


LOG = logging.getLogger(__name__)


def model_query(*args, **kwargs):
    session = kwargs.get('session') or sql_session.get_session()
    query = session.query(*args)

    return query


"""
LOGBOOK
"""


def logbook_create(name, lb_id=None):
    """Creates a new LogBook model with matching lb_id"""
    # Create a LogBook model to save
    lb_ref = models.LogBook()
    # Update attributes of the LogBook model
    lb_ref.name = name
    if lb_id:
        lb_ref.logbook_id = lb_id
    # Save the LogBook to the database
    lb_ref.save()


def logbook_destroy(lb_id):
    """Deletes the LogBook model with matching lb_id"""
    # Get the session to interact with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the LogBook model
        lb = _logbook_get_model(lb_id, session=session)
        # Delete the LogBook model from the database
        lb.delete(session=session)


def logbook_save(lb):
    """Saves a generic LogBook object to the db"""
    # Try to create the LogBook model
    try:
        logbook_create(lb.name, lb.uuid)
    # Do nothing if it is already there
    except exc.IntegrityError:
        pass

    # Get a copy of the LogBook in the database
    db_lb = logbook_get(lb.uuid)

    for fd in lb:
        # Save each FlowDetail
        flowdetail_save(fd)

        # Add the FlowDetail model to the LogBook model if it is not there
        if fd not in db_lb:
            logbook_add_flow_detail(lb.uuid, fd.uuid)


def logbook_delete(lb):
    """Deletes a LogBook from db based on a generic type"""
    # Get a session to interact with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the LogBook model
        lb_model = _logbook_get_model(lb.uuid, session=session)

    # Raise an error if the LogBook model still has FlowDetails
    if lb_model.flowdetails:
        raise exception.Error("Logbook <%s> still has "
                              "dependents." % (lb.uuid,))
    # Destroy the model if it is safe
    else:
        logbook_destroy(lb.uuid)


def logbook_get(lb_id):
    """Gets a LogBook with matching lb_id, if it exists"""
    # Get a session to interact with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the LogBook model from the database
        lb = _logbook_get_model(lb_id, session=session)

    # Create a generic LogBook to return
    retVal = logbook.LogBook(lb.name, lb.logbook_id)

    # Add the generic FlowDetails associated with this LogBook
    for fd in lb.flowdetails:
        retVal.add_flow_detail(flowdetail_get(fd.flowdetail_id))

    return retVal


def logbook_add_flow_detail(lb_id, fd_id):
    """Adds a FlowDetail with id fd_id to a LogBook with id lb_id"""
    # Get a session to interact with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the LogBook model from the database
        lb = _logbook_get_model(lb_id, session=session)
        # Get the FlowDetail model from the database
        fd = _flowdetail_get_model(fd_id, session=session)
        # Add the FlowDetail model to the LogBook model
        lb.flowdetails.append(fd)


def logbook_remove_flowdetail(lb_id, fd_id):
    """Removes a FlowDetail with id fd_id from a LogBook with id lb_id"""
    # Get a session to interact with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the LogBook model
        lb = _logbook_get_model(lb_id, session=session)
        # Remove the FlowDetail model from the LogBook model
        lb.flowdetails = [fd for fd in lb.flowdetails
                          if fd.flowdetail_id != fd_id]


def logbook_get_ids_names():
    """Returns all LogBook ids and names"""
    # Get a List of all LogBook models
    lbs = model_query(models.LogBook).all()

    # Get all of the LogBook uuids
    lb_ids = [lb.logbook_id for lb in lbs]
    # Get all of the LogBook names
    names = [lb.name for lb in lbs]

    # Return a dict with uuids and names
    return dict(zip(lb_ids, names))


def _logbook_get_model(lb_id, session=None):
    """Gets a LogBook model with matching lb_id, if it exists"""
    # Get a query of LogBooks by uuid
    query = model_query(models.LogBook, session=session).\
        filter_by(logbook_id=lb_id)

    # If there are no elements in the Query, raise a NotFound exception
    if not query.first():
        raise exception.NotFound("No LogBook found with id "
                                 "%s." % (lb_id,))

    # Return the first item in the Query
    return query.first()


def _logbook_exists(lb_id, session=None):
    """Check if a LogBook with lb_id exists"""
    # Gets a Query of all LogBook models
    query = model_query(models.LogBook, session=session).\
        filter_by(logbook_id=lb_id)

    # Return False if the query is empty
    if not query.first():
        return False

    # Return True if there is something in the query
    return True


"""
FLOWDETAIL
"""


def flowdetail_create(name, wf, fd_id=None):
    """Create a new FlowDetail model with matching fd_id"""
    # Create a FlowDetail model to be saved
    fd_ref = models.FlowDetail()
    # Update attributes of FlowDetail model to be saved
    fd_ref.name = name
    if fd_id:
        fd_ref.flowdetail_id = fd_id
    # Save FlowDetail model to database
    fd_ref.save()


def flowdetail_destroy(fd_id):
    """Deletes the FlowDetail model with matching fd_id"""
    # Get a session for interaction with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the FlowDetail model
        fd = _flowdetail_get_model(fd_id, session=session)
        # Delete the FlowDetail from the database
        fd.delete(session=session)


def flowdetail_save(fd):
    """Saves a generic FlowDetail object to the db"""
    # Try to create the FlowDetail model
    try:
        flowdetail_create(fd.name, fd.flow, fd.uuid)
    # Do nothing if it is already there
    except exc.IntegrityError:
        pass

    # Get a copy of the FlowDetail in the database
    db_fd = flowdetail_get(fd.uuid)

    for td in fd:
        # Save each TaskDetail
        taskdetail_save(td)

        # Add the TaskDetail model to the FlowDetail model if it is not there
        if td not in db_fd:
            flowdetail_add_task_detail(fd.uuid, td.uuid)


def flowdetail_delete(fd):
    """Deletes a FlowDetail from db based on a generic type"""
    # Get a session to interact with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the FlowDetail model
        fd_model = _flowdetail_get_model(fd.uuid, session=session)

    # Raise an error if the FlowDetail model still has TaskDetails
    if fd_model.taskdetails:
        raise exception.Error("FlowDetail <%s> still has "
                              "dependents." % (fd.uuid,))
    # If it is safe, destroy the FlowDetail model from the database
    else:
        flowdetail_destroy(fd.uuid)


def flowdetail_get(fd_id):
    """Gets a FlowDetail with matching fd_id, if it exists"""
    # Get a session for interaction with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the FlowDetail model from the database
        fd = _flowdetail_get_model(fd_id, session=session)

    # Create a generic FlowDetail to return
    retVal = flowdetail.FlowDetail(fd.name, None, fd.flowdetail_id)

    # Update attributes to match
    retVal.updated_at = fd.updated_at

    # Add the TaskDetails belonging to this FlowDetail to itself
    for td in fd.taskdetails:
        retVal.add_task_detail(taskdetail_get(td.taskdetail_id))

    return retVal


def flowdetail_add_task_detail(fd_id, td_id):
    """Adds a TaskDetail with id td_id to a Flowdetail with id fd_id"""
    # Get a session for interaction with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the FlowDetail model
        fd = _flowdetail_get_model(fd_id, session=session)
        # Get the TaskDetail model
        td = _taskdetail_get_model(td_id, session=session)
        # Add the TaskDetail model to the FlowDetail model
        fd.taskdetails.append(td)


def flowdetail_remove_taskdetail(fd_id, td_id):
    """Removes a TaskDetail with id td_id from a FlowDetail with id fd_id"""
    # Get a session for interaction with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the FlowDetail model
        fd = _flowdetail_get_model(fd_id, session=session)
        # Remove the TaskDetail from the FlowDetail model
        fd.taskdetails = [td for td in fd.taskdetails
                          if td.taskdetail_id != td_id]


def flowdetail_get_ids_names():
    """Returns all FlowDetail ids and names"""
    # Get all FlowDetail models
    fds = model_query(models.FlowDetail).all()

    # Get the uuids of all FlowDetail models
    fd_ids = [fd.flowdetail_id for fd in fds]
    # Get the names of all FlowDetail models
    names = [fd.name for fd in fds]

    # Return a dict of uuids and names
    return dict(zip(fd_ids, names))


def _flowdetail_get_model(fd_id, session=None):
    """Gets a FlowDetail model with matching fd_id, if it exists"""
    # Get a query of FlowDetails by uuid
    query = model_query(models.FlowDetail, session=session).\
        filter_by(flowdetail_id=fd_id)

    # Raise a NotFound exception if the query is empty
    if not query.first():
        raise exception.NotFound("No FlowDetail found with id "
                                 "%s." % (fd_id,))

    # Return the first entry in the query
    return query.first()


def _flowdetail_exists(fd_id, session=None):
    """Checks if a FlowDetail with fd_id exists"""
    # Get a query of FlowDetails by uuid
    query = model_query(models.FlowDetail, session=session).\
        filter_by(flowdetail_id=fd_id)

    # Return False if the query is empty
    if not query.first():
        return False

    # Return True if there is something in the query
    return True


"""
TASKDETAIL
"""


def taskdetail_create(name, tsk, td_id=None):
    """Create a new TaskDetail model with matching td_id"""
    # Create a TaskDetail model to add
    td_ref = models.TaskDetail()
    # Update the attributes of the TaskDetail model to add
    td_ref.name = name
    if td_id:
        td_ref.taskdetail_id = td_id

    td_ref.task_id = tsk.uuid
    td_ref.task_name = tsk.name
    td_ref.task_provides = list(tsk.provides)
    td_ref.task_requires = list(tsk.requires)
    td_ref.task_optional = list(tsk.optional)
    # Save the TaskDetail model to the database
    td_ref.save()


def taskdetail_destroy(td_id):
    """Deletes the TaskDetail model with matching td_id"""
    # Get a session for interaction with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the TaskDetail model to delete
        td = _taskdetail_get_model(td_id, session=session)
        # Delete the TaskDetail model from the database
        td.delete(session=session)


def taskdetail_save(td):
    """Saves a generic TaskDetail object to the db"""
    # Create a TaskDetail model if it does not already exist
    if not _taskdetail_exists(td.uuid):
        taskdetail_create(td.name, td.task, td.uuid)

    # Prepare values to be saved to the TaskDetail model
    values = dict(state=td.state,
                  results=td.results,
                  exception=td.exception,
                  stacktrace=td.stacktrace,
                  meta=td.meta)

    # Update the TaskDetail model with the values of the generic TaskDetail
    taskdetail_update(td.uuid, values)


def taskdetail_delete(td):
    """Deletes a TaskDetail from db based on a generic type"""
    # Destroy the TaskDetail if it exists
    taskdetail_destroy(td.uuid)


def taskdetail_get(td_id):
    """Gets a TaskDetail with matching td_id, if it exists"""
    # Get a session for interaction with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the TaskDetail model
        td = _taskdetail_get_model(td_id, session=session)

    # Create a generic type Task to return as part of the TaskDetail
    tsk = None

    # Create a generic type TaskDetail to return
    retVal = taskdetail.TaskDetail(td.name, tsk, td.taskdetail_id)
    # Update the TaskDetail to reflect the data in the database
    retVal.updated_at = td.updated_at
    retVal.state = td.state
    retVal.results = td.results
    retVal.exception = td.exception
    retVal.stacktrace = td.stacktrace
    retVal.meta = td.meta

    return retVal


def taskdetail_update(td_id, values):
    """Updates a TaskDetail with matching td_id"""
    # Get a session for interaction with the database
    session = sql_session.get_session()
    with session.begin():
        # Get the TaskDetail model
        td = _taskdetail_get_model(td_id, session=session)

        # Update the TaskDetail model with values
        td.update(values)
        # Write the TaskDetail model changes to the database
        td.save(session=session)


def taskdetail_get_ids_names():
    """Returns all TaskDetail ids and names"""
    # Get all TaskDetail models
    tds = model_query(models.TaskDetail).all()

    # Get the list of TaskDetail uuids
    td_ids = [td.taskdetail_id for td in tds]
    # Get the list of TaskDetail names
    names = [td.name for td in tds]

    #Return a dict of uuids and names
    return dict(zip(td_ids, names))


def _taskdetail_get_model(td_id, session=None):
    """Gets a TaskDetail model with matching td_id, if it exists"""
    # Get a query of TaskDetails by uuid
    query = model_query(models.TaskDetail, session=session).\
        filter_by(taskdetail_id=td_id)

    # Raise a NotFound exception if the query is empty
    if not query.first():
        raise exception.NotFound("No TaskDetail found with id "
                                 "%s." % (td_id,))

    return query.first()


def _taskdetail_exists(td_id, session=None):
    """Check if a TaskDetail with td_id exists"""
    # Get a query of TaskDetails by uuid
    query = model_query(models.TaskDetail, session=session).\
        filter_by(taskdetail_id=td_id)

    # Return False if the query is empty
    if not query.first():
        return False

    # Return True if there is something in the query
    return True
