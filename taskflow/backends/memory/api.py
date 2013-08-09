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

"""Implementation of in-memory backend."""

import logging

from taskflow.backends.memory import memory
from taskflow import exceptions as exception
from taskflow.generics import flowdetail
from taskflow.generics import logbook
from taskflow.generics import taskdetail
from taskflow.utils import LockingDict

LOG = logging.getLogger(__name__)

logbooks = LockingDict()
flowdetails = LockingDict()
taskdetails = LockingDict()


"""
LOGBOOK
"""


def logbook_create(name, lb_id=None):
    """Creates a new LogBook model with matching lb_id"""
    # Create the LogBook model
    lb = memory.MemoryLogBook(name, lb_id)

    # Store it in the LockingDict for LogBooks
    logbooks[lb_id] = lb


def logbook_destroy(lb_id):
    """Deletes the LogBook model with matching lb_id"""
    # Try deleting the LogBook
    try:
        del logbooks[lb_id]
    # Raise a NotFound error if the LogBook doesn't exist
    except KeyError:
        raise exception.NotFound("No Logbook found with id "
                                 "%s." % (lb_id,))


def logbook_save(lb):
    """Saves a generic LogBook object to the db"""
    # Create a LogBook model if one doesn't exist
    if not _logbook_exists(lb.uuid):
        logbook_create(lb.name, lb.uuid)

    # Get a copy of the LogBook model in the LockingDict
    ld_lb = logbook_get(lb.uuid)

    for fd in lb:
        # Save each FlowDetail the LogBook to save has
        fd.save()

        # Add the FlowDetail to the LogBook model if not already there
        if fd not in ld_lb:
            logbook_add_flow_detail(lb.uuid, fd.uuid)


def logbook_delete(lb):
    """Deletes a LogBook from db based on a generic type"""
    # Try to get the LogBook
    try:
        ld_lb = logbooks[lb.uuid]
    # Raise a NotFound exception if the LogBook cannot be found
    except KeyError:
        raise exception.NotFound("No Logbook found with id "
                                 "%s." % (lb.uuid,))

    # Raise an error if the LogBook still has FlowDetails
    if len(ld_lb):
        raise exception.Error("Logbook <%s> still has "
                              "dependents." % (lb.uuid,))
    # Destroy the LogBook model if it is safe
    else:
        logbook_destroy(lb.uuid)


def logbook_get(lb_id):
    """Gets a LogBook with matching lb_id, if it exists"""
    # Try to get the LogBook
    try:
        ld_lb = logbooks[lb_id]
    # Raise a NotFound exception if the LogBook is not there
    except KeyError:
        raise exception.NotFound("No Logbook found with id "
                                 "%s." % (lb_id,))

    # Acquire a read lock on the LogBook
    with ld_lb.acquire_lock(read=True):
        # Create a generic type LogBook to return
        retVal = logbook.LogBook(ld_lb.name, ld_lb.uuid)

        # Attach the appropriate generic FlowDetails to the generic LogBook
        for fd in ld_lb:
            retVal.add_flow_detail(flowdetail_get(fd.uuid))

    return retVal


def logbook_add_flow_detail(lb_id, fd_id):
    """Adds a FlowDetail with id fd_id to a LogBook with id lb_id"""
    # Try to get the LogBook
    try:
        ld_lb = logbooks[lb_id]
    # Raise a NotFound exception if the LogBook is not there
    except KeyError:
        raise exception.NotFound("No Logbook found with id "
                                 "%s." % (lb_id,))

    # Try to get the FlowDetail to add
    try:
        ld_fd = flowdetails[fd_id]
    # Raise a NotFound exception if the FlowDetail is not there
    except KeyError:
        raise exception.NotFound("No FlowDetail found with id "
                                 "%s." % (fd_id,))

    # Acquire a write lock on the LogBook
    with ld_lb.acquire_lock(read=False):
        # Add the FlowDetail model to the LogBook model
        ld_lb.add_flow_detail(ld_fd)


def logbook_remove_flowdetail(lb_id, fd_id):
    """Removes a FlowDetail with id fd_id from a LogBook with id lb_id"""
    # Try to get the LogBook
    try:
        ld_lb = logbooks[lb_id]
    # Raise a NotFound exception if it is not there
    except KeyError:
        raise exception.NotFound("No Logbook found with id "
                                 "%s." % (lb_id,))

    # Try to get the FlowDetail to remove
    try:
        ld_fd = flowdetails[fd_id]
    # Raise a NotFound exception if it is not there
    except KeyError:
        raise exception.NotFound("No FlowDetail found with id "
                                 "%s." % (fd_id,))

    # Acquire a write lock on the LogBook model
    with ld_lb.acquire_lock(read=False):
        # Remove the FlowDetail from the LogBook model
        ld_lb.remove_flow_detail(ld_fd)


def logbook_get_ids_names():
    """Returns all LogBook ids and names"""
    lb_ids = []
    lb_names = []

    # Iterate through the LockingDict and append to the Lists
    for (k, v) in logbooks.items():
        lb_ids.append(k)
        lb_names.append(v.name)

    # Return a dict of the ids and names
    return dict(zip(lb_ids, lb_names))


def _logbook_exists(lb_id, session=None):
    """Check if a LogBook with lb_id exists"""
    return lb_id in logbooks.keys()


"""
FLOWDETAIL
"""


def flowdetail_create(name, wf, fd_id=None):
    """Create a new FlowDetail model with matching fd_id"""
    # Create a FlowDetail model to save
    fd = memory.MemoryFlowDetail(name, wf, fd_id)

    #Save the FlowDetail model to the LockingDict
    flowdetails[fd_id] = fd


def flowdetail_destroy(fd_id):
    """Deletes the FlowDetail model with matching fd_id"""
    # Try to delete the FlowDetail model
    try:
        del flowdetails[fd_id]
    # Raise a NotFound exception if the FlowDetail is not there
    except KeyError:
        raise exception.NotFound("No FlowDetail found with id "
                                 "%s." % (fd_id,))


def flowdetail_save(fd):
    """Saves a generic FlowDetail object to the db"""
    # Create a FlowDetail model if one does not exist
    if not _flowdetail_exists(fd.uuid):
        flowdetail_create(fd.name, fd.flow, fd.uuid)

    # Get a copy of the FlowDetail model in the LockingDict
    ld_fd = flowdetail_get(fd.uuid)

    for td in fd:
        # Save the TaskDetails in the FlowDetail to save
        td.save()

        # Add the TaskDetail model to the FlowDetail model if it is not there
        if td not in ld_fd:
            flowdetail_add_task_detail(fd.uuid, td.uuid)


def flowdetail_delete(fd):
    """Deletes a FlowDetail from db based on a generic type"""
    # Try to get the FlowDetails
    try:
        ld_fd = flowdetails[fd.uuid]
    # Raise a NotFound exception if the FlowDetail is not there
    except KeyError:
        raise exception.NotFound("No FlowDetail found with id "
                                 "%s." % (fd.uuid,))

    # Raise an error if the FlowDetail still has TaskDetails
    if len(ld_fd):
        raise exception.Error("FlowDetail <%s> still has "
                              "dependents." % (fd.uuid,))
    # If it is safe, delete the FlowDetail model
    else:
        flowdetail_destroy(fd.uuid)


def flowdetail_get(fd_id):
    """Gets a FlowDetail with matching fd_id, if it exists"""
    # Try to get the FlowDetail
    try:
        fd = flowdetails[fd_id]
    # Raise a NotFound exception if it is not there
    except KeyError:
        raise exception.NotFound("No FlowDetail found with id "
                                 "%s." % (fd_id,))

    # Acquire a read lock on the FlowDetail
    with fd.acquire_lock(read=True):
        # Get the Flow this FlowDetail represents
        wf = fd.flow

        # Create a FlowDetail to return
        retVal = flowdetail.FlowDetail(fd.name, wf, fd.uuid)

        # Change updated_at to reflect the current data
        retVal.updated_at = fd.updated_at

        # Add the generic TaskDetails to the FlowDetail to return
        for td in fd:
            retVal.add_task_detail(taskdetail_get(td.uuid))

    return retVal


def flowdetail_add_task_detail(fd_id, td_id):
    """Adds a TaskDetail with id td_id to a Flowdetail with id fd_id"""
    # Try to get the FlowDetail
    try:
        fd = flowdetails[fd_id]
    # Raise a NotFound exception if it is not there
    except KeyError:
        raise exception.NotFound("No FlowDetail found with id "
                                 "%s." % (fd_id,))

    # Try to get the TaskDetail to add
    try:
        td = taskdetails[td_id]
    # Raise a NotFound exception if it is not there
    except KeyError:
        raise exception.NotFound("No TaskDetail found with id "
                                 "%s." % (td_id,))

    # Acquire a write lock on the FlowDetail model
    with fd.acquire_lock(read=False):
        # Add the TaskDetail to the FlowDetail model
        fd.add_task_detail(td)


def flowdetail_remove_taskdetail(fd_id, td_id):
    """Removes a TaskDetail with id td_id from a FlowDetail with id fd_id"""
    # Try to get the FlowDetail
    try:
        fd = flowdetails[fd_id]
    # Raise a NotFound exception if it is not there
    except KeyError:
        raise exception.NotFound("No FlowDetail found with id "
                                 "%s." % (fd_id,))

    # Try to get the TaskDetail to remove
    try:
        td = taskdetails[td_id]
    # Raise a NotFound exception if it is not there
    except KeyError:
        raise exception.NotFound("No TaskDetail found with id "
                                 "%s." % (td_id,))

    # Acquire a write lock on the FlowDetail
    with fd.acquire_lock(read=False):
        # Remove the TaskDetail model from the FlowDetail model
        fd.remove_task_detail(td)


def flowdetail_get_ids_names():
    """Returns all FlowDetail ids and names"""
    fd_ids = []
    fd_names = []

    # Iterate through the LockingDict and append to lists
    for (k, v) in flowdetails.items():
        fd_ids.append(k)
        fd_names.append(v.name)

    # Return a dict of the uuids and names
    return dict(zip(fd_ids, fd_names))


def _flowdetail_exists(fd_id):
    """Checks if a FlowDetail with fd_id exists"""
    return fd_id in flowdetails.keys()


"""
TASKDETAIL
"""


def taskdetail_create(name, tsk, td_id=None):
    """Create a new TaskDetail model with matching td_id"""
    # Create a TaskDetail model to save
    td = memory.MemoryTaskDetail(name, tsk, td_id)
    # Save the TaskDetail model to the LockingDict
    taskdetails[td_id] = td


def taskdetail_destroy(td_id):
    """Deletes the TaskDetail model with matching td_id"""
    # Try to delete the TaskDetails
    try:
        del taskdetails[td_id]
    # Raise a NotFound exception if it is not there
    except KeyError:
        raise exception.NotFound("No TaskDetail found with id "
                                 "%s." % (td_id,))


def taskdetail_save(td):
    """Saves a generic TaskDetail object to the db"""
    # Create a TaskDetail model if it doesn't exist
    if not _taskdetail_exists(td.uuid):
        taskdetail_create(td.name, td.task, td.uuid)

    # Prepare values for updating
    values = dict(state=td.state,
                  results=td.results,
                  exception=td.exception,
                  stacktrace=td.stacktrace,
                  meta=td.meta)

    # Update the values of the TaskDetail model
    taskdetail_update(td.uuid, values)


def taskdetail_delete(td):
    """Deletes a TaskDetail from db based on a generic type"""
    # Destroy the TaskDetail model
    taskdetail_destroy(td.uuid)


def taskdetail_get(td_id):
    """Gets a TaskDetail with matching td_id, if it exists"""
    # Try to get the TaskDetail
    try:
        ld_td = taskdetails[td_id]
    # Raise NotFound exception if it is not there
    except KeyError:
        raise exception.NotFound("No TaskDetail found with id "
                                 "%s." % (td_id,))

    # Acquire a read lock
    with ld_td.acquire_lock(read=True):
        # Get the Task this TaskDetail represents
        tsk = ld_td.task

        # Update TaskDetail to return
        retVal = taskdetail.TaskDetail(ld_td.name, tsk, ld_td.uuid)
        retVal.updated_at = ld_td.updated_at
        retVal.state = ld_td.state
        retVal.results = ld_td.results
        retVal.exception = ld_td.exception
        retVal.stacktrace = ld_td.stacktrace
        retVal.meta = ld_td.meta

    return retVal


def taskdetail_update(td_id, values):
    """Updates a TaskDetail with matching td_id"""
    # Try to get the TaskDetail
    try:
        ld_td = taskdetails[td_id]
    # Raise a NotFound exception if it is not there
    except KeyError:
        raise exception.NotFound("No TaskDetail found with id "
                                 "%s." % (td_id,))

    # Acquire a write lock for the TaskDetail
    with ld_td.acquire_lock(read=False):
        # Write the values to the TaskDetail
        for k, v in values.iteritems():
            setattr(ld_td, k, v)


def taskdetail_get_ids_names():
    """Returns all TaskDetail ids and names"""
    td_ids = []
    td_names = []

    # Iterate through the LockingDict and append to Lists
    for (k, v) in taskdetails.items():
        td_ids.append(k)
        td_names.append(v.name)

    # Return a dict of uuids and names
    return dict(zip(td_ids, td_names))


def _taskdetail_exists(td_id, session=None):
    """Check if a TaskDetail with td_id exists"""
    return td_id in taskdetails.keys()
