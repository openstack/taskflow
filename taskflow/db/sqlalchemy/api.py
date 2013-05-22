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

from taskflow.db.sqlalchemy import models
from taskflow.db.sqlalchemy.session import get_session

from taskflow.openstack.common import exception

LOG = logging.getLogger(__name__)

def model_query(context, *args, **kwargs):
    session = kwargs.get('session') or get_session()
    query = session.query(*args)

    return query

"""
LOGBOOK
"""

def logbook_get(context, lb_id, session=None):
    """Return a logbook with matching lb_id"""
    query = model_query(context, models.LogBook, session=session).\
        filter_by(logbook_id=lb_id)

    if not query:
        raise exception.NotFound("No LogBook found with id "
                                 "%s." % (lb_id,))

    return query.first()

def logbook_get_by_name(context, lb_name):
    """Return all logbooks with matching name"""
    query = model_query(context, models.LogBook).\
        filter_by(name=lb_name)

    if not query:
        raise exception.NotFound("LogBook %s not found."
                                 % (lb_name,))

    return query.all()

def logbook_create(context, name, lb_id=None):
    """Create a new logbook"""
    lb_ref = models.LogBook()
    lb_ref.name = name
    if lb_id:
        lb_ref.logbook_id = lb_id
    lb_ref.save()

    return lb_ref

def logbook_get_workflows(context, lb_id):
    """Return all workflows associated with a logbook"""
    lb = logbook_get(context, lb_id)

    return lb.workflows

def logbook_add_workflow(context, lb_id, wf_name):
    """Add Workflow to given LogBook"""
    session = get_session()
    with session.begin():
        wf = workflow_get(context, wf_name, session=session)
        lb = logbook_get(context, lb_id, session=session)

        lb.workflows.append(wf)

        return lb.workflows

"""
JOB
"""

def job_get(context, job_id, session=None):
    """Return Job with matching job_id"""
    query = model_query(context, models.Workflow, session=session).\
        filter_by(job_id=job_id)

    if not query:
        raise exception.NotFound("No Job with id %s found"
                                 % (job_id,))


"""
WORKFLOW
"""

def workflow_get(context, wf_name, session=None):
    """Return one workflow with matching workflow_id"""
    query = model_query(context, models.Workflow, session=session).\
        filter_by(name=wf_name)

    if not query:
        raise exception.NotFound("Workflow %s not found." % (wf_name,))

    return query.first()

def workflow_get_all(context):
    """Return all workflows"""
    results = model_query(context, models.Workflow).all()

    if not results:
        raise exception.NotFound("No Workflows were found.")

    return results

def workflow_get_names(context):
    """Return all workflow names"""
    results = model_query(context, models.Workflow.name).all()

    return zip(*results)

def workflow_get_tasks(context, wf_name):
    """Return all tasks for a given Workflow"""
    wf = workflow_get(context, wf_name)

    return wf.tasks

def workflow_add_task(context, wf_id, task_id):
    """Add a task to a given workflow"""
    session = get_session()
    with session.begin():
        task = task_get(context, task_id, session=session)
        wf = workflow_get(context, wf_id, session=session)
        wf.tasks.append(task)
        return wf.tasks

def workflow_create(context, workflow_name):
    """Create new workflow with workflow_id"""
    workflow_ref = models.Workflow()
    workflow_ref.name = workflow_name
    workflow_ref.save()

    return workflow_ref

def workflow_destroy(context, wf_id):
    """Delete a given Workflow"""
    session = get_session()
    with session.begin():
        wf = workflow_get(context, wf_id, session=session)
        wf.delete()

"""
TASK
"""

def task_get(context, task_id, session=None):
    """Return Task with task_id"""
    result = model_query(context, models.Task, session=session).\
        filter_by(task_id=task_id)

    if not result:
        raise exception.NotFound("No Task found with id "
                                 "%s." % (task_id,))

    return result

def task_create(context, task_name, wf_id, task_id=None):
    """Create task associated with given workflow"""
    task_ref = models.Task()
    task_ref.name = task_name
    task_ref.wf_id = wf_id
    if task_id:
        task_ref.task_id = task_id
    task_ref.save()

    return task_ref

def task_update(context, task_id, values):
    """Update Task with given values"""
    session = get_session()
    with session.begin():
        task = task_get(context, task_id)

        task.update(values)
        task.save()

def task_destroy(context, task_id):
    """Delete an existing Task"""
    session = get_session()
    with session.begin():
        task = task_get(context, task_id, session=session)
        task.delete()
