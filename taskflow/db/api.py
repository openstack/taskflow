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

"""Implementation of SQLAlchemy Backend"""

from oslo.config import cfg

from taskflow.common import config
from taskflow import utils

SQL_CONNECTION = 'sqlite://'
db_opts = [
    cfg.StrOpt('db_backend',
               default='sqlalchemy',
               help='The backend to use for db')]

CONF = cfg.CONF
CONF.register_opts(db_opts)

IMPL = utils.LazyPluggable('db_backend',
                           sqlalchemy='taskflow.db.sqlalchemy.api')

def configure():
    global SQL_CONNECTION
    global SQL_IDLE_TIMEOUT
    config.register_db_opts()
    SQL_CONNECTION = cfg.CONF.sql_connection
    SQL_IDLE_TIMEOUT = cfg.CONF.sql_idle_timeout

"""
LOGBOOK
"""

def logbook_get(context, lb_id):
    return IMPL.logbook_get(context, lb_id)

def logbook_get_by_name(context, lb_name):
    return IMPL.logbook_get_by_name(context, lb_name)

def logbook_create(context, lb_name, lb_id=None):
    return IMPL.logbook_create(context, lb_name, lb_id)

def logbook_get_workflows(context, lb_id):
    return IMPL.logbook_get_workflows(context, lb_id)

def logbook_add_workflow(context, lb_id, wf_name):
    return IMPL.logbook_add_workflow(context, lb_id, wf_name)

def logbook_destroy(context, lb_id):
    return IMPL.logbook_destroy(context, lb_id)

"""
JOB
"""

def job_get(context, job_id):
    return IMPL.job_get(context, job_id)

def job_update(context, job_id, values):
    return IMPL.job_update(context, job_id, values)

def job_add_workflow(context, job_id, wf_id):
    return IMPL.job_add_workflow(context, job_id, wf_id)

def job_get_owner(context, job_id):
    return IMPL.job_get_owner(context, job_id)

def job_get_state(context, job_id):
    return IMPL.job_get_state(context, job_id)

def job_destroy(context, job_id):
    return IMPL.job_destroy(context, job_id)

"""
WORKFLOW
"""

def workflow_get(context, wf_name):
    return IMPL.workflow_get(context, wf_name)

def workflow_get_all(context):
    return IMPL.workflow_get_all(context)

def workflow_get_names(context):
    return IMPL.workflow_get_names(context)

def workflow_get_tasks(context, wf_name):
    return IMPL.workflow_get_tasks(context, wf_name)

def workflow_add_task(context, wf_name, task_id):
    return IMPL.workflow_add_task(context, wf_name, task_id)

def workflow_create(context, wf_name):
    return IMPL.workflow_create(context, wf_name)

def workflow_destroy(context, wf_name):
    return IMPL.workflow_destroy(context, wf_name)

"""
TASK
"""

def task_get(context, task_id):
    return IMPL.task_get(context, task_id)

def task_create(context, task_name, wf_id, task_id=None):
    return IMPL.task_create(context, task_name, wf_id, task_id)

def task_update(context, task_id, values):
    return IMPL.task_update(context, task_id, values)

def task_destroy(context, task_id):
    return IMPL.task_destroy(context, task_id)
