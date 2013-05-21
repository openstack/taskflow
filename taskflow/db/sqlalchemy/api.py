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

def workflow_get(context, workflow_id):
    """Return one workflow with matching workflow_id"""
    result = model_query(context, models.Workflow).get(workflow_id)

    if not result:
        raise exception.NotFound("No workflow found "
                                 "with id %s." % (workflow_id,))

def workflow_get_all(context):
    """Return all workflows"""
    results = model_query(context, models.Workflow).all()

    if not results:
        raise exception.NotFound("No workflows were found.")

    return results

def workflow_get_names(context):
    """Return all workflow names"""
    results = model_auery(context, models.Workflow.name).all()

    return zip(*results)

def workflow_create(context, workflow_id):
    """Create new workflow with workflow_id"""
    
    
