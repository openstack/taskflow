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

import logging

import stevedore.driver

from taskflow import exceptions as exc

# NOTE(harlowja): this is the entrypoint namespace, not the module namespace.
CONDUCTOR_NAMESPACE = 'taskflow.conductors'

LOG = logging.getLogger(__name__)


def fetch(kind, name, jobboard, namespace=CONDUCTOR_NAMESPACE, **kwargs):
    """Fetch a conductor backend with the given options.

    This fetch method will look for the entrypoint 'kind' in the entrypoint
    namespace, and then attempt to instantiate that entrypoint using the
    provided name, jobboard and any board specific kwargs.
    """
    LOG.debug('Looking for %r conductor driver in %r', kind, namespace)
    try:
        mgr = stevedore.driver.DriverManager(
            namespace, kind,
            invoke_on_load=True,
            invoke_args=(name, jobboard),
            invoke_kwds=kwargs)
        return mgr.driver
    except RuntimeError as e:
        raise exc.NotFound("Could not find conductor %s" % (kind), e)
