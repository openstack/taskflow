# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import logging
from stevedore import driver

from taskflow import exceptions as exc
from taskflow.openstack.common.py3kcompat import urlutils


# NOTE(harlowja): this is the entrypoint namespace, not the module namespace.
BACKEND_NAMESPACE = 'taskflow.persistence'

LOG = logging.getLogger(__name__)


def fetch(conf, namespace=BACKEND_NAMESPACE):
    backend_name = urlutils.urlparse(conf['connection']).scheme
    LOG.debug('Looking for %r backend driver in %r', backend_name, namespace)
    try:
        mgr = driver.DriverManager(namespace, backend_name,
                                   invoke_on_load=True,
                                   invoke_kwds={'conf': conf})
        return mgr.driver
    except RuntimeError as e:
        raise exc.NotFound("Could not find backend %s: %s" % (backend_name, e))
