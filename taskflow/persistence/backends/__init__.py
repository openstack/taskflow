# -*- coding: utf-8 -*-

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

import contextlib
import logging
import re

from stevedore import driver

from taskflow import exceptions as exc


# NOTE(harlowja): this is the entrypoint namespace, not the module namespace.
BACKEND_NAMESPACE = 'taskflow.persistence'

# NOTE(imelnikov): regular expression to get scheme from URI,
# see RFC 3986 section 3.1
SCHEME_REGEX = re.compile(r"^([A-Za-z]{1}[A-Za-z0-9+.-]*):")

LOG = logging.getLogger(__name__)


def fetch(conf, namespace=BACKEND_NAMESPACE, **kwargs):
    """Fetches a given backend using the given configuration (and any backend
    specific kwargs) in the given entrypoint namespace.
    """
    connection = conf['connection']

    match = SCHEME_REGEX.match(connection)
    if match:
        backend_name = match.group(1)
    else:
        backend_name = connection

    LOG.debug('Looking for %r backend driver in %r', backend_name, namespace)
    try:
        mgr = driver.DriverManager(namespace, backend_name,
                                   invoke_on_load=True,
                                   invoke_args=(conf,),
                                   invoke_kwds=kwargs)
        return mgr.driver
    except RuntimeError as e:
        raise exc.NotFound("Could not find backend %s: %s" % (backend_name, e))


@contextlib.contextmanager
def backend(conf, namespace=BACKEND_NAMESPACE, **kwargs):
    """Fetches a persistence backend, ensures that it is upgraded and upon
    context manager completion closes the backend.
    """
    with contextlib.closing(fetch(conf, namespace=namespace, **kwargs)) as be:
        with contextlib.closing(be.get_connection()) as conn:
            conn.upgrade()
        yield be
