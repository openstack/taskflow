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

import contextlib

from stevedore import driver

from taskflow import exceptions as exc
from taskflow import logging
from taskflow.utils import misc


# NOTE(harlowja): this is the entrypoint namespace, not the module namespace.
BACKEND_NAMESPACE = 'taskflow.jobboards'

LOG = logging.getLogger(__name__)


def fetch(name, conf, namespace=BACKEND_NAMESPACE, **kwargs):
    """Fetch a jobboard backend with the given configuration.

    This fetch method will look for the entrypoint name in the entrypoint
    namespace, and then attempt to instantiate that entrypoint using the
    provided name, configuration and any board specific kwargs.

    NOTE(harlowja): to aid in making it easy to specify configuration and
    options to a board the configuration (which is typical just a dictionary)
    can also be a URI string that identifies the entrypoint name and any
    configuration specific to that board.

    For example, given the following configuration URI::

        zookeeper://<not-used>/?a=b&c=d

    This will look for the entrypoint named 'zookeeper' and will provide
    a configuration object composed of the URI's components, in this case that
    is ``{'a': 'b', 'c': 'd'}`` to the constructor of that board
    instance (also including the name specified).
    """
    board, conf = misc.extract_driver_and_conf(conf, 'board')
    LOG.debug('Looking for %r jobboard driver in %r', board, namespace)
    try:
        mgr = driver.DriverManager(namespace, board,
                                   invoke_on_load=True,
                                   invoke_args=(name, conf),
                                   invoke_kwds=kwargs)
        return mgr.driver
    except RuntimeError as e:
        raise exc.NotFound("Could not find jobboard %s" % (board), e)


@contextlib.contextmanager
def backend(name, conf, namespace=BACKEND_NAMESPACE, **kwargs):
    """Fetches a jobboard, connects to it and closes it on completion.

    This allows a board instance to fetched, connected to, and then used in a
    context manager statement with the board being closed upon context
    manager exit.
    """
    jb = fetch(name, conf, namespace=namespace, **kwargs)
    jb.connect()
    with contextlib.closing(jb):
        yield jb
