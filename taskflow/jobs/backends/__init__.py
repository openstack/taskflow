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
import logging

import six
from stevedore import driver

from taskflow import exceptions as exc
from taskflow.utils import misc


# NOTE(harlowja): this is the entrypoint namespace, not the module namespace.
BACKEND_NAMESPACE = 'taskflow.jobboards'

LOG = logging.getLogger(__name__)


def fetch(name, conf, namespace=BACKEND_NAMESPACE, **kwargs):
    """Fetch a jobboard backend with the given configuration (and any board
    specific kwargs) in the given entrypoint namespace and create it with the
    given name.
    """
    if isinstance(conf, six.string_types):
        conf = {'board': conf}
    board = conf['board']
    try:
        pieces = misc.parse_uri(board)
    except (TypeError, ValueError):
        pass
    else:
        board = pieces['scheme']
        conf = misc.merge_uri(pieces, conf.copy())
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
    """Fetches a jobboard backend, connects to it and allows it to be used in
    a context manager statement with the jobboard being closed upon completion.
    """
    jb = fetch(name, conf, namespace=namespace, **kwargs)
    jb.connect()
    with contextlib.closing(jb):
        yield jb
