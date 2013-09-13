# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

import six
import stevedore.driver

from taskflow.persistence import backends as p_backends
from taskflow.utils import persistence_utils as p_utils


# NOTE(imelnikov): this is the entrypoint namespace, not the module namespace.
ENGINES_NAMESPACE = 'taskflow.engines'


def load(flow, store=None, flow_detail=None, book=None,
         engine_conf=None, backend=None, namespace=ENGINES_NAMESPACE):
    """Load flow into engine

    This function creates and prepares engine to run the
    flow. All that is left is to run the engine with 'run()' method.

    Which engine to load is specified in 'engine_conf' parameter. It
    can be a string that names engine type or a dictionary which holds
    engine type (with 'engine' key) and additional engine-specific
    configuration (for example, executor for multithreaded engine).

    Which storage backend to use is defined by backend parameter. It
    can be backend itself, or a dictionary that is passed to
    taskflow.persistence.backends.fetch to obtain backend.

    :param flow: flow to load
    :param store: dict -- data to put to storage to satisfy flow requirements
    :param flow_detail: FlowDetail that holds state of the flow
    :param book: LogBook to create flow detail in if flow_detail is None
    :param engine_conf: engine type and configuration configuration
    :param backend: storage backend to use or configuration
    :param namespace: driver namespace for stevedore (default is fine
       if you don't know what is it)
    :returns: engine
    """

    if engine_conf is None:
        engine_conf = {'engine': 'default'}

    # NOTE(imelnikov): this allows simpler syntax
    if isinstance(engine_conf, six.string_types):
        engine_conf = {'engine': engine_conf}

    if isinstance(backend, dict):
        backend = p_backends.fetch(backend)

    if flow_detail is None:
        if book is None:
            _lb, flow_detail = p_utils.temporary_flow_detail(backend)
        else:
            flow_detail = p_utils.create_flow_detail(flow, book=book,
                                                     backend=backend)

    mgr = stevedore.driver.DriverManager(
        namespace, engine_conf['engine'],
        invoke_on_load=True,
        invoke_kwds={
            'conf': engine_conf.copy(),
            'flow': flow,
            'flow_detail': flow_detail,
            'backend': backend
        })
    engine = mgr.driver
    if store:
        engine.storage.inject(store)
    return engine


def run(flow, store=None, engine_conf=None, backend=None):
    """Run the flow

    This function load the flow into engine (with 'load' function)
    and runs the engine.

    Which engine to load is specified in 'engine_conf' parameter. It
    can be a string that names engine type or a dictionary which holds
    engine type (with 'engine' key) and additional engine-specific
    configuration (for example, executor for multithreaded engine).

    Which storage backend to use is defined by backend parameter. It
    can be backend itself, or a dictionary that is passed to
    taskflow.persistence.backends.fetch to obtain backend.

    :param flow: flow to load
    :param store: dict -- data to put to storage to satisfy flow requirements
    :param engine_conf: engine type and configuration configuration
    :param backend: storage backend to use or configuration
    :returns: dictionary of all named task results (see Storage.fetch_all)
    """
    engine = load(flow, store=store, engine_conf=engine_conf, backend=backend)
    engine.run()
    return engine.storage.fetch_all()
