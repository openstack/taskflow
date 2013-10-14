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

from taskflow.openstack.common import importutils
from taskflow.persistence import backends as p_backends
from taskflow.utils import persistence_utils as p_utils
from taskflow.utils import reflection


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

    :param flow: flow to run
    :param store: dict -- data to put to storage to satisfy flow requirements
    :param engine_conf: engine type and configuration configuration
    :param backend: storage backend to use or configuration
    :returns: dictionary of all named task results (see Storage.fetch_all)
    """
    engine = load(flow, store=store, engine_conf=engine_conf, backend=backend)
    engine.run()
    return engine.storage.fetch_all()


def load_from_factory(flow_factory, factory_args=None, factory_kwargs=None,
                      store=None, book=None, engine_conf=None, backend=None):
    """Load flow from factory function into engine

    Gets flow factory function (or name of it) and creates flow with
    it. Then, flow is loaded into engine with load(), and factory
    function fully qualified name is saved to flow metadata so that
    it can be later resumed with resume.

    :param flow_factory: function or string: function that creates the flow
    :param factory_args: list or tuple of factory positional arguments
    :param factory_kwargs: dict of factory keyword arguments
    :param store: dict -- data to put to storage to satisfy flow requirements
    :param book: LogBook to create flow detail in
    :param engine_conf: engine type and configuration configuration
    :param backend: storage backend to use or configuration
    :returns: engine
    """

    if isinstance(flow_factory, six.string_types):
        factory_fun = importutils.import_class(flow_factory)
        factory_name = flow_factory
    else:
        factory_fun = flow_factory
        factory_name = reflection.get_callable_name(flow_factory)
        try:
            reimported = importutils.import_class(factory_name)
            assert reimported == factory_fun
        except (ImportError, AssertionError):
            raise ValueError('Flow factory %r is not reimportable by name %s'
                             % (factory_fun, factory_name))

    args = factory_args or []
    kwargs = factory_kwargs or {}
    flow = factory_fun(*args, **kwargs)
    factory_data = dict(name=factory_name, args=args, kwargs=kwargs)

    if isinstance(backend, dict):
        backend = p_backends.fetch(backend)
    flow_detail = p_utils.create_flow_detail(flow, book=book, backend=backend,
                                             meta={'factory': factory_data})
    return load(flow=flow, flow_detail=flow_detail,
                store=store, book=book,
                engine_conf=engine_conf, backend=backend)


def flow_from_detail(flow_detail):
    """Recreate flow previously loaded with load_form_factory

    Gets flow factory name from metadata, calls it to recreate the flow

    :param flow_detail: FlowDetail that holds state of the flow to load
    """
    try:
        factory_data = flow_detail.meta['factory']
    except (KeyError, AttributeError, TypeError):
        raise ValueError('Cannot reconstruct flow %s %s: '
                         'no factory information saved.'
                         % (flow_detail.name, flow_detail.uuid))

    try:
        factory_fun = importutils.import_class(factory_data['name'])
    except (KeyError, ImportError):
        raise ImportError('Could not import factory for flow %s %s'
                          % (flow_detail.name, flow_detail.uuid))

    args = factory_data.get('args', ())
    kwargs = factory_data.get('kwargs', {})
    return factory_fun(*args, **kwargs)


def load_from_detail(flow_detail, store=None, engine_conf=None, backend=None):
    """Reload flow previously loaded with load_form_factory

    Gets flow factory name from metadata, calls it to recreate the flow
    and loads flow into engine with load().

    :param flow_detail: FlowDetail that holds state of the flow to load
    :param store: dict -- data to put to storage to satisfy flow requirements
    :param engine_conf: engine type and configuration configuration
    :param backend: storage backend to use or configuration
    :returns: engine
    """
    flow = flow_from_detail(flow_detail)
    return load(flow, flow_detail=flow_detail,
                store=store, engine_conf=engine_conf, backend=backend)
