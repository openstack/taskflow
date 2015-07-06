# -*- coding: utf-8 -*-

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

import contextlib

from oslo_utils import importutils
from oslo_utils import reflection
import six
import stevedore.driver

from taskflow import exceptions as exc
from taskflow import logging
from taskflow.persistence import backends as p_backends
from taskflow.utils import misc
from taskflow.utils import persistence_utils as p_utils

LOG = logging.getLogger(__name__)

# NOTE(imelnikov): this is the entrypoint namespace, not the module namespace.
ENGINES_NAMESPACE = 'taskflow.engines'

# The default entrypoint engine type looked for when it is not provided.
ENGINE_DEFAULT = 'default'


def _extract_engine(engine, **kwargs):
    """Extracts the engine kind and any associated options."""
    kind = engine
    if not kind:
        kind = ENGINE_DEFAULT

    # See if it's a URI and if so, extract any further options...
    options = {}
    try:
        uri = misc.parse_uri(kind)
    except (TypeError, ValueError):
        pass
    else:
        kind = uri.scheme
        options = misc.merge_uri(uri, options.copy())

    # Merge in any leftover **kwargs into the options, this makes it so
    # that the provided **kwargs override any URI/engine specific
    # options.
    options.update(kwargs)
    return (kind, options)


def _fetch_factory(factory_name):
    try:
        return importutils.import_class(factory_name)
    except (ImportError, ValueError) as e:
        raise ImportError("Could not import factory %r: %s"
                          % (factory_name, e))


def _fetch_validate_factory(flow_factory):
    if isinstance(flow_factory, six.string_types):
        factory_fun = _fetch_factory(flow_factory)
        factory_name = flow_factory
    else:
        factory_fun = flow_factory
        factory_name = reflection.get_callable_name(flow_factory)
        try:
            reimported = _fetch_factory(factory_name)
            assert reimported == factory_fun
        except (ImportError, AssertionError):
            raise ValueError('Flow factory %r is not reimportable by name %s'
                             % (factory_fun, factory_name))
    return (factory_name, factory_fun)


def load(flow, store=None, flow_detail=None, book=None,
         backend=None, namespace=ENGINES_NAMESPACE,
         engine=ENGINE_DEFAULT, **kwargs):
    """Load a flow into an engine.

    This function creates and prepares an engine to run the provided flow. All
    that is left after this returns is to run the engine with the
    engines :py:meth:`~taskflow.engines.base.Engine.run` method.

    Which engine to load is specified via the ``engine`` parameter. It
    can be a string that names the engine type to use, or a string that
    is a URI with a scheme that names the engine type to use and further
    options contained in the URI's host, port, and query parameters...

    Which storage backend to use is defined by the backend parameter. It
    can be backend itself, or a dictionary that is passed to
    :py:func:`~taskflow.persistence.backends.fetch` to obtain a
    viable backend.

    :param flow: flow to load
    :param store: dict -- data to put to storage to satisfy flow requirements
    :param flow_detail: FlowDetail that holds the state of the flow (if one is
        not provided then one will be created for you in the provided backend)
    :param book: LogBook to create flow detail in if flow_detail is None
    :param backend: storage backend to use or configuration that defines it
    :param namespace: driver namespace for stevedore (or empty for default)
    :param engine: string engine type or URI string with scheme that contains
                   the engine type and any URI specific components that will
                   become part of the engine options.
    :param kwargs: arbitrary keyword arguments passed as options (merged with
                   any extracted ``engine``), typically used for any engine
                   specific options that do not fit as any of the
                   existing arguments.
    :returns: engine
    """

    kind, options = _extract_engine(engine, **kwargs)

    if isinstance(backend, dict):
        backend = p_backends.fetch(backend)

    if flow_detail is None:
        flow_detail = p_utils.create_flow_detail(flow, book=book,
                                                 backend=backend)

    LOG.debug('Looking for %r engine driver in %r', kind, namespace)
    try:
        mgr = stevedore.driver.DriverManager(
            namespace, kind,
            invoke_on_load=True,
            invoke_args=(flow, flow_detail, backend, options))
        engine = mgr.driver
    except RuntimeError as e:
        raise exc.NotFound("Could not find engine '%s'" % (kind), e)
    else:
        if store:
            engine.storage.inject(store)
        return engine


def run(flow, store=None, flow_detail=None, book=None,
        backend=None, namespace=ENGINES_NAMESPACE,
        engine=ENGINE_DEFAULT, **kwargs):
    """Run the flow.

    This function loads the flow into an engine (with the :func:`load() <load>`
    function) and runs the engine.

    The arguments are interpreted as for :func:`load() <load>`.

    :returns: dictionary of all named
              results (see :py:meth:`~.taskflow.storage.Storage.fetch_all`)
    """
    engine = load(flow, store=store, flow_detail=flow_detail, book=book,
                  backend=backend, namespace=namespace,
                  engine=engine, **kwargs)
    engine.run()
    return engine.storage.fetch_all()


def save_factory_details(flow_detail,
                         flow_factory, factory_args, factory_kwargs,
                         backend=None):
    """Saves the given factories reimportable attributes into the flow detail.

    This function saves the factory name, arguments, and keyword arguments
    into the given flow details object  and if a backend is provided it will
    also ensure that the backend saves the flow details after being updated.

    :param flow_detail: FlowDetail that holds state of the flow to load
    :param flow_factory: function or string: function that creates the flow
    :param factory_args: list or tuple of factory positional arguments
    :param factory_kwargs: dict of factory keyword arguments
    :param backend: storage backend to use or configuration
    """
    if not factory_args:
        factory_args = []
    if not factory_kwargs:
        factory_kwargs = {}
    factory_name, _factory_fun = _fetch_validate_factory(flow_factory)
    factory_data = {
        'factory': {
            'name': factory_name,
            'args': factory_args,
            'kwargs': factory_kwargs,
        },
    }
    if not flow_detail.meta:
        flow_detail.meta = factory_data
    else:
        flow_detail.meta.update(factory_data)
    if backend is not None:
        if isinstance(backend, dict):
            backend = p_backends.fetch(backend)
        with contextlib.closing(backend.get_connection()) as conn:
            conn.update_flow_details(flow_detail)


def load_from_factory(flow_factory, factory_args=None, factory_kwargs=None,
                      store=None, book=None, backend=None,
                      namespace=ENGINES_NAMESPACE, engine=ENGINE_DEFAULT,
                      **kwargs):
    """Loads a flow from a factory function into an engine.

    Gets flow factory function (or name of it) and creates flow with
    it. Then, the flow is loaded into an engine with the :func:`load() <load>`
    function, and the factory function fully qualified name is saved to flow
    metadata so that it can be later resumed.

    :param flow_factory: function or string: function that creates the flow
    :param factory_args: list or tuple of factory positional arguments
    :param factory_kwargs: dict of factory keyword arguments

    Further arguments are interpreted as for :func:`load() <load>`.

    :returns: engine
    """

    _factory_name, factory_fun = _fetch_validate_factory(flow_factory)
    if not factory_args:
        factory_args = []
    if not factory_kwargs:
        factory_kwargs = {}
    flow = factory_fun(*factory_args, **factory_kwargs)
    if isinstance(backend, dict):
        backend = p_backends.fetch(backend)
    flow_detail = p_utils.create_flow_detail(flow, book=book, backend=backend)
    save_factory_details(flow_detail,
                         flow_factory, factory_args, factory_kwargs,
                         backend=backend)
    return load(flow=flow, store=store, flow_detail=flow_detail, book=book,
                backend=backend, namespace=namespace,
                engine=engine, **kwargs)


def flow_from_detail(flow_detail):
    """Reloads a flow previously saved.

    Gets the flow factories name and any arguments and keyword arguments from
    the flow details metadata, and then calls that factory to recreate the
    flow.

    :param flow_detail: FlowDetail that holds state of the flow to load
    """
    try:
        factory_data = flow_detail.meta['factory']
    except (KeyError, AttributeError, TypeError):
        raise ValueError('Cannot reconstruct flow %s %s: '
                         'no factory information saved.'
                         % (flow_detail.name, flow_detail.uuid))

    try:
        factory_fun = _fetch_factory(factory_data['name'])
    except (KeyError, ImportError):
        raise ImportError('Could not import factory for flow %s %s'
                          % (flow_detail.name, flow_detail.uuid))

    args = factory_data.get('args', ())
    kwargs = factory_data.get('kwargs', {})
    return factory_fun(*args, **kwargs)


def load_from_detail(flow_detail, store=None, backend=None,
                     namespace=ENGINES_NAMESPACE, engine=ENGINE_DEFAULT,
                     **kwargs):
    """Reloads an engine previously saved.

    This reloads the flow using the
    :func:`flow_from_detail() <flow_from_detail>` function and then calls
    into the :func:`load() <load>` function to create an engine from that flow.

    :param flow_detail: FlowDetail that holds state of the flow to load

    Further arguments are interpreted as for :func:`load() <load>`.

    :returns: engine
    """
    flow = flow_from_detail(flow_detail)
    return load(flow, flow_detail=flow_detail,
                store=store, backend=backend,
                namespace=namespace, engine=engine, **kwargs)
