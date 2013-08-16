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

import threading

from taskflow import exceptions as exc
from taskflow.openstack.common import importutils


_BACKEND_MAPPING = {
    'memory': 'taskflow.persistence.backends.memory.api',
    'sqlalchemy': 'taskflow.persistence.backends.sqlalchemy.api',
    # TODO(harlowja): we likely need to allow more customization here so that
    # its easier for a user of this library to alter the impl to there own
    # choicing, aka, cinder has its own DB, or heat may want to write this
    # information into swift, we need a way to accomodate that.
}
_BACKEND_MAPPING_LOCK = threading.RLock()
_BACKENDS = {}
_BACKEND_LOCK = threading.RLock()


def register(backend, module):
    """Register a new (or override an old) backend type.

    Instead of being restricted to the existing types that are pre-registered
    in taskflow it is useful to allow others to either override those types
    or add new ones (since all backend types can not be predicted ahead of
    time).
    """
    with _BACKEND_MAPPING_LOCK:
        _BACKEND_MAPPING[backend] = str(module)


def fetch(backend):
    """Fetch a backend impl. for a given backend type."""
    with _BACKEND_MAPPING_LOCK:
        if backend not in _BACKEND_MAPPING:
            raise exc.NotFound("Unknown backend %s requested" % (backend))
        mod = _BACKEND_MAPPING.get(backend, backend)
    with _BACKEND_LOCK:
        if mod in _BACKENDS:
            return _BACKENDS[mod]
        backend_mod = importutils.import_module(mod)
        backend_impl = backend_mod.get_backend()
        _BACKENDS[mod] = backend_impl
        return backend_impl
