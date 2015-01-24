# -*- coding: utf-8 -*-

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

import functools

import six

from taskflow import exceptions as exc
from taskflow import logging
from taskflow.persistence import base
from taskflow.persistence import logbook
from taskflow.utils import lock_utils

LOG = logging.getLogger(__name__)


class _Memory(object):
    """Where the data is really stored."""

    def __init__(self):
        self.log_books = {}
        self.flow_details = {}
        self.atom_details = {}

    def clear_all(self):
        self.log_books.clear()
        self.flow_details.clear()
        self.atom_details.clear()


class _MemoryHelper(object):
    """Helper functionality for the memory backends & connections."""

    def __init__(self, memory):
        self._memory = memory

    @staticmethod
    def _fetch_clone_args(incoming):
        if isinstance(incoming, (logbook.LogBook, logbook.FlowDetail)):
            # We keep our own copy of the added contents of the following
            # types so we don't need the clone to retain them directly...
            return {
                'retain_contents': False,
            }
        return {}

    def construct(self, uuid, container):
        """Reconstructs a object from the given uuid and storage container."""
        source = container[uuid]
        clone_kwargs = self._fetch_clone_args(source)
        clone = source['object'].copy(**clone_kwargs)
        rebuilder = source.get('rebuilder')
        if rebuilder:
            for component in map(rebuilder, source['components']):
                clone.add(component)
        return clone

    def merge(self, incoming, saved_info=None):
        """Merges the incoming object into the local memories copy."""
        if saved_info is None:
            if isinstance(incoming, logbook.LogBook):
                saved_info = self._memory.log_books.setdefault(
                    incoming.uuid, {})
            elif isinstance(incoming, logbook.FlowDetail):
                saved_info = self._memory.flow_details.setdefault(
                    incoming.uuid, {})
            elif isinstance(incoming, logbook.AtomDetail):
                saved_info = self._memory.atom_details.setdefault(
                    incoming.uuid, {})
            else:
                raise TypeError("Unknown how to merge '%s' (%s)"
                                % (incoming, type(incoming)))
        try:
            saved_info['object'].merge(incoming)
        except KeyError:
            clone_kwargs = self._fetch_clone_args(incoming)
            saved_info['object'] = incoming.copy(**clone_kwargs)
        if isinstance(incoming, logbook.LogBook):
            flow_details = saved_info.setdefault('components', set())
            if 'rebuilder' not in saved_info:
                saved_info['rebuilder'] = functools.partial(
                    self.construct, container=self._memory.flow_details)
            for flow_detail in incoming:
                flow_details.add(self.merge(flow_detail))
        elif isinstance(incoming, logbook.FlowDetail):
            atom_details = saved_info.setdefault('components', set())
            if 'rebuilder' not in saved_info:
                saved_info['rebuilder'] = functools.partial(
                    self.construct, container=self._memory.atom_details)
            for atom_detail in incoming:
                atom_details.add(self.merge(atom_detail))
        return incoming.uuid


class MemoryBackend(base.Backend):
    """A in-memory (non-persistent) backend.

    This backend writes logbooks, flow details, and atom details to in-memory
    dictionaries and retrieves from those dictionaries as needed.
    """
    def __init__(self, conf=None):
        super(MemoryBackend, self).__init__(conf)
        self._memory = _Memory()
        self._helper = _MemoryHelper(self._memory)
        self._lock = lock_utils.ReaderWriterLock()

    def _construct_from(self, container):
        return dict((uuid, self._helper.construct(uuid, container))
                    for uuid in six.iterkeys(container))

    @property
    def log_books(self):
        with self._lock.read_lock():
            return self._construct_from(self._memory.log_books)

    @property
    def flow_details(self):
        with self._lock.read_lock():
            return self._construct_from(self._memory.flow_details)

    @property
    def atom_details(self):
        with self._lock.read_lock():
            return self._construct_from(self._memory.atom_details)

    def get_connection(self):
        return Connection(self)

    def close(self):
        pass


class Connection(base.Connection):
    """A connection to an in-memory backend."""

    def __init__(self, backend):
        self._backend = backend
        self._helper = backend._helper
        self._memory = backend._memory
        self._lock = backend._lock

    def upgrade(self):
        pass

    def validate(self):
        pass

    @property
    def backend(self):
        return self._backend

    def close(self):
        pass

    def clear_all(self):
        with self._lock.write_lock():
            self._memory.clear_all()

    def destroy_logbook(self, book_uuid):
        with self._lock.write_lock():
            try:
                # Do the same cascading delete that the sql layer does.
                book_info = self._memory.log_books.pop(book_uuid)
            except KeyError:
                raise exc.NotFound("No logbook found with uuid '%s'"
                                   % book_uuid)
            else:
                while book_info['components']:
                    flow_uuid = book_info['components'].pop()
                    flow_info = self._memory.flow_details.pop(flow_uuid)
                    while flow_info['components']:
                        atom_uuid = flow_info['components'].pop()
                        self._memory.atom_details.pop(atom_uuid)

    def update_atom_details(self, atom_detail):
        with self._lock.write_lock():
            try:
                atom_info = self._memory.atom_details[atom_detail.uuid]
                return self._helper.construct(
                    self._helper.merge(atom_detail, saved_info=atom_info),
                    self._memory.atom_details)
            except KeyError:
                raise exc.NotFound("No atom details found with uuid '%s'"
                                   % atom_detail.uuid)

    def update_flow_details(self, flow_detail):
        with self._lock.write_lock():
            try:
                flow_info = self._memory.flow_details[flow_detail.uuid]
                return self._helper.construct(
                    self._helper.merge(flow_detail, saved_info=flow_info),
                    self._memory.flow_details)
            except KeyError:
                raise exc.NotFound("No flow details found with uuid '%s'"
                                   % flow_detail.uuid)

    def save_logbook(self, book):
        with self._lock.write_lock():
            return self._helper.construct(self._helper.merge(book),
                                          self._memory.log_books)

    def get_logbook(self, book_uuid):
        with self._lock.read_lock():
            try:
                return self._helper.construct(book_uuid,
                                              self._memory.log_books)
            except KeyError:
                raise exc.NotFound("No logbook found with uuid '%s'"
                                   % book_uuid)

    def get_logbooks(self):
        # Don't hold locks while iterating...
        with self._lock.read_lock():
            book_uuids = set(six.iterkeys(self._memory.log_books))
        for book_uuid in book_uuids:
            try:
                with self._lock.read_lock():
                    book = self._helper.construct(book_uuid,
                                                  self._memory.log_books)
                yield book
            except KeyError:
                pass
