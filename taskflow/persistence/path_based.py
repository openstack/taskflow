# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Rackspace Hosting All Rights Reserved.
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

import abc

from taskflow import exceptions as exc
from taskflow.persistence import base
from taskflow.persistence import models


class PathBasedBackend(base.Backend, metaclass=abc.ABCMeta):
    """Base class for persistence backends that address data by path

    Subclasses of this backend write logbooks, flow details, and atom details
    to a provided base path in some filesystem-like storage. They will create
    and store those objects in three key directories (one for logbooks, one
    for flow details and one for atom details). They create those associated
    directories and then create files inside those directories that represent
    the contents of those objects for later reading and writing.
    """

    #: Default path used when none is provided.
    DEFAULT_PATH = None

    def __init__(self, conf):
        super(PathBasedBackend, self).__init__(conf)
        self._path = self._conf.get('path', None)
        if not self._path:
            self._path = self.DEFAULT_PATH

    @property
    def path(self):
        return self._path


class PathBasedConnection(base.Connection, metaclass=abc.ABCMeta):
    """Base class for path based backend connections."""

    def __init__(self, backend):
        self._backend = backend
        self._book_path = self._join_path(backend.path, "books")
        self._flow_path = self._join_path(backend.path, "flow_details")
        self._atom_path = self._join_path(backend.path, "atom_details")

    @staticmethod
    def _serialize(obj):
        if isinstance(obj, models.LogBook):
            return obj.to_dict(marshal_time=True)
        elif isinstance(obj, models.FlowDetail):
            return obj.to_dict()
        elif isinstance(obj, models.AtomDetail):
            return base._format_atom(obj)
        else:
            raise exc.StorageFailure("Invalid storage class %s" % type(obj))

    @staticmethod
    def _deserialize(cls, data):
        if issubclass(cls, models.LogBook):
            return cls.from_dict(data, unmarshal_time=True)
        elif issubclass(cls, models.FlowDetail):
            return cls.from_dict(data)
        elif issubclass(cls, models.AtomDetail):
            atom_class = models.atom_detail_class(data['type'])
            return atom_class.from_dict(data['atom'])
        else:
            raise exc.StorageFailure("Invalid storage class %s" % cls)

    @property
    def backend(self):
        return self._backend

    @property
    def book_path(self):
        return self._book_path

    @property
    def flow_path(self):
        return self._flow_path

    @property
    def atom_path(self):
        return self._atom_path

    @abc.abstractmethod
    def _join_path(self, *parts):
        """Accept path parts, and return a joined path"""

    @abc.abstractmethod
    def _get_item(self, path):
        """Fetch a single item from the backend"""

    @abc.abstractmethod
    def _set_item(self, path, value, transaction):
        """Write a single item to the backend"""

    @abc.abstractmethod
    def _del_tree(self, path, transaction):
        """Recursively deletes a folder from the backend."""

    @abc.abstractmethod
    def _get_children(self, path):
        """Get a list of child items of a path"""

    @abc.abstractmethod
    def _ensure_path(self, path):
        """Recursively ensure that a path (folder) in the backend exists"""

    @abc.abstractmethod
    def _create_link(self, src_path, dest_path, transaction):
        """Create a symlink-like link between two paths"""

    @abc.abstractmethod
    def _transaction(self):
        """Context manager that yields a transaction"""

    def _get_obj_path(self, obj):
        if isinstance(obj, models.LogBook):
            path = self.book_path
        elif isinstance(obj, models.FlowDetail):
            path = self.flow_path
        elif isinstance(obj, models.AtomDetail):
            path = self.atom_path
        else:
            raise exc.StorageFailure("Invalid storage class %s" % type(obj))
        return self._join_path(path, obj.uuid)

    def _update_object(self, obj, transaction, ignore_missing=False):
        path = self._get_obj_path(obj)
        try:
            item_data = self._get_item(path)
            existing_obj = self._deserialize(type(obj), item_data)
            obj = existing_obj.merge(obj)
        except exc.NotFound:
            if not ignore_missing:
                raise
        self._set_item(path, self._serialize(obj), transaction)
        return obj

    def get_logbooks(self, lazy=False):
        for book_uuid in self._get_children(self.book_path):
            yield self.get_logbook(book_uuid, lazy=lazy)

    def get_logbook(self, book_uuid, lazy=False):
        book_path = self._join_path(self.book_path, book_uuid)
        book_data = self._get_item(book_path)
        book = self._deserialize(models.LogBook, book_data)
        if not lazy:
            for flow_details in self.get_flows_for_book(book_uuid):
                book.add(flow_details)
        return book

    def save_logbook(self, book):
        book_path = self._get_obj_path(book)
        with self._transaction() as transaction:
            self._update_object(book, transaction, ignore_missing=True)
            for flow_details in book:
                flow_path = self._get_obj_path(flow_details)
                link_path = self._join_path(book_path, flow_details.uuid)
                self._do_update_flow_details(flow_details, transaction,
                                             ignore_missing=True)
                self._create_link(flow_path, link_path, transaction)
        return book

    def get_flows_for_book(self, book_uuid, lazy=False):
        book_path = self._join_path(self.book_path, book_uuid)
        for flow_uuid in self._get_children(book_path):
            yield self.get_flow_details(flow_uuid, lazy)

    def get_flow_details(self, flow_uuid, lazy=False):
        flow_path = self._join_path(self.flow_path, flow_uuid)
        flow_data = self._get_item(flow_path)
        flow_details = self._deserialize(models.FlowDetail, flow_data)
        if not lazy:
            for atom_details in self.get_atoms_for_flow(flow_uuid):
                flow_details.add(atom_details)
        return flow_details

    def _do_update_flow_details(self, flow_detail, transaction,
                                ignore_missing=False):
        flow_path = self._get_obj_path(flow_detail)
        self._update_object(flow_detail, transaction,
                            ignore_missing=ignore_missing)
        for atom_details in flow_detail:
            atom_path = self._get_obj_path(atom_details)
            link_path = self._join_path(flow_path, atom_details.uuid)
            self._create_link(atom_path, link_path, transaction)
            self._update_object(atom_details, transaction, ignore_missing=True)
        return flow_detail

    def update_flow_details(self, flow_detail, ignore_missing=False):
        with self._transaction() as transaction:
            return self._do_update_flow_details(flow_detail, transaction,
                                                ignore_missing=ignore_missing)

    def get_atoms_for_flow(self, flow_uuid):
        flow_path = self._join_path(self.flow_path, flow_uuid)
        for atom_uuid in self._get_children(flow_path):
            yield self.get_atom_details(atom_uuid)

    def get_atom_details(self, atom_uuid):
        atom_path = self._join_path(self.atom_path, atom_uuid)
        atom_data = self._get_item(atom_path)
        return self._deserialize(models.AtomDetail, atom_data)

    def update_atom_details(self, atom_detail, ignore_missing=False):
        with self._transaction() as transaction:
            return self._update_object(atom_detail, transaction,
                                       ignore_missing=ignore_missing)

    def _do_destroy_logbook(self, book_uuid, transaction):
        book_path = self._join_path(self.book_path, book_uuid)
        for flow_uuid in self._get_children(book_path):
            flow_path = self._join_path(self.flow_path, flow_uuid)
            for atom_uuid in self._get_children(flow_path):
                atom_path = self._join_path(self.atom_path, atom_uuid)
                self._del_tree(atom_path, transaction)
            self._del_tree(flow_path, transaction)
        self._del_tree(book_path, transaction)

    def destroy_logbook(self, book_uuid):
        with self._transaction() as transaction:
            return self._do_destroy_logbook(book_uuid, transaction)

    def clear_all(self):
        with self._transaction() as transaction:
            for path in (self.book_path, self.flow_path, self.atom_path):
                self._del_tree(path, transaction)

    def upgrade(self):
        for path in (self.book_path, self.flow_path, self.atom_path):
            self._ensure_path(path)

    def close(self):
        pass
