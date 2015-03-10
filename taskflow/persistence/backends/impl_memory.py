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

import contextlib
import copy
import os

from taskflow import exceptions as exc
from taskflow.persistence import path_based
from taskflow.types import tree
from taskflow.utils import lock_utils


class MemoryBackend(path_based.PathBasedBackend):
    """A in-memory (non-persistent) backend.

    This backend writes logbooks, flow details, and atom details to in-memory
    dictionaries and retrieves from those dictionaries as needed.

    This backend does *not* provide true transactional semantics. It does
    guarantee that there will be no inter-thread race conditions when
    writing and reading by using a read/write locks.
    """
    def __init__(self, conf=None):
        super(MemoryBackend, self).__init__(conf)
        if self._path is None:
            self._path = os.sep
        self.memory = tree.Node(self._path)
        self.lock = lock_utils.ReaderWriterLock()

    def get_connection(self):
        return Connection(self)

    def close(self):
        pass


class Connection(path_based.PathBasedConnection):
    def __init__(self, backend):
        super(Connection, self).__init__(backend)
        self.upgrade()

    @contextlib.contextmanager
    def _memory_lock(self, write=False):
        if write:
            lock = self.backend.lock.write_lock
        else:
            lock = self.backend.lock.read_lock

        with lock():
            try:
                yield
            except exc.TaskFlowException as e:
                raise
            except Exception as e:
                raise exc.StorageFailure("Storage backend internal error", e)

    def _fetch_node(self, path):
        node = self.backend.memory.find(path)
        if node is None:
            raise exc.NotFound("Item not found %s" % path)
        return node

    def _join_path(self, *parts):
        return os.path.join(*parts)

    def _get_item(self, path):
        with self._memory_lock():
            return copy.deepcopy(self._fetch_node(path).metadata['value'])

    def _set_item(self, path, value, transaction):
        value = copy.deepcopy(value)
        try:
            item_node = self._fetch_node(path)
            item_node.metadata.update(value=value)
        except exc.NotFound:
            dirname, basename = os.path.split(path)
            parent_node = self._fetch_node(dirname)
            parent_node.add(tree.Node(path, name=basename, value=value))

    def _del_tree(self, path, transaction):
        node = self._fetch_node(path)
        node.disassociate()

    def _get_children(self, path):
        with self._memory_lock():
            return [node.metadata['name'] for node in self._fetch_node(path)]

    def _ensure_path(self, path):
        with self._memory_lock(write=True):
            path = os.path.normpath(path)
            parts = path.split(os.sep)
            node = self.backend.memory
            for p in range(len(parts) - 1):
                node_path = os.sep.join(parts[:p + 2])
                try:
                    node = self._fetch_node(node_path)
                except exc.NotFound:
                    node.add(tree.Node(node_path, name=parts[p + 1]))

    def _create_link(self, src_path, dest_path, transaction):
        dirname, basename = os.path.split(dest_path)
        parent_node = self._fetch_node(dirname)
        parent_node.add(tree.Node(dest_path, name=basename, target=src_path))

    @contextlib.contextmanager
    def _transaction(self):
        """This just wraps a global write-lock"""
        with self._memory_lock(write=True):
            yield

    def validate(self):
        pass
