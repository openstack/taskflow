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
import posixpath as pp

from taskflow import exceptions as exc
from taskflow.persistence import path_based
from taskflow.types import tree
from taskflow.utils import lock_utils


class FakeFilesystem(object):
    """An in-memory filesystem-like structure.

    This filesystem uses posix style paths **only** so users must be careful
    to use the ``posixpath`` module instead of the ``os.path`` one which will
    vary depending on the operating system which the active python is running
    in (the decision to use ``posixpath`` was to avoid the path variations
    which are not relevant in an implementation of a in-memory fake
    filesystem).

    Example usage:

    >>> from taskflow.persistence.backends import impl_memory
    >>> fs = impl_memory.FakeFilesystem()
    >>> fs.ensure_path('/a/b/c')
    >>> fs['/a/b/c'] = 'd'
    >>> print(fs['/a/b/c'])
    d
    >>> del fs['/a/b/c']
    >>> fs.ls("/a/b")
    []
    """

    #: Root path of the in-memory filesystem.
    root_path = pp.sep

    @classmethod
    def _normpath(cls, path):
        if not path.startswith(cls.root_path):
            raise ValueError("This filesystem can only normalize absolute"
                             " paths: '%s' is not valid" % path)
        return pp.normpath(path)

    def __init__(self, deep_copy=True):
        self._root = tree.Node(self.root_path, value=None)
        if deep_copy:
            self._copier = copy.deepcopy
        else:
            self._copier = copy.copy

    def ensure_path(self, path):
        """Ensure the path (and parents) exists."""
        path = self._normpath(path)
        # Ignore the root path as we already checked for that; and it
        # will always exist/can't be removed anyway...
        if path == self._root.item:
            return
        node = self._root
        for piece in self._iter_pieces(path):
            child_node = node.find(piece, only_direct=True,
                                   include_self=False)
            if child_node is None:
                child_node = tree.Node(piece, value=None)
                node.add(child_node)
            node = child_node

    def _fetch_node(self, path):
        node = self._root
        path = self._normpath(path)
        if path == self._root.item:
            return node
        for piece in self._iter_pieces(path):
            node = node.find(piece, only_direct=True,
                             include_self=False)
            if node is None:
                raise exc.NotFound("Path '%s' not found" % path)
        return node

    def _get_item(self, path, links=None):
        node = self._fetch_node(path)
        if 'target' in node.metadata:
            # Follow the link (and watch out for loops)...
            path = node.metadata['target']
            if links is None:
                links = []
            if path in links:
                raise ValueError("Recursive link following not"
                                 " allowed (loop %s detected)"
                                 % (links + [path]))
            else:
                links.append(path)
            return self._get_item(path, links=links)
        else:
            return self._copier(node.metadata['value'])

    def ls(self, path):
        """Return list of all children of the given path."""
        return [node.item for node in self._fetch_node(path)]

    def _iter_pieces(self, path, include_root=False):
        if path == self._root.item:
            # Check for this directly as the following doesn't work with
            # split correctly:
            #
            # >>> path = "/"
            # path.split(pp.sep)
            # ['', '']
            parts = []
        else:
            parts = path.split(pp.sep)[1:]
        if include_root:
            parts.insert(0, self._root.item)
        for piece in parts:
            yield piece

    def __delitem__(self, path):
        node = self._fetch_node(path)
        if node is self._root:
            raise ValueError("Can not delete '%s'" % self._root.item)
        node.disassociate()

    def pformat(self):
        """Pretty format this in-memory filesystem."""
        return self._root.pformat()

    def symlink(self, src_path, dest_path):
        """Link the destionation path to the source path."""
        dest_path = self._normpath(dest_path)
        src_path = self._normpath(src_path)
        dirname, basename = pp.split(dest_path)
        parent_node = self._fetch_node(dirname)
        child_node = parent_node.find(basename,
                                      only_direct=True,
                                      include_self=False)
        if child_node is None:
            child_node = tree.Node(basename, value=None)
            parent_node.add(child_node)
        child_node.metadata['target'] = src_path

    def __getitem__(self, path):
        return self._get_item(path)

    def __setitem__(self, path, value):
        path = self._normpath(path)
        value = self._copier(value)
        try:
            item_node = self._fetch_node(path)
            item_node.metadata.update(value=value)
        except exc.NotFound:
            dirname, basename = pp.split(path)
            parent_node = self._fetch_node(dirname)
            parent_node.add(tree.Node(basename, value=value))


class MemoryBackend(path_based.PathBasedBackend):
    """A in-memory (non-persistent) backend.

    This backend writes logbooks, flow details, and atom details to a
    in-memory filesystem-like structure (rooted by the ``memory``
    instance variable).

    This backend does *not* provide true transactional semantics. It does
    guarantee that there will be no inter-thread race conditions when
    writing and reading by using a read/write locks.
    """
    def __init__(self, conf=None):
        super(MemoryBackend, self).__init__(conf)
        if self._path is None:
            self._path = pp.sep
        self.memory = FakeFilesystem(deep_copy=self._conf.get('deep_copy',
                                                              True))
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

    def _join_path(self, *parts):
        return pp.join(*parts)

    def _get_item(self, path):
        with self._memory_lock():
            return self.backend.memory[path]

    def _set_item(self, path, value, transaction):
        self.backend.memory[path] = value

    def _del_tree(self, path, transaction):
        del self.backend.memory[path]

    def _get_children(self, path):
        with self._memory_lock():
            return self.backend.memory.ls(path)

    def _ensure_path(self, path):
        with self._memory_lock(write=True):
            self.backend.memory.ensure_path(path)

    def _create_link(self, src_path, dest_path, transaction):
        self.backend.memory.symlink(src_path, dest_path)

    @contextlib.contextmanager
    def _transaction(self):
        """This just wraps a global write-lock."""
        with self._memory_lock(write=True):
            yield

    def validate(self):
        pass
