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
import itertools
import posixpath as pp

import fasteners
import six

from taskflow import exceptions as exc
from taskflow.persistence import path_based
from taskflow.types import tree


class FakeInode(tree.Node):
    """A in-memory filesystem inode-like object."""

    def __init__(self, item, path, value=None):
        super(FakeInode, self).__init__(item, path=path, value=value)


class FakeFilesystem(object):
    """An in-memory filesystem-like structure.

    This filesystem uses posix style paths **only** so users must be careful
    to use the ``posixpath`` module instead of the ``os.path`` one which will
    vary depending on the operating system which the active python is running
    in (the decision to use ``posixpath`` was to avoid the path variations
    which are not relevant in an implementation of a in-memory fake
    filesystem).

    **Not** thread-safe when a single filesystem is mutated at the same
    time by multiple threads. For example having multiple threads call into
    :meth:`~taskflow.persistence.backends.impl_memory.FakeFilesystem.clear`
    at the same time could potentially end badly. It is thread-safe when only
    :meth:`~taskflow.persistence.backends.impl_memory.FakeFilesystem.get`
    or other read-only actions (like calling into
    :meth:`~taskflow.persistence.backends.impl_memory.FakeFilesystem.ls`)
    are occuring at the same time.

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
    >>> fs.get("/a/b/c", 'blob')
    'blob'
    """

    #: Root path of the in-memory filesystem.
    root_path = pp.sep

    @classmethod
    def normpath(cls, path):
        """Return a normalized absolutized version of the pathname path."""
        if not path:
            raise ValueError("This filesystem can only normalize paths"
                             " that are not empty")
        if not path.startswith(cls.root_path):
            raise ValueError("This filesystem can only normalize"
                             " paths that start with %s: '%s' is not"
                             " valid" % (cls.root_path, path))
        return pp.normpath(path)

    #: Split a pathname into a tuple of ``(head, tail)``.
    split = staticmethod(pp.split)

    @staticmethod
    def join(*pieces):
        """Join many path segments together."""
        return pp.sep.join(pieces)

    def __init__(self, deep_copy=True):
        self._root = FakeInode(self.root_path, self.root_path)
        self._reverse_mapping = {
            self.root_path: self._root,
        }
        if deep_copy:
            self._copier = copy.deepcopy
        else:
            self._copier = copy.copy

    def ensure_path(self, path):
        """Ensure the path (and parents) exists."""
        path = self.normpath(path)
        # Ignore the root path as we already checked for that; and it
        # will always exist/can't be removed anyway...
        if path == self._root.item:
            return
        node = self._root
        for piece in self._iter_pieces(path):
            child_node = node.find(piece, only_direct=True,
                                   include_self=False)
            if child_node is None:
                child_node = self._insert_child(node, piece)
            node = child_node

    def _insert_child(self, parent_node, basename, value=None):
        child_path = self.join(parent_node.metadata['path'], basename)
        # This avoids getting '//a/b' (duplicated sep at start)...
        #
        # Which can happen easily if something like the following is given.
        # >>> x = ['/', 'b']
        # >>> pp.sep.join(x)
        # '//b'
        if child_path.startswith(pp.sep * 2):
            child_path = child_path[1:]
        child_node = FakeInode(basename, child_path, value=value)
        parent_node.add(child_node)
        self._reverse_mapping[child_path] = child_node
        return child_node

    def _fetch_node(self, path, normalized=False):
        if not normalized:
            normed_path = self.normpath(path)
        else:
            normed_path = path
        try:
            return self._reverse_mapping[normed_path]
        except KeyError:
            raise exc.NotFound("Path '%s' not found" % path)

    def get(self, path, default=None):
        """Fetch the value of given path (and return default if not found)."""
        try:
            return self._get_item(self.normpath(path))
        except exc.NotFound:
            return default

    def _get_item(self, path, links=None):
        node = self._fetch_node(path, normalized=True)
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

    def _up_to_root_selector(self, root_node, child_node):
        # Build the path from the child to the root and stop at the
        # root, and then form a path string...
        path_pieces = [child_node.item]
        for parent_node in child_node.path_iter(include_self=False):
            if parent_node is root_node:
                break
            path_pieces.append(parent_node.item)
        if len(path_pieces) > 1:
            path_pieces.reverse()
        return self.join(*path_pieces)

    @staticmethod
    def _metadata_path_selector(root_node, child_node):
        return child_node.metadata['path']

    def ls_r(self, path, absolute=False):
        """Return list of all children of the given path (recursively)."""
        node = self._fetch_node(path)
        if absolute:
            selector_func = self._metadata_path_selector
        else:
            selector_func = self._up_to_root_selector
        return [selector_func(node, child_node)
                for child_node in node.bfs_iter()]

    def ls(self, path, absolute=False):
        """Return list of all children of the given path (not recursive)."""
        node = self._fetch_node(path)
        if absolute:
            selector_func = self._metadata_path_selector
        else:
            selector_func = self._up_to_root_selector
        child_node_it = iter(node)
        return [selector_func(node, child_node)
                for child_node in child_node_it]

    def clear(self):
        """Remove all nodes (except the root) from this filesystem."""
        self._reverse_mapping = {
            self.root_path: self._root,
        }
        for node in list(self._root.reverse_iter()):
            node.disassociate()

    def delete(self, path, recursive=False):
        """Deletes a node (optionally its children) from this filesystem."""
        path = self.normpath(path)
        node = self._fetch_node(path, normalized=True)
        if node is self._root and not recursive:
            raise ValueError("Can not delete '%s'" % self._root.item)
        if recursive:
            child_paths = (child.metadata['path'] for child in node.bfs_iter())
        else:
            node_child_count = node.child_count()
            if node_child_count:
                raise ValueError("Can not delete '%s', it has %s children"
                                 % (path, node_child_count))
            child_paths = []
        if node is self._root:
            # Don't drop/pop the root...
            paths = child_paths
            drop_nodes = []
        else:
            paths = itertools.chain([path], child_paths)
            drop_nodes = [node]
        for path in paths:
            self._reverse_mapping.pop(path, None)
        for node in drop_nodes:
            node.disassociate()

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
        self.delete(path, recursive=True)

    @staticmethod
    def _stringify_node(node):
        if 'target' in node.metadata:
            return "%s (link to %s)" % (node.item, node.metadata['target'])
        else:
            return six.text_type(node.item)

    def pformat(self):
        """Pretty format this in-memory filesystem."""
        return self._root.pformat(stringify_node=self._stringify_node)

    def symlink(self, src_path, dest_path):
        """Link the destionation path to the source path."""
        dest_path = self.normpath(dest_path)
        src_path = self.normpath(src_path)
        try:
            dest_node = self._fetch_node(dest_path, normalized=True)
        except exc.NotFound:
            parent_path, basename = self.split(dest_path)
            parent_node = self._fetch_node(parent_path, normalized=True)
            dest_node = self._insert_child(parent_node, basename)
        dest_node.metadata['target'] = src_path

    def __getitem__(self, path):
        return self._get_item(self.normpath(path))

    def __setitem__(self, path, value):
        path = self.normpath(path)
        value = self._copier(value)
        try:
            node = self._fetch_node(path, normalized=True)
            node.metadata.update(value=value)
        except exc.NotFound:
            parent_path, basename = self.split(path)
            parent_node = self._fetch_node(parent_path, normalized=True)
            self._insert_child(parent_node, basename, value=value)


class MemoryBackend(path_based.PathBasedBackend):
    """A in-memory (non-persistent) backend.

    This backend writes logbooks, flow details, and atom details to a
    in-memory filesystem-like structure (rooted by the ``memory``
    instance variable).

    This backend does *not* provide true transactional semantics. It does
    guarantee that there will be no inter-thread race conditions when
    writing and reading by using a read/write locks.
    """

    #: Default path used when none is provided.
    DEFAULT_PATH = pp.sep

    def __init__(self, conf=None):
        super(MemoryBackend, self).__init__(conf)
        self.memory = FakeFilesystem(deep_copy=self._conf.get('deep_copy',
                                                              True))
        self.lock = fasteners.ReaderWriterLock()

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
            except exc.TaskFlowException:
                raise
            except Exception:
                exc.raise_with_cause(exc.StorageFailure,
                                     "Storage backend internal error")

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
