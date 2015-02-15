# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import collections
import os

import six


class FrozenNode(Exception):
    """Exception raised when a frozen node is modified."""

    def __init__(self):
        super(FrozenNode, self).__init__("Frozen node(s) can't be modified")


class _DFSIter(object):
    """Depth first iterator (non-recursive) over the child nodes."""

    def __init__(self, root, include_self=False):
        self.root = root
        self.include_self = bool(include_self)

    def __iter__(self):
        stack = []
        if self.include_self:
            stack.append(self.root)
        else:
            stack.extend(self.root.reverse_iter())
        while stack:
            node = stack.pop()
            # Visit the node.
            yield node
            # Traverse the left & right subtree.
            stack.extend(node.reverse_iter())


class _BFSIter(object):
    """Breadth first iterator (non-recursive) over the child nodes."""

    def __init__(self, root, include_self=False):
        self.root = root
        self.include_self = bool(include_self)

    def __iter__(self):
        q = collections.deque()
        if self.include_self:
            q.append(self.root)
        else:
            q.extend(self.root.reverse_iter())
        while q:
            node = q.popleft()
            # Visit the node.
            yield node
            # Traverse the left & right subtree.
            q.extend(node.reverse_iter())


class Node(object):
    """A n-ary node class that can be used to create tree structures."""

    # Constants used when pretty formatting the node (and its children).
    STARTING_PREFIX = ""
    EMPTY_SPACE_SEP = " "
    HORIZONTAL_CONN = "__"
    VERTICAL_CONN = "|"
    LINE_SEP = os.linesep

    def __init__(self, item, **kwargs):
        self.item = item
        self.parent = None
        self.metadata = dict(kwargs)
        self.frozen = False
        self._children = []

    def freeze(self):
        if not self.frozen:
            # This will DFS until all children are frozen as well, only
            # after that works do we freeze ourselves (this makes it so
            # that we don't become frozen if a child node fails to perform
            # the freeze operation).
            for n in self:
                n.freeze()
            self.frozen = True

    def add(self, child):
        if self.frozen:
            raise FrozenNode()
        child.parent = self
        self._children.append(child)

    def empty(self):
        """Returns if the node is a leaf node."""
        return self.child_count() == 0

    def path_iter(self, include_self=True):
        """Yields back the path from this node to the root node."""
        if include_self:
            node = self
        else:
            node = self.parent
        while node is not None:
            yield node
            node = node.parent

    def find(self, item):
        """Returns the node for an item if it exists in this node.

        This will search not only this node but also any children nodes and
        finally if nothing is found then None is returned instead of a node
        object.

        :param item: item to lookup.
        :returns: the node for an item if it exists in this node
        """
        for n in self.dfs_iter(include_self=True):
            if n.item == item:
                return n
        return None

    def __contains__(self, item):
        """Returns whether item exists in this node or this nodes children.

        :returns: if the item exists in this node or nodes children,
                  true if the item exists, false otherwise
        :rtype: boolean
        """
        return self.find(item) is not None

    def __getitem__(self, index):
        # NOTE(harlowja): 0 is the right most index, len - 1 is the left most
        return self._children[index]

    def pformat(self):
        """Recursively formats a node into a nice string representation.

        **Example**::

            >>> from taskflow.types import tree
            >>> yahoo = tree.Node("CEO")
            >>> yahoo.add(tree.Node("Infra"))
            >>> yahoo[0].add(tree.Node("Boss"))
            >>> yahoo[0][0].add(tree.Node("Me"))
            >>> yahoo.add(tree.Node("Mobile"))
            >>> yahoo.add(tree.Node("Mail"))
            >>> print(yahoo.pformat())
            CEO
            |__Infra
            |  |__Boss
            |     |__Me
            |__Mobile
            |__Mail
        """
        def _inner_pformat(node, level):
            if level == 0:
                yield six.text_type(node.item)
                prefix = self.STARTING_PREFIX
            else:
                yield self.HORIZONTAL_CONN + six.text_type(node.item)
                prefix = self.EMPTY_SPACE_SEP * len(self.HORIZONTAL_CONN)
            child_count = node.child_count()
            for (i, child) in enumerate(node):
                for (j, text) in enumerate(_inner_pformat(child, level + 1)):
                    if j == 0 or i + 1 < child_count:
                        text = prefix + self.VERTICAL_CONN + text
                    else:
                        text = prefix + self.EMPTY_SPACE_SEP + text
                    yield text
        expected_lines = self.child_count(only_direct=False)
        accumulator = six.StringIO()
        for i, line in enumerate(_inner_pformat(self, 0)):
            accumulator.write(line)
            if i < expected_lines:
                accumulator.write(self.LINE_SEP)
        return accumulator.getvalue()

    def child_count(self, only_direct=True):
        """Returns how many children this node has.

        This can be either only the direct children of this node or inclusive
        of all children nodes of this node (children of children and so-on).

        NOTE(harlowja): it does not account for the current node in this count.
        """
        if not only_direct:
            count = 0
            for _node in self.dfs_iter():
                count += 1
            return count
        return len(self._children)

    def __iter__(self):
        """Iterates over the direct children of this node (right->left)."""
        for c in self._children:
            yield c

    def reverse_iter(self):
        """Iterates over the direct children of this node (left->right)."""
        for c in reversed(self._children):
            yield c

    def index(self, item):
        """Finds the child index of a given item, searches in added order."""
        index_at = None
        for (i, child) in enumerate(self._children):
            if child.item == item:
                index_at = i
                break
        if index_at is None:
            raise ValueError("%s is not contained in any child" % (item))
        return index_at

    def dfs_iter(self, include_self=False):
        """Depth first iteration (non-recursive) over the child nodes."""
        return _DFSIter(self, include_self=include_self)

    def bfs_iter(self, include_self=False):
        """Breadth first iteration (non-recursive) over the child nodes."""
        return _BFSIter(self, include_self=include_self)
