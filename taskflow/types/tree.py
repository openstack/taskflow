# -*- coding: utf-8 -*-

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
import itertools
import os

import six

from taskflow.types import graph
from taskflow.utils import iter_utils
from taskflow.utils import misc


class FrozenNode(Exception):
    """Exception raised when a frozen node is modified."""

    def __init__(self):
        super(FrozenNode, self).__init__("Frozen node(s) can't be modified")


class _DFSIter(object):
    """Depth first iterator (non-recursive) over the child nodes."""

    def __init__(self, root, include_self=False, right_to_left=True):
        self.root = root
        self.right_to_left = bool(right_to_left)
        self.include_self = bool(include_self)

    def __iter__(self):
        stack = []
        if self.include_self:
            stack.append(self.root)
        else:
            if self.right_to_left:
                stack.extend(self.root.reverse_iter())
            else:
                # Traverse the left nodes first to the right nodes.
                stack.extend(iter(self.root))
        while stack:
            # Visit the node.
            node = stack.pop()
            yield node
            if self.right_to_left:
                stack.extend(node.reverse_iter())
            else:
                # Traverse the left nodes first to the right nodes.
                stack.extend(iter(node))


class _BFSIter(object):
    """Breadth first iterator (non-recursive) over the child nodes."""

    def __init__(self, root, include_self=False, right_to_left=False):
        self.root = root
        self.right_to_left = bool(right_to_left)
        self.include_self = bool(include_self)

    def __iter__(self):
        q = collections.deque()
        if self.include_self:
            q.append(self.root)
        else:
            if self.right_to_left:
                q.extend(iter(self.root))
            else:
                # Traverse the left nodes first to the right nodes.
                q.extend(self.root.reverse_iter())
        while q:
            # Visit the node.
            node = q.popleft()
            yield node
            if self.right_to_left:
                q.extend(iter(node))
            else:
                # Traverse the left nodes first to the right nodes.
                q.extend(node.reverse_iter())


class Node(object):
    """A n-ary node class that can be used to create tree structures."""

    #: Default string prefix used in :py:meth:`.pformat`.
    STARTING_PREFIX = ""

    #: Default string used to create empty space used in :py:meth:`.pformat`.
    EMPTY_SPACE_SEP = " "

    HORIZONTAL_CONN = "__"
    """
    Default string used to horizontally connect a node to its
    parent (used in :py:meth:`.pformat`.).
    """

    VERTICAL_CONN = "|"
    """
    Default string used to vertically connect a node to its
    parent (used in :py:meth:`.pformat`).
    """

    #: Default line separator used in :py:meth:`.pformat`.
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

    @misc.disallow_when_frozen(FrozenNode)
    def add(self, child):
        """Adds a child to this node (appends to left of existing children).

        NOTE(harlowja): this will also set the childs parent to be this node.
        """
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

    def find_first_match(self, matcher, only_direct=False, include_self=True):
        """Finds the *first* node that matching callback returns true.

        This will search not only this node but also any children nodes (in
        depth first order, from right to left) and finally if nothing is
        matched then ``None`` is returned instead of a node object.

        :param matcher: callback that takes one positional argument (a node)
                        and returns true if it matches desired node or false
                        if not.
        :param only_direct: only look at current node and its
                            direct children (implies that this does not
                            search using depth first).
        :param include_self: include the current node during searching.

        :returns: the node that matched (or ``None``)
        """
        if only_direct:
            if include_self:
                it = itertools.chain([self], self.reverse_iter())
            else:
                it = self.reverse_iter()
        else:
            it = self.dfs_iter(include_self=include_self)
        return iter_utils.find_first_match(it, matcher)

    def find(self, item, only_direct=False, include_self=True):
        """Returns the *first* node for an item if it exists in this node.

        This will search not only this node but also any children nodes (in
        depth first order, from right to left) and finally if nothing is
        matched then ``None`` is returned instead of a node object.

        :param item: item to look for.
        :param only_direct: only look at current node and its
                            direct children (implies that this does not
                            search using depth first).
        :param include_self: include the current node during searching.

        :returns: the node that matched provided item (or ``None``)
        """
        return self.find_first_match(lambda n: n.item == item,
                                     only_direct=only_direct,
                                     include_self=include_self)

    @misc.disallow_when_frozen(FrozenNode)
    def disassociate(self):
        """Removes this node from its parent (if any).

        :returns: occurrences of this node that were removed from its parent.
        """
        occurrences = 0
        if self.parent is not None:
            p = self.parent
            self.parent = None
            # Remove all instances of this node from its parent.
            while True:
                try:
                    p._children.remove(self)
                except ValueError:
                    break
                else:
                    occurrences += 1
        return occurrences

    @misc.disallow_when_frozen(FrozenNode)
    def remove(self, item, only_direct=False, include_self=True):
        """Removes a item from this nodes children.

        This will search not only this node but also any children nodes and
        finally if nothing is found then a value error is raised instead of
        the normally returned *removed* node object.

        :param item: item to lookup.
        :param only_direct: only look at current node and its
                            direct children (implies that this does not
                            search using depth first).
        :param include_self: include the current node during searching.
        """
        node = self.find(item, only_direct=only_direct,
                         include_self=include_self)
        if node is None:
            raise ValueError("Item '%s' not found to remove" % item)
        else:
            node.disassociate()
            return node

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

    def pformat(self, stringify_node=None,
                linesep=LINE_SEP, vertical_conn=VERTICAL_CONN,
                horizontal_conn=HORIZONTAL_CONN, empty_space=EMPTY_SPACE_SEP,
                starting_prefix=STARTING_PREFIX):
        """Formats this node + children into a nice string representation.

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
        if stringify_node is None:
            # Default to making a unicode string out of the nodes item...
            stringify_node = lambda node: six.text_type(node.item)
        expected_lines = self.child_count(only_direct=False) + 1
        buff = six.StringIO()
        conn = vertical_conn + horizontal_conn
        stop_at_parent = self
        for i, node in enumerate(self.dfs_iter(include_self=True), 1):
            prefix = []
            connected_to_parent = False
            last_node = node
            # Walk through *most* of this nodes parents, and form the expected
            # prefix that each parent should require, repeat this until we
            # hit the root node (self) and use that as our nodes prefix
            # string...
            parent_node_it = iter_utils.while_is_not(
                node.path_iter(include_self=True), stop_at_parent)
            for j, parent_node in enumerate(parent_node_it):
                if parent_node is stop_at_parent:
                    if j > 0:
                        if not connected_to_parent:
                            prefix.append(conn)
                            connected_to_parent = True
                        else:
                            # If the node was connected already then it must
                            # have had more than one parent, so we want to put
                            # the right final starting prefix on (which may be
                            # a empty space or another vertical connector)...
                            last_node = self._children[-1]
                            m = last_node.find_first_match(lambda n: n is node,
                                                           include_self=False,
                                                           only_direct=False)
                            if m is not None:
                                prefix.append(empty_space)
                            else:
                                prefix.append(vertical_conn)
                elif parent_node is node:
                    # Skip ourself... (we only include ourself so that
                    # we can use the 'j' variable to determine if the only
                    # node requested is ourself in the first place); used
                    # in the first conditional here...
                    pass
                else:
                    if not connected_to_parent:
                        prefix.append(conn)
                        spaces = len(horizontal_conn)
                        connected_to_parent = True
                    else:
                        # If we have already been connected to our parent
                        # then determine if this current node is the last
                        # node of its parent (and in that case just put
                        # on more spaces), otherwise put a vertical connector
                        # on and less spaces...
                        if parent_node[-1] is not last_node:
                            prefix.append(vertical_conn)
                            spaces = len(horizontal_conn)
                        else:
                            spaces = len(conn)
                    prefix.append(empty_space * spaces)
                last_node = parent_node
            prefix.append(starting_prefix)
            for prefix_piece in reversed(prefix):
                buff.write(prefix_piece)
            buff.write(stringify_node(node))
            if i != expected_lines:
                buff.write(linesep)
        return buff.getvalue()

    def child_count(self, only_direct=True):
        """Returns how many children this node has.

        This can be either only the direct children of this node or inclusive
        of all children nodes of this node (children of children and so-on).

        NOTE(harlowja): it does not account for the current node in this count.
        """
        if not only_direct:
            return iter_utils.count(self.dfs_iter())
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

    def dfs_iter(self, include_self=False, right_to_left=True):
        """Depth first iteration (non-recursive) over the child nodes."""
        return _DFSIter(self,
                        include_self=include_self,
                        right_to_left=right_to_left)

    def bfs_iter(self, include_self=False, right_to_left=False):
        """Breadth first iteration (non-recursive) over the child nodes."""
        return _BFSIter(self,
                        include_self=include_self,
                        right_to_left=right_to_left)

    def to_digraph(self):
        """Converts this node + its children into a ordered directed graph.

        The graph returned will have the same structure as the
        this node and its children (and tree node metadata will be translated
        into graph node metadata).

        :returns: a directed graph
        :rtype: :py:class:`taskflow.types.graph.OrderedDiGraph`
        """
        g = graph.OrderedDiGraph()
        for node in self.bfs_iter(include_self=True, right_to_left=True):
            g.add_node(node.item, **node.metadata)
            if node is not self:
                g.add_edge(node.parent.item, node.item)
        return g
