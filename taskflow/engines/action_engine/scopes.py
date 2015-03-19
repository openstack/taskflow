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

from taskflow import atom as atom_type
from taskflow import flow as flow_type
from taskflow import logging

LOG = logging.getLogger(__name__)


def _extract_atoms_iter(node, idx=-1):
    # Always go left to right, since right to left is the pattern order
    # and we want to go backwards and not forwards through that ordering...
    if idx == -1:
        children_iter = node.reverse_iter()
    else:
        children_iter = reversed(node[0:idx])
    for child in children_iter:
        if isinstance(child.item, flow_type.Flow):
            for atom in _extract_atoms_iter(child):
                yield atom
        elif isinstance(child.item, atom_type.Atom):
            yield child.item
        else:
            raise TypeError(
                "Unknown extraction item '%s' (%s)" % (child.item,
                                                       type(child.item)))


class ScopeWalker(object):
    """Walks through the scopes of a atom using a engines compilation.

    NOTE(harlowja): for internal usage only.

    This will walk the visible scopes that are accessible for the given
    atom, which can be used by some external entity in some meaningful way,
    for example to find dependent values...
    """

    def __init__(self, compilation, atom, names_only=False):
        self._node = compilation.hierarchy.find(atom)
        if self._node is None:
            raise ValueError("Unable to find atom '%s' in compilation"
                             " hierarchy" % atom)
        self._level_cache = {}
        self._atom = atom
        self._graph = compilation.execution_graph
        self._names_only = names_only
        self._predecessors = None

    #: Function that extracts the *associated* atoms of a given tree node.
    _extract_atoms_iter = staticmethod(_extract_atoms_iter)

    def __iter__(self):
        """Iterates over the visible scopes.

        How this works is the following:

        We first grab all the predecessors of the given atom (lets call it
        ``Y``) by using the :py:class:`~.compiler.Compilation` execution
        graph (and doing a reverse breadth-first expansion to gather its
        predecessors), this is useful since we know they *always* will
        exist (and execute) before this atom but it does not tell us the
        corresponding scope *level* (flow, nested flow...) that each
        predecessor was created in, so we need to find this information.

        For that information we consult the location of the atom ``Y`` in the
        :py:class:`~.compiler.Compilation` hierarchy/tree. We lookup in a
        reverse order the parent ``X`` of ``Y`` and traverse backwards from
        the index in the parent where ``Y`` exists to all siblings (and
        children of those siblings) in ``X`` that we encounter in this
        backwards search (if a sibling is a flow itself, its atom(s)
        will be recursively expanded and included). This collection will
        then be assumed to be at the same scope. This is what is called
        a *potential* single scope, to make an *actual* scope we remove the
        items from the *potential* scope that are **not** predecessors
        of ``Y`` to form the *actual* scope which we then yield back.

        Then for additional scopes we continue up the tree, by finding the
        parent of ``X`` (lets call it ``Z``) and perform the same operation,
        going through the children in a reverse manner from the index in
        parent ``Z`` where ``X`` was located. This forms another *potential*
        scope which we provide back as an *actual* scope after reducing the
        potential set to only include predecessors previously gathered. We
        then repeat this process until we no longer have any parent
        nodes (aka we have reached the top of the tree) or we run out of
        predecessors.
        """
        if self._predecessors is None:
            pred_iter = self._graph.bfs_predecessors_iter(self._atom)
            self._predecessors = set(pred_iter)
        predecessors = self._predecessors.copy()
        last = self._node
        for lvl, parent in enumerate(self._node.path_iter(include_self=False)):
            if not predecessors:
                break
            last_idx = parent.index(last.item)
            try:
                visible, removals = self._level_cache[lvl]
                predecessors = predecessors - removals
            except KeyError:
                visible = []
                removals = set()
                for atom in self._extract_atoms_iter(parent, idx=last_idx):
                    if atom in predecessors:
                        predecessors.remove(atom)
                        removals.add(atom)
                        visible.append(atom)
                    if not predecessors:
                        break
                self._level_cache[lvl] = (visible, removals)
                if LOG.isEnabledFor(logging.BLATHER):
                    visible_names = [a.name for a in visible]
                    LOG.blather("Scope visible to '%s' (limited by parent '%s'"
                                " index < %s) is: %s", self._atom,
                                parent.item.name, last_idx, visible_names)
            if self._names_only:
                yield [a.name for a in visible]
            else:
                yield visible
            last = parent
