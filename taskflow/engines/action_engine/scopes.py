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


def _extract_atoms(node, idx=-1):
    # Always go left to right, since right to left is the pattern order
    # and we want to go backwards and not forwards through that ordering...
    if idx == -1:
        children_iter = node.reverse_iter()
    else:
        children_iter = reversed(node[0:idx])
    atoms = []
    for child in children_iter:
        if isinstance(child.item, flow_type.Flow):
            atoms.extend(_extract_atoms(child))
        elif isinstance(child.item, atom_type.Atom):
            atoms.append(child.item)
        else:
            raise TypeError(
                "Unknown extraction item '%s' (%s)" % (child.item,
                                                       type(child.item)))
    return atoms


class ScopeWalker(object):
    """Walks through the scopes of a atom using a engines compilation.

    This will walk the visible scopes that are accessible for the given
    atom, which can be used by some external entity in some meaningful way,
    for example to find dependent values...
    """

    def __init__(self, compilation, atom, names_only=False):
        self._node = compilation.hierarchy.find(atom)
        if self._node is None:
            raise ValueError("Unable to find atom '%s' in compilation"
                             " hierarchy" % atom)
        self._atom = atom
        self._graph = compilation.execution_graph
        self._names_only = names_only

    def __iter__(self):
        """Iterates over the visible scopes.

        How this works is the following:

        We find all the possible predecessors of the given atom, this is useful
        since we know they occurred before this atom but it doesn't tell us
        the corresponding scope *level* that each predecessor was created in,
        so we need to find this information.

        For that information we consult the location of the atom ``Y`` in the
        node hierarchy. We lookup in a reverse order the parent ``X`` of ``Y``
        and traverse backwards from the index in the parent where ``Y``
        occurred, all children in ``X`` that we encounter in this backwards
        search (if a child is a flow itself, its atom contents will be
        expanded) will be assumed to be at the same scope. This is then a
        *potential* single scope, to make an *actual* scope we remove the items
        from the *potential* scope that are not predecessors of ``Y`` to form
        the *actual* scope.

        Then for additional scopes we continue up the tree, by finding the
        parent of ``X`` (lets call it ``Z``) and perform the same operation,
        going through the children in a reverse manner from the index in
        parent ``Z`` where ``X`` was located. This forms another *potential*
        scope which we provide back as an *actual* scope after reducing the
        potential set by the predecessors of ``Y``. We then repeat this process
        until we no longer have any parent nodes (aka have reached the top of
        the tree) or we run out of predecessors.
        """
        predecessors = set(self._graph.bfs_predecessors_iter(self._atom))
        last = self._node
        for parent in self._node.path_iter(include_self=False):
            if not predecessors:
                break
            last_idx = parent.index(last.item)
            visible = []
            for a in _extract_atoms(parent, idx=last_idx):
                if a in predecessors:
                    predecessors.remove(a)
                    if not self._names_only:
                        visible.append(a)
                    else:
                        visible.append(a.name)
            if LOG.isEnabledFor(logging.BLATHER):
                if not self._names_only:
                    visible_names = [a.name for a in visible]
                else:
                    visible_names = visible
                LOG.blather("Scope visible to '%s' (limited by parent '%s'"
                            " index < %s) is: %s", self._atom,
                            parent.item.name, last_idx, visible_names)
            yield visible
            last = parent
