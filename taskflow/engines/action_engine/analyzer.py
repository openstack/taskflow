# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

from networkx.algorithms import traversal
import six

from taskflow import retry as retry_atom
from taskflow import states as st


class Analyzer(object):
    """Analyzes a compilation and aids in execution processes.

    Its primary purpose is to get the next atoms for execution or reversion
    by utilizing the compilations underlying structures (graphs, nodes and
    edge relations...) and using this information along with the atom
    state/states stored in storage to provide other useful functionality to
    the rest of the runtime system.
    """

    def __init__(self, runtime):
        self._storage = runtime.storage
        self._execution_graph = runtime.compilation.execution_graph
        self._check_atom_transition = runtime.check_atom_transition

    def get_next_nodes(self, node=None):
        if node is None:
            execute = self.browse_nodes_for_execute()
            revert = self.browse_nodes_for_revert()
            return execute + revert

        state = self.get_state(node)
        intention = self._storage.get_atom_intention(node.name)
        if state == st.SUCCESS:
            if intention == st.REVERT:
                return [node]
            elif intention == st.EXECUTE:
                return self.browse_nodes_for_execute(node)
            else:
                return []
        elif state == st.REVERTED:
            return self.browse_nodes_for_revert(node)
        elif state == st.FAILURE:
            return self.browse_nodes_for_revert()
        else:
            return []

    def browse_nodes_for_execute(self, node=None):
        """Browse next nodes to execute.

        This returns a collection of nodes that are ready to be executed, if
        given a specific node it will only examine the successors of that node,
        otherwise it will examine the whole graph.
        """
        if node:
            nodes = self._execution_graph.successors(node)
        else:
            nodes = self._execution_graph.nodes_iter()

        available_nodes = []
        for node in nodes:
            if self._is_ready_for_execute(node):
                available_nodes.append(node)
        return available_nodes

    def browse_nodes_for_revert(self, node=None):
        """Browse next nodes to revert.

        This returns a collection of nodes that are ready to be be reverted, if
        given a specific node it will only examine the predecessors of that
        node, otherwise it will examine the whole graph.
        """
        if node:
            nodes = self._execution_graph.predecessors(node)
        else:
            nodes = self._execution_graph.nodes_iter()

        available_nodes = []
        for node in nodes:
            if self._is_ready_for_revert(node):
                available_nodes.append(node)
        return available_nodes

    def _is_ready_for_execute(self, atom):
        """Checks if atom is ready to be executed."""
        state = self.get_state(atom)
        intention = self._storage.get_atom_intention(atom.name)
        transition = self._check_atom_transition(atom, state, st.RUNNING)
        if not transition or intention != st.EXECUTE:
            return False

        atom_names = []
        for prev_atom in self._execution_graph.predecessors(atom):
            atom_names.append(prev_atom.name)

        atom_states = self._storage.get_atoms_states(atom_names)
        return all(state == st.SUCCESS and intention == st.EXECUTE
                   for state, intention in six.itervalues(atom_states))

    def _is_ready_for_revert(self, atom):
        """Checks if atom is ready to be reverted."""
        state = self.get_state(atom)
        intention = self._storage.get_atom_intention(atom.name)
        transition = self._check_atom_transition(atom, state, st.REVERTING)
        if not transition or intention not in (st.REVERT, st.RETRY):
            return False

        atom_names = []
        for prev_atom in self._execution_graph.successors(atom):
            atom_names.append(prev_atom.name)

        atom_states = self._storage.get_atoms_states(atom_names)
        return all(state in (st.PENDING, st.REVERTED)
                   for state, intention in six.itervalues(atom_states))

    def iterate_subgraph(self, atom):
        """Iterates a subgraph connected to given atom."""
        for _src, dst in traversal.dfs_edges(self._execution_graph, atom):
            yield dst

    def iterate_retries(self, state=None):
        """Iterates retry controllers that match the provided state.

        If no state is provided it will yield back all retry controllers.
        """
        for node in self._execution_graph.nodes_iter():
            if isinstance(node, retry_atom.Retry):
                if not state or self.get_state(node) == state:
                    yield node

    def iterate_all_nodes(self):
        for node in self._execution_graph.nodes_iter():
            yield node

    def find_atom_retry(self, atom):
        return self._execution_graph.node[atom].get('retry')

    def is_success(self):
        for atom in self.iterate_all_nodes():
            if self.get_state(atom) != st.SUCCESS:
                return False
        return True

    def get_state(self, atom):
        return self._storage.get_atom_state(atom.name)
