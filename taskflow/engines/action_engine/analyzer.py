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

import functools
import itertools

from networkx.algorithms import traversal
import six

from taskflow import states as st


class IgnoreDecider(object):
    """Checks any provided edge-deciders and determines if ok to run."""

    def __init__(self, atom, edge_deciders):
        self._atom = atom
        self._edge_deciders = edge_deciders

    def check(self, runtime):
        """Returns bool of whether this decider should allow running."""
        results = {}
        for name in six.iterkeys(self._edge_deciders):
            results[name] = runtime.storage.get(name)
        for local_decider in six.itervalues(self._edge_deciders):
            if not local_decider(history=results):
                return False
        return True

    def affect(self, runtime):
        """If the :py:func:`~.check` returns false, affects associated atoms.

        This will alter the associated atom + successor atoms by setting there
        state to ``IGNORE`` so that they are ignored in future runtime
        activities.
        """
        successors_iter = runtime.analyzer.iterate_subgraph(self._atom)
        runtime.reset_nodes(itertools.chain([self._atom], successors_iter),
                            state=st.IGNORE, intention=st.IGNORE)

    def check_and_affect(self, runtime):
        """Handles :py:func:`~.check` + :py:func:`~.affect` in right order."""
        proceed = self.check(runtime)
        if not proceed:
            self.affect(runtime)
        return proceed


class NoOpDecider(object):
    """No-op decider that says it is always ok to run & has no effect(s)."""

    def check(self, runtime):
        """Always good to go."""
        return True

    def affect(self, runtime):
        """Does nothing."""

    def check_and_affect(self, runtime):
        """Handles :py:func:`~.check` + :py:func:`~.affect` in right order.

        Does nothing.
        """
        return self.check(runtime)


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
        self._fetch_edge_deciders = runtime.fetch_edge_deciders
        self._fetch_retries = functools.partial(
            runtime.fetch_atoms_by_kind, 'retry')

    def get_next_nodes(self, node=None):
        """Get next nodes to run (originating from node or all nodes)."""
        if node is None:
            execute = self.browse_nodes_for_execute()
            revert = self.browse_nodes_for_revert()
            return execute + revert
        state = self.get_state(node)
        intention = self._storage.get_atom_intention(node.name)
        if state == st.SUCCESS:
            if intention == st.REVERT:
                return [
                    (node, NoOpDecider()),
                ]
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

        This returns a collection of nodes that *may* be ready to be
        executed, if given a specific node it will only examine the successors
        of that node, otherwise it will examine the whole graph.
        """
        if node is not None:
            nodes = self._execution_graph.successors(node)
        else:
            nodes = self._execution_graph.nodes_iter()
        ready_nodes = []
        for node in nodes:
            is_ready, late_decider = self._get_maybe_ready_for_execute(node)
            if is_ready:
                ready_nodes.append((node, late_decider))
        return ready_nodes

    def browse_nodes_for_revert(self, node=None):
        """Browse next nodes to revert.

        This returns a collection of nodes that *may* be ready to be be
        reverted, if given a specific node it will only examine the
        predecessors of that node, otherwise it will examine the whole
        graph.
        """
        if node is not None:
            nodes = self._execution_graph.predecessors(node)
        else:
            nodes = self._execution_graph.nodes_iter()
        ready_nodes = []
        for node in nodes:
            is_ready, late_decider = self._get_maybe_ready_for_revert(node)
            if is_ready:
                ready_nodes.append((node, late_decider))
        return ready_nodes

    def _get_maybe_ready_for_execute(self, atom):
        """Returns if an atom is *likely* ready to be executed."""

        state = self.get_state(atom)
        intention = self._storage.get_atom_intention(atom.name)
        transition = self._check_atom_transition(atom, state, st.RUNNING)
        if not transition or intention != st.EXECUTE:
            return (False, None)

        predecessor_names = []
        for previous_atom in self._execution_graph.predecessors(atom):
            predecessor_names.append(previous_atom.name)

        predecessor_states = self._storage.get_atoms_states(predecessor_names)
        predecessor_states_iter = six.itervalues(predecessor_states)
        ok_to_run = all(state == st.SUCCESS and intention == st.EXECUTE
                        for state, intention in predecessor_states_iter)

        if not ok_to_run:
            return (False, None)
        else:
            edge_deciders = self._fetch_edge_deciders(atom)
            return (True, IgnoreDecider(atom, edge_deciders))

    def _get_maybe_ready_for_revert(self, atom):
        """Returns if an atom is *likely* ready to be reverted."""

        state = self.get_state(atom)
        intention = self._storage.get_atom_intention(atom.name)
        transition = self._check_atom_transition(atom, state, st.REVERTING)
        if not transition or intention not in (st.REVERT, st.RETRY):
            return (False, None)

        predecessor_names = []
        for previous_atom in self._execution_graph.successors(atom):
            predecessor_names.append(previous_atom.name)

        predecessor_states = self._storage.get_atoms_states(predecessor_names)
        predecessor_states_iter = six.itervalues(predecessor_states)
        ok_to_run = all(state in (st.PENDING, st.REVERTED)
                        for state, intention in predecessor_states_iter)

        if not ok_to_run:
            return (False, None)
        else:
            return (True, NoOpDecider())

    def iterate_subgraph(self, atom):
        """Iterates a subgraph connected to given atom."""
        for _src, dst in traversal.dfs_edges(self._execution_graph, atom):
            yield dst

    def iterate_retries(self, state=None):
        """Iterates retry atoms that match the provided state.

        If no state is provided it will yield back all retry atoms.
        """
        for atom in self._fetch_retries():
            if not state or self.get_state(atom) == state:
                yield atom

    def iterate_all_nodes(self):
        """Yields back all nodes in the execution graph."""
        for node in self._execution_graph.nodes_iter():
            yield node

    def find_atom_retry(self, atom):
        """Returns the retry atom associated to the given atom (or none)."""
        return self._execution_graph.node[atom].get('retry')

    def is_success(self):
        """Checks if all nodes in the execution graph are in 'happy' state."""
        for atom in self.iterate_all_nodes():
            atom_state = self.get_state(atom)
            if atom_state == st.IGNORE:
                continue
            if atom_state != st.SUCCESS:
                return False
        return True

    def get_state(self, atom):
        """Gets the state of a given atom (from the backend storage unit)."""
        return self._storage.get_atom_state(atom.name)
