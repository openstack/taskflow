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

import abc
import itertools
import weakref

import six

from taskflow.engines.action_engine import compiler as co
from taskflow import states as st
from taskflow.utils import iter_utils


def _depth_first_iterate(graph, connected_to_functors, initial_nodes_iter):
    """Iterates connected nodes in execution graph (from starting set).

    Jumps over nodes with ``noop`` attribute (does not yield them back).
    """
    stack = list(initial_nodes_iter)
    while stack:
        node = stack.pop()
        node_attrs = graph.node[node]
        if not node_attrs.get('noop'):
            yield node
        try:
            node_kind = node_attrs['kind']
            connected_to_functor = connected_to_functors[node_kind]
        except KeyError:
            pass
        else:
            stack.extend(connected_to_functor(node))


@six.add_metaclass(abc.ABCMeta)
class Decider(object):
    """Base class for deciders.

    Provides interface to be implemented by sub-classes
    Decider checks whether next atom in flow should be executed or not
    """

    @abc.abstractmethod
    def check(self, runtime):
        """Returns bool of whether this decider should allow running."""

    @abc.abstractmethod
    def affect(self, runtime):
        """If the :py:func:`~.check` returns false, affects associated atoms.

        """

    def check_and_affect(self, runtime):
        """Handles :py:func:`~.check` + :py:func:`~.affect` in right order."""
        proceed = self.check(runtime)
        if not proceed:
            self.affect(runtime)
        return proceed


class IgnoreDecider(Decider):
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
        successors_iter = runtime.analyzer.iterate_connected_atoms(self._atom)
        runtime.reset_atoms(itertools.chain([self._atom], successors_iter),
                            state=st.IGNORE, intention=st.IGNORE)


class NoOpDecider(Decider):
    """No-op decider that says it is always ok to run & has no effect(s)."""

    def check(self, runtime):
        """Always good to go."""
        return True

    def affect(self, runtime):
        """Does nothing."""


class Analyzer(object):
    """Analyzes a compilation and aids in execution processes.

    Its primary purpose is to get the next atoms for execution or reversion
    by utilizing the compilations underlying structures (graphs, nodes and
    edge relations...) and using this information along with the atom
    state/states stored in storage to provide other useful functionality to
    the rest of the runtime system.
    """

    def __init__(self, runtime):
        self._runtime = weakref.proxy(runtime)
        self._storage = runtime.storage
        self._execution_graph = runtime.compilation.execution_graph

    def iter_next_atoms(self, atom=None):
        """Iterate next atoms to run (originating from atom or all atoms)."""
        if atom is None:
            return iter_utils.unique_seen(self.browse_atoms_for_execute(),
                                          self.browse_atoms_for_revert())
        state = self.get_state(atom)
        intention = self._storage.get_atom_intention(atom.name)
        if state == st.SUCCESS:
            if intention == st.REVERT:
                return iter([
                    (atom, NoOpDecider()),
                ])
            elif intention == st.EXECUTE:
                return self.browse_atoms_for_execute(atom=atom)
            else:
                return iter([])
        elif state == st.REVERTED:
            return self.browse_atoms_for_revert(atom=atom)
        elif state == st.FAILURE:
            return self.browse_atoms_for_revert()
        else:
            return iter([])

    def browse_atoms_for_execute(self, atom=None):
        """Browse next atoms to execute.

        This returns a iterator of atoms that *may* be ready to be
        executed, if given a specific atom, it will only examine the successors
        of that atom, otherwise it will examine the whole graph.
        """
        if atom is None:
            atom_it = self.iterate_nodes(co.ATOMS)
        else:
            successors_iter = self._execution_graph.successors_iter
            atom_it = _depth_first_iterate(self._execution_graph,
                                           {co.FLOW: successors_iter},
                                           successors_iter(atom))
        for atom in atom_it:
            is_ready, late_decider = self._get_maybe_ready_for_execute(atom)
            if is_ready:
                yield (atom, late_decider)

    def browse_atoms_for_revert(self, atom=None):
        """Browse next atoms to revert.

        This returns a iterator of atoms that *may* be ready to be be
        reverted, if given a specific atom it will only examine the
        predecessors of that atom, otherwise it will examine the whole
        graph.
        """
        if atom is None:
            atom_it = self.iterate_nodes(co.ATOMS)
        else:
            predecessors_iter = self._execution_graph.predecessors_iter
            atom_it = _depth_first_iterate(self._execution_graph,
                                           {co.FLOW: predecessors_iter},
                                           predecessors_iter(atom))
        for atom in atom_it:
            is_ready, late_decider = self._get_maybe_ready_for_revert(atom)
            if is_ready:
                yield (atom, late_decider)

    def _get_maybe_ready(self, atom, transition_to, allowed_intentions,
                         connected_fetcher, connected_checker,
                         decider_fetcher):
        state = self.get_state(atom)
        ok_to_transition = self._runtime.check_atom_transition(atom, state,
                                                               transition_to)
        if not ok_to_transition:
            return (False, None)
        intention = self._storage.get_atom_intention(atom.name)
        if intention not in allowed_intentions:
            return (False, None)
        connected_states = self._storage.get_atoms_states(
            connected_atom.name for connected_atom in connected_fetcher(atom))
        ok_to_run = connected_checker(six.itervalues(connected_states))
        if not ok_to_run:
            return (False, None)
        else:
            return (True, decider_fetcher(atom))

    def _get_maybe_ready_for_execute(self, atom):
        """Returns if an atom is *likely* ready to be executed."""
        def decider_fetcher(atom):
            edge_deciders = self._runtime.fetch_edge_deciders(atom)
            if edge_deciders:
                return IgnoreDecider(atom, edge_deciders)
            else:
                return NoOpDecider()
        predecessors_iter = self._execution_graph.predecessors_iter
        connected_fetcher = lambda atom: \
            _depth_first_iterate(self._execution_graph,
                                 {co.FLOW: predecessors_iter},
                                 predecessors_iter(atom))
        connected_checker = lambda connected_iter: \
            all(state == st.SUCCESS and intention == st.EXECUTE
                for state, intention in connected_iter)
        return self._get_maybe_ready(atom, st.RUNNING, [st.EXECUTE],
                                     connected_fetcher, connected_checker,
                                     decider_fetcher)

    def _get_maybe_ready_for_revert(self, atom):
        """Returns if an atom is *likely* ready to be reverted."""
        successors_iter = self._execution_graph.successors_iter
        connected_fetcher = lambda atom: \
            _depth_first_iterate(self._execution_graph,
                                 {co.FLOW: successors_iter},
                                 successors_iter(atom))
        connected_checker = lambda connected_iter: \
            all(state in (st.PENDING, st.REVERTED)
                for state, _intention in connected_iter)
        decider_fetcher = lambda atom: NoOpDecider()
        return self._get_maybe_ready(atom, st.REVERTING, [st.REVERT, st.RETRY],
                                     connected_fetcher, connected_checker,
                                     decider_fetcher)

    def iterate_connected_atoms(self, atom):
        """Iterates **all** successor atoms connected to given atom."""
        successors_iter = self._execution_graph.successors_iter
        return _depth_first_iterate(
            self._execution_graph, {
                co.FLOW: successors_iter,
                co.TASK: successors_iter,
                co.RETRY: successors_iter,
            }, successors_iter(atom))

    def iterate_retries(self, state=None):
        """Iterates retry atoms that match the provided state.

        If no state is provided it will yield back all retry atoms.
        """
        for atom in self.iterate_nodes((co.RETRY,)):
            if not state or self.get_state(atom) == state:
                yield atom

    def iterate_nodes(self, allowed_kinds):
        """Yields back all nodes of specified kinds in the execution graph."""
        for node, node_data in self._execution_graph.nodes_iter(data=True):
            if node_data['kind'] in allowed_kinds:
                yield node

    def find_retry(self, node):
        """Returns the retry atom associated to the given node (or none)."""
        return self._execution_graph.node[node].get(co.RETRY)

    def is_success(self):
        """Checks if all atoms in the execution graph are in 'happy' state."""
        for atom in self.iterate_nodes(co.ATOMS):
            atom_state = self.get_state(atom)
            if atom_state == st.IGNORE:
                continue
            if atom_state != st.SUCCESS:
                return False
        return True

    def get_state(self, atom):
        """Gets the state of a given atom (from the backend storage unit)."""
        return self._storage.get_atom_state(atom.name)
