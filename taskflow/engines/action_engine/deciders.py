# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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

import six

from taskflow import deciders
from taskflow.engines.action_engine import compiler
from taskflow.engines.action_engine import traversal
from taskflow import logging
from taskflow import states

LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class Decider(object):
    """Base class for deciders.

    Provides interface to be implemented by sub-classes.

    Deciders check whether next atom in flow should be executed or not.
    """

    @abc.abstractmethod
    def tally(self, runtime):
        """Tally edge deciders on whether this decider should allow running.

        The returned value is a list of edge deciders that voted
        'nay' (do not allow running).
        """

    @abc.abstractmethod
    def affect(self, runtime, nay_voters):
        """Affects associated atoms due to at least one 'nay' edge decider.

        This will alter the associated atom + some set of successor atoms by
        setting there state and intention to ``IGNORE`` so that they are
        ignored in future runtime activities.
        """

    def check_and_affect(self, runtime):
        """Handles :py:func:`~.tally` + :py:func:`~.affect` in right order.

        NOTE(harlowja):  If there are zero 'nay' edge deciders then it is
        assumed this decider should allow running.

        Returns boolean of whether this decider allows for running (or not).
        """
        nay_voters = self.tally(runtime)
        if nay_voters:
            self.affect(runtime, nay_voters)
            return False
        return True


def _affect_all_successors(atom, runtime):
    execution_graph = runtime.compilation.execution_graph
    successors_iter = traversal.depth_first_iterate(
        execution_graph, atom, traversal.Direction.FORWARD)
    runtime.reset_atoms(itertools.chain([atom], successors_iter),
                        state=states.IGNORE, intention=states.IGNORE)


def _affect_successor_tasks_in_same_flow(atom, runtime):
    execution_graph = runtime.compilation.execution_graph
    successors_iter = traversal.depth_first_iterate(
        execution_graph, atom, traversal.Direction.FORWARD,
        # Do not go through nested flows but do follow *all* tasks that
        # are directly connected in this same flow (thus the reason this is
        # called the same flow decider); retries are direct successors
        # of flows, so they should also be not traversed through, but
        # setting this explicitly ensures that.
        through_flows=False, through_retries=False)
    runtime.reset_atoms(itertools.chain([atom], successors_iter),
                        state=states.IGNORE, intention=states.IGNORE)


def _affect_atom(atom, runtime):
    runtime.reset_atoms([atom], state=states.IGNORE, intention=states.IGNORE)


def _affect_direct_task_neighbors(atom, runtime):
    def _walk_neighbors():
        execution_graph = runtime.compilation.execution_graph
        for node in execution_graph.successors_iter(atom):
            node_data = execution_graph.node[node]
            if node_data['kind'] == compiler.TASK:
                yield node
    successors_iter = _walk_neighbors()
    runtime.reset_atoms(itertools.chain([atom], successors_iter),
                        state=states.IGNORE, intention=states.IGNORE)


class IgnoreDecider(Decider):
    """Checks any provided edge-deciders and determines if ok to run."""

    _depth_strategies = {
        deciders.Depth.ALL: _affect_all_successors,
        deciders.Depth.ATOM: _affect_atom,
        deciders.Depth.FLOW: _affect_successor_tasks_in_same_flow,
        deciders.Depth.NEIGHBORS: _affect_direct_task_neighbors,
    }

    def __init__(self, atom, edge_deciders):
        self._atom = atom
        self._edge_deciders = edge_deciders

    def tally(self, runtime):
        voters = {
            'run_it': [],
            'do_not_run_it': [],
            'ignored': [],
        }
        history = {}
        if self._edge_deciders:
            # Gather all atoms (the ones that were not ignored) results so
            # that those results can be used by the decider(s) that are
            # making a decision as to pass or not pass...
            states_intentions = runtime.storage.get_atoms_states(
                ed.from_node.name for ed in self._edge_deciders
                if ed.kind in compiler.ATOMS)
            for atom_name in six.iterkeys(states_intentions):
                atom_state, _atom_intention = states_intentions[atom_name]
                if atom_state != states.IGNORE:
                    history[atom_name] = runtime.storage.get(atom_name)
            for ed in self._edge_deciders:
                if (ed.kind in compiler.ATOMS and
                        # It was an ignored atom (not included in history and
                        # the only way that is possible is via above loop
                        # skipping it...)
                        ed.from_node.name not in history):
                    voters['ignored'].append(ed)
                    continue
                if not ed.decider(history=history):
                    voters['do_not_run_it'].append(ed)
                else:
                    voters['run_it'].append(ed)
        if LOG.isEnabledFor(logging.TRACE):
            LOG.trace("Out of %s deciders there were %s 'do no run it'"
                      " voters, %s 'do run it' voters and %s 'ignored'"
                      " voters for transition to atom '%s' given history %s",
                      sum(len(eds) for eds in six.itervalues(voters)),
                      list(ed.from_node.name
                           for ed in voters['do_not_run_it']),
                      list(ed.from_node.name for ed in voters['run_it']),
                      list(ed.from_node.name for ed in voters['ignored']),
                      self._atom.name, history)
        return voters['do_not_run_it']

    def affect(self, runtime, nay_voters):
        # If there were many 'nay' edge deciders that were targeted
        # at this atom, then we need to pick the one which has the widest
        # impact and respect that one as the decider depth that will
        # actually affect things.
        widest_depth = deciders.pick_widest(ed.depth for ed in nay_voters)
        affector = self._depth_strategies[widest_depth]
        return affector(self._atom, runtime)


class NoOpDecider(Decider):
    """No-op decider that says it is always ok to run & has no effect(s)."""

    def tally(self, runtime):
        """Always good to go."""
        return []

    def affect(self, runtime, nay_voters):
        """Does nothing."""
