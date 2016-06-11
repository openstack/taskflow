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

import operator
import weakref

from taskflow.engines.action_engine import compiler as co
from taskflow.engines.action_engine import deciders
from taskflow.engines.action_engine import traversal
from taskflow import logging
from taskflow import states as st
from taskflow.utils import iter_utils

LOG = logging.getLogger(__name__)


class Selector(object):
    """Selector that uses a compilation and aids in execution processes.

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
            return iter_utils.unique_seen((self._browse_atoms_for_execute(),
                                           self._browse_atoms_for_revert()),
                                          seen_selector=operator.itemgetter(0))
        state = self._storage.get_atom_state(atom.name)
        intention = self._storage.get_atom_intention(atom.name)
        if state == st.SUCCESS:
            if intention == st.REVERT:
                return iter([
                    (atom, deciders.NoOpDecider()),
                ])
            elif intention == st.EXECUTE:
                return self._browse_atoms_for_execute(atom=atom)
            else:
                return iter([])
        elif state == st.REVERTED:
            return self._browse_atoms_for_revert(atom=atom)
        elif state == st.FAILURE:
            return self._browse_atoms_for_revert()
        else:
            return iter([])

    def _browse_atoms_for_execute(self, atom=None):
        """Browse next atoms to execute.

        This returns a iterator of atoms that *may* be ready to be
        executed, if given a specific atom, it will only examine the successors
        of that atom, otherwise it will examine the whole graph.
        """
        if atom is None:
            atom_it = self._runtime.iterate_nodes(co.ATOMS)
        else:
            # NOTE(harlowja): the reason this uses breadth first is so that
            # when deciders are applied that those deciders can be applied
            # from top levels to lower levels since lower levels *may* be
            # able to run even if top levels have deciders that decide to
            # ignore some atoms... (going deeper first would make this
            # problematic to determine as top levels can have their deciders
            # applied **after** going deeper).
            atom_it = traversal.breadth_first_iterate(
                self._execution_graph, atom, traversal.Direction.FORWARD)
        for atom in atom_it:
            is_ready, late_decider = self._get_maybe_ready_for_execute(atom)
            if is_ready:
                yield (atom, late_decider)

    def _browse_atoms_for_revert(self, atom=None):
        """Browse next atoms to revert.

        This returns a iterator of atoms that *may* be ready to be be
        reverted, if given a specific atom it will only examine the
        predecessors of that atom, otherwise it will examine the whole
        graph.
        """
        if atom is None:
            atom_it = self._runtime.iterate_nodes(co.ATOMS)
        else:
            atom_it = traversal.breadth_first_iterate(
                self._execution_graph, atom, traversal.Direction.BACKWARD,
                # Stop at the retry boundary (as retries 'control' there
                # surronding atoms, and we don't want to back track over
                # them so that they can correctly affect there associated
                # atoms); we do though need to jump through all tasks since
                # if a predecessor Y was ignored and a predecessor Z before Y
                # was not it should be eligible to now revert...
                through_retries=False)
        for atom in atom_it:
            is_ready, late_decider = self._get_maybe_ready_for_revert(atom)
            if is_ready:
                yield (atom, late_decider)

    def _get_maybe_ready(self, atom, transition_to, allowed_intentions,
                         connected_fetcher, ready_checker,
                         decider_fetcher, for_what="?"):
        def iter_connected_states():
            # Lazily iterate over connected states so that ready checkers
            # can stop early (vs having to consume and check all the
            # things...)
            for atom in connected_fetcher():
                # TODO(harlowja): make this storage api better, its not
                # especially clear what the following is doing (mainly
                # to avoid two calls into storage).
                atom_states = self._storage.get_atoms_states([atom.name])
                yield (atom, atom_states[atom.name])
        # NOTE(harlowja): How this works is the following...
        #
        # 1. First check if the current atom can even transition to the
        #    desired state, if not this atom is definitely not ready to
        #    execute or revert.
        # 2. Check if the actual atoms intention is in one of the desired/ok
        #    intentions, if it is not there we are still not ready to execute
        #    or revert.
        # 3. Iterate over (atom, atom_state, atom_intention) for all the
        #    atoms the 'connected_fetcher' callback yields from underlying
        #    storage and direct that iterator into the 'ready_checker'
        #    callback, that callback should then iterate over these entries
        #    and determine if it is ok to execute or revert.
        # 4. If (and only if) 'ready_checker' returns true, then
        #    the 'decider_fetcher' callback is called to get a late decider
        #    which can (if it desires) affect this ready result (but does
        #    so right before the atom is about to be scheduled).
        state = self._storage.get_atom_state(atom.name)
        ok_to_transition = self._runtime.check_atom_transition(atom, state,
                                                               transition_to)
        if not ok_to_transition:
            LOG.trace("Atom '%s' is not ready to %s since it can not"
                      " transition to %s from its current state %s",
                      atom, for_what, transition_to, state)
            return (False, None)
        intention = self._storage.get_atom_intention(atom.name)
        if intention not in allowed_intentions:
            LOG.trace("Atom '%s' is not ready to %s since its current"
                      " intention %s is not in allowed intentions %s",
                      atom, for_what, intention, allowed_intentions)
            return (False, None)
        ok_to_run = ready_checker(iter_connected_states())
        if not ok_to_run:
            return (False, None)
        else:
            return (True, decider_fetcher())

    def _get_maybe_ready_for_execute(self, atom):
        """Returns if an atom is *likely* ready to be executed."""
        def ready_checker(pred_connected_it):
            for pred in pred_connected_it:
                pred_atom, (pred_atom_state, pred_atom_intention) = pred
                if (pred_atom_state in (st.SUCCESS, st.IGNORE) and
                        pred_atom_intention in (st.EXECUTE, st.IGNORE)):
                    continue
                LOG.trace("Unable to begin to execute since predecessor"
                          " atom '%s' is in state %s with intention %s",
                          pred_atom, pred_atom_state, pred_atom_intention)
                return False
            LOG.trace("Able to let '%s' execute", atom)
            return True
        decider_fetcher = lambda: \
            deciders.IgnoreDecider(
                atom, self._runtime.fetch_edge_deciders(atom))
        connected_fetcher = lambda: \
            traversal.depth_first_iterate(self._execution_graph, atom,
                                          # Whether the desired atom
                                          # can execute is dependent on its
                                          # predecessors outcomes (thus why
                                          # we look backwards).
                                          traversal.Direction.BACKWARD)
        # If this atoms current state is able to be transitioned to RUNNING
        # and its intention is to EXECUTE and all of its predecessors executed
        # successfully or were ignored then this atom is ready to execute.
        LOG.trace("Checking if '%s' is ready to execute", atom)
        return self._get_maybe_ready(atom, st.RUNNING, [st.EXECUTE],
                                     connected_fetcher, ready_checker,
                                     decider_fetcher, for_what='execute')

    def _get_maybe_ready_for_revert(self, atom):
        """Returns if an atom is *likely* ready to be reverted."""
        def ready_checker(succ_connected_it):
            for succ in succ_connected_it:
                succ_atom, (succ_atom_state, _succ_atom_intention) = succ
                if succ_atom_state not in (st.PENDING, st.REVERTED, st.IGNORE):
                    LOG.trace("Unable to begin to revert since successor"
                              " atom '%s' is in state %s", succ_atom,
                              succ_atom_state)
                    return False
            LOG.trace("Able to let '%s' revert", atom)
            return True
        noop_decider = deciders.NoOpDecider()
        connected_fetcher = lambda: \
            traversal.depth_first_iterate(self._execution_graph, atom,
                                          # Whether the desired atom
                                          # can revert is dependent on its
                                          # successors states (thus why we
                                          # look forwards).
                                          traversal.Direction.FORWARD)
        decider_fetcher = lambda: noop_decider
        # If this atoms current state is able to be transitioned to REVERTING
        # and its intention is either REVERT or RETRY and all of its
        # successors are either PENDING or REVERTED then this atom is ready
        # to revert.
        LOG.trace("Checking if '%s' is ready to revert", atom)
        return self._get_maybe_ready(atom, st.REVERTING, [st.REVERT, st.RETRY],
                                     connected_fetcher, ready_checker,
                                     decider_fetcher, for_what='revert')
