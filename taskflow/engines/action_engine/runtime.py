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

import functools

from futurist import waiters

from taskflow.engines.action_engine.actions import retry as ra
from taskflow.engines.action_engine.actions import task as ta
from taskflow.engines.action_engine import analyzer as an
from taskflow.engines.action_engine import builder as bu
from taskflow.engines.action_engine import completer as co
from taskflow.engines.action_engine import scheduler as sched
from taskflow.engines.action_engine import scopes as sc
from taskflow import flow
from taskflow import states as st
from taskflow import task
from taskflow.utils import misc


class Runtime(object):
    """A aggregate of runtime objects, properties, ... used during execution.

    This object contains various utility methods and properties that represent
    the collection of runtime components and functionality needed for an
    action engine to run to completion.
    """

    def __init__(self, compilation, storage, atom_notifier,
                 task_executor, retry_executor):
        self._atom_notifier = atom_notifier
        self._task_executor = task_executor
        self._retry_executor = retry_executor
        self._storage = storage
        self._compilation = compilation
        self._atom_cache = {}
        self._atoms_by_kind = {}

    def compile(self):
        """Compiles & caches frequently used execution helper objects.

        Build out a cache of commonly used item that are associated
        with the contained atoms (by name), and are useful to have for
        quick lookup on (for example, the change state handler function for
        each atom, the scope walker object for each atom, the task or retry
        specific scheduler and so-on).
        """
        change_state_handlers = {
            'task': functools.partial(self.task_action.change_state,
                                      progress=0.0),
            'retry': self.retry_action.change_state,
        }
        schedulers = {
            'retry': self.retry_scheduler,
            'task': self.task_scheduler,
        }
        execution_graph = self._compilation.execution_graph
        all_retry_atoms = []
        all_task_atoms = []
        for atom in self.analyzer.iterate_all_nodes():
            metadata = {}
            walker = sc.ScopeWalker(self.compilation, atom, names_only=True)
            if isinstance(atom, task.BaseTask):
                check_transition_handler = st.check_task_transition
                change_state_handler = change_state_handlers['task']
                scheduler = schedulers['task']
                all_task_atoms.append(atom)
            else:
                check_transition_handler = st.check_retry_transition
                change_state_handler = change_state_handlers['retry']
                scheduler = schedulers['retry']
                all_retry_atoms.append(atom)
            edge_deciders = {}
            for previous_atom in execution_graph.predecessors(atom):
                # If there is any link function that says if this connection
                # is able to run (or should not) ensure we retain it and use
                # it later as needed.
                u_v_data = execution_graph.adj[previous_atom][atom]
                u_v_decider = u_v_data.get(flow.LINK_DECIDER)
                if u_v_decider is not None:
                    edge_deciders[previous_atom.name] = u_v_decider
            metadata['scope_walker'] = walker
            metadata['check_transition_handler'] = check_transition_handler
            metadata['change_state_handler'] = change_state_handler
            metadata['scheduler'] = scheduler
            metadata['edge_deciders'] = edge_deciders
            self._atom_cache[atom.name] = metadata
        self._atoms_by_kind['retry'] = all_retry_atoms
        self._atoms_by_kind['task'] = all_task_atoms

    @property
    def compilation(self):
        return self._compilation

    @property
    def storage(self):
        return self._storage

    @misc.cachedproperty
    def analyzer(self):
        return an.Analyzer(self)

    @misc.cachedproperty
    def builder(self):
        return bu.MachineBuilder(self, waiters.wait_for_any)

    @misc.cachedproperty
    def completer(self):
        return co.Completer(self)

    @misc.cachedproperty
    def scheduler(self):
        return sched.Scheduler(self)

    @misc.cachedproperty
    def task_scheduler(self):
        return sched.TaskScheduler(self)

    @misc.cachedproperty
    def retry_scheduler(self):
        return sched.RetryScheduler(self)

    @misc.cachedproperty
    def retry_action(self):
        return ra.RetryAction(self._storage,
                              self._atom_notifier,
                              self._retry_executor)

    @misc.cachedproperty
    def task_action(self):
        return ta.TaskAction(self._storage,
                             self._atom_notifier,
                             self._task_executor)

    def check_atom_transition(self, atom, current_state, target_state):
        """Checks if the atom can transition to the provided target state."""
        # This does not check if the name exists (since this is only used
        # internally to the engine, and is not exposed to atoms that will
        # not exist and therefore doesn't need to handle that case).
        metadata = self._atom_cache[atom.name]
        check_transition_handler = metadata['check_transition_handler']
        return check_transition_handler(current_state, target_state)

    def fetch_edge_deciders(self, atom):
        """Fetches the edge deciders for the given atom."""
        # This does not check if the name exists (since this is only used
        # internally to the engine, and is not exposed to atoms that will
        # not exist and therefore doesn't need to handle that case).
        metadata = self._atom_cache[atom.name]
        return metadata['edge_deciders']

    def fetch_atoms_by_kind(self, kind):
        """Fetches all the atoms of a given kind.

        NOTE(harlowja): Currently only ``task`` or ``retry`` are valid
                        kinds of atoms (requesting other kinds will just
                        return empty lists).
        """
        return self._atoms_by_kind.get(kind, [])

    def fetch_scheduler(self, atom):
        """Fetches the cached specific scheduler for the given atom."""
        # This does not check if the name exists (since this is only used
        # internally to the engine, and is not exposed to atoms that will
        # not exist and therefore doesn't need to handle that case).
        metadata = self._atom_cache[atom.name]
        return metadata['scheduler']

    def fetch_scopes_for(self, atom_name):
        """Fetches a walker of the visible scopes for the given atom."""
        try:
            metadata = self._atom_cache[atom_name]
        except KeyError:
            # This signals to the caller that there is no walker for whatever
            # atom name was given that doesn't really have any associated atom
            # known to be named with that name; this is done since the storage
            # layer will call into this layer to fetch a scope for a named
            # atom and users can provide random names that do not actually
            # exist...
            return None
        else:
            return metadata['scope_walker']

    # Various helper methods used by the runtime components; not for public
    # consumption...

    def reset_nodes(self, atoms, state=st.PENDING, intention=st.EXECUTE):
        """Resets all the provided atoms to the given state and intention."""
        tweaked = []
        for atom in atoms:
            metadata = self._atom_cache[atom.name]
            if state or intention:
                tweaked.append((atom, state, intention))
            if state:
                change_state_handler = metadata['change_state_handler']
                change_state_handler(atom, state)
            if intention:
                self.storage.set_atom_intention(atom.name, intention)
        return tweaked

    def reset_all(self, state=st.PENDING, intention=st.EXECUTE):
        """Resets all atoms to the given state and intention."""
        return self.reset_nodes(self.analyzer.iterate_all_nodes(),
                                state=state, intention=intention)

    def reset_subgraph(self, atom, state=st.PENDING, intention=st.EXECUTE):
        """Resets a atoms subgraph to the given state and intention.

        The subgraph is contained of all of the atoms successors.
        """
        return self.reset_nodes(self.analyzer.iterate_subgraph(atom),
                                state=state, intention=intention)

    def retry_subflow(self, retry):
        """Prepares a retrys + its subgraph for execution.

        This sets the retrys intention to ``EXECUTE`` and resets all of its
        subgraph (its successors) to the ``PENDING`` state with an ``EXECUTE``
        intention.
        """
        self.storage.set_atom_intention(retry.name, st.EXECUTE)
        self.reset_subgraph(retry)
