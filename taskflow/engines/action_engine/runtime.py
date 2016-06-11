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
import functools

from futurist import waiters

from taskflow import deciders as de
from taskflow.engines.action_engine.actions import retry as ra
from taskflow.engines.action_engine.actions import task as ta
from taskflow.engines.action_engine import builder as bu
from taskflow.engines.action_engine import compiler as com
from taskflow.engines.action_engine import completer as co
from taskflow.engines.action_engine import scheduler as sched
from taskflow.engines.action_engine import scopes as sc
from taskflow.engines.action_engine import selector as se
from taskflow.engines.action_engine import traversal as tr
from taskflow import exceptions as exc
from taskflow import logging
from taskflow import states as st
from taskflow.utils import misc

from taskflow.flow import (LINK_DECIDER, LINK_DECIDER_DEPTH)  # noqa

# Small helper to make the edge decider tuples more easily useable...
_EdgeDecider = collections.namedtuple('_EdgeDecider',
                                      'from_node,kind,decider,depth')

LOG = logging.getLogger(__name__)


class Runtime(object):
    """A aggregate of runtime objects, properties, ... used during execution.

    This object contains various utility methods and properties that represent
    the collection of runtime components and functionality needed for an
    action engine to run to completion.
    """

    def __init__(self, compilation, storage, atom_notifier,
                 task_executor, retry_executor,
                 options=None):
        self._atom_notifier = atom_notifier
        self._task_executor = task_executor
        self._retry_executor = retry_executor
        self._storage = storage
        self._compilation = compilation
        self._atom_cache = {}
        self._options = misc.safe_copy_dict(options)

    def _walk_edge_deciders(self, graph, atom):
        """Iterates through all nodes, deciders that alter atoms execution."""
        # This is basically a reverse breadth first exploration, with
        # special logic to further traverse down flow nodes as needed...
        predecessors_iter = graph.predecessors_iter
        nodes = collections.deque((u_node, atom)
                                  for u_node in predecessors_iter(atom))
        visited = set()
        while nodes:
            u_node, v_node = nodes.popleft()
            u_node_kind = graph.node[u_node]['kind']
            u_v_data = graph.adj[u_node][v_node]
            try:
                decider = u_v_data[LINK_DECIDER]
                decider_depth = u_v_data.get(LINK_DECIDER_DEPTH)
                if decider_depth is None:
                    decider_depth = de.Depth.ALL
                yield _EdgeDecider(u_node, u_node_kind,
                                   decider, decider_depth)
            except KeyError:
                pass
            if u_node_kind == com.FLOW and u_node not in visited:
                # Avoid re-exploring the same flow if we get to this same
                # flow by a different *future* path...
                visited.add(u_node)
                # Since we *currently* jump over flow node(s), we need to make
                # sure that any prior decider that was directed at this flow
                # node also gets used during future decisions about this
                # atom node.
                nodes.extend((u_u_node, u_node)
                             for u_u_node in predecessors_iter(u_node))

    def compile(self):
        """Compiles & caches frequently used execution helper objects.

        Build out a cache of commonly used item that are associated
        with the contained atoms (by name), and are useful to have for
        quick lookup on (for example, the change state handler function for
        each atom, the scope walker object for each atom, the task or retry
        specific scheduler and so-on).
        """
        change_state_handlers = {
            com.TASK: functools.partial(self.task_action.change_state,
                                        progress=0.0),
            com.RETRY: self.retry_action.change_state,
        }
        schedulers = {
            com.RETRY: self.retry_scheduler,
            com.TASK: self.task_scheduler,
        }
        check_transition_handlers = {
            com.TASK: st.check_task_transition,
            com.RETRY: st.check_retry_transition,
        }
        actions = {
            com.TASK: self.task_action,
            com.RETRY: self.retry_action,
        }
        graph = self._compilation.execution_graph
        for node, node_data in graph.nodes_iter(data=True):
            node_kind = node_data['kind']
            if node_kind in com.FLOWS:
                continue
            elif node_kind in com.ATOMS:
                check_transition_handler = check_transition_handlers[node_kind]
                change_state_handler = change_state_handlers[node_kind]
                scheduler = schedulers[node_kind]
                action = actions[node_kind]
            else:
                raise exc.CompilationFailure("Unknown node kind '%s'"
                                             " encountered" % node_kind)
            metadata = {}
            deciders_it = self._walk_edge_deciders(graph, node)
            walker = sc.ScopeWalker(self.compilation, node, names_only=True)
            metadata['scope_walker'] = walker
            metadata['check_transition_handler'] = check_transition_handler
            metadata['change_state_handler'] = change_state_handler
            metadata['scheduler'] = scheduler
            metadata['edge_deciders'] = tuple(deciders_it)
            metadata['action'] = action
            LOG.trace("Compiled %s metadata for node %s (%s)",
                      metadata, node.name, node_kind)
            self._atom_cache[node.name] = metadata
        # TODO(harlowja): optimize the different decider depths to avoid
        # repeated full successor searching; this can be done by searching
        # for the widest depth of parent(s), and limiting the search of
        # children by the that depth.

    @property
    def compilation(self):
        return self._compilation

    @property
    def storage(self):
        return self._storage

    @property
    def options(self):
        return self._options

    @misc.cachedproperty
    def selector(self):
        return se.Selector(self)

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

    def _fetch_atom_metadata_entry(self, atom_name, metadata_key):
        return self._atom_cache[atom_name][metadata_key]

    def check_atom_transition(self, atom, current_state, target_state):
        """Checks if the atom can transition to the provided target state."""
        # This does not check if the name exists (since this is only used
        # internally to the engine, and is not exposed to atoms that will
        # not exist and therefore doesn't need to handle that case).
        check_transition_handler = self._fetch_atom_metadata_entry(
            atom.name, 'check_transition_handler')
        return check_transition_handler(current_state, target_state)

    def fetch_edge_deciders(self, atom):
        """Fetches the edge deciders for the given atom."""
        # This does not check if the name exists (since this is only used
        # internally to the engine, and is not exposed to atoms that will
        # not exist and therefore doesn't need to handle that case).
        return self._fetch_atom_metadata_entry(atom.name, 'edge_deciders')

    def fetch_scheduler(self, atom):
        """Fetches the cached specific scheduler for the given atom."""
        # This does not check if the name exists (since this is only used
        # internally to the engine, and is not exposed to atoms that will
        # not exist and therefore doesn't need to handle that case).
        return self._fetch_atom_metadata_entry(atom.name, 'scheduler')

    def fetch_action(self, atom):
        """Fetches the cached action handler for the given atom."""
        metadata = self._atom_cache[atom.name]
        return metadata['action']

    def fetch_scopes_for(self, atom_name):
        """Fetches a walker of the visible scopes for the given atom."""
        try:
            return self._fetch_atom_metadata_entry(atom_name, 'scope_walker')
        except KeyError:
            # This signals to the caller that there is no walker for whatever
            # atom name was given that doesn't really have any associated atom
            # known to be named with that name; this is done since the storage
            # layer will call into this layer to fetch a scope for a named
            # atom and users can provide random names that do not actually
            # exist...
            return None

    # Various helper methods used by the runtime components; not for public
    # consumption...

    def iterate_retries(self, state=None):
        """Iterates retry atoms that match the provided state.

        If no state is provided it will yield back all retry atoms.
        """
        if state:
            atoms = list(self.iterate_nodes((com.RETRY,)))
            atom_states = self._storage.get_atoms_states(atom.name
                                                         for atom in atoms)
            for atom in atoms:
                atom_state, _atom_intention = atom_states[atom.name]
                if atom_state == state:
                    yield atom
        else:
            for atom in self.iterate_nodes((com.RETRY,)):
                yield atom

    def iterate_nodes(self, allowed_kinds):
        """Yields back all nodes of specified kinds in the execution graph."""
        graph = self._compilation.execution_graph
        for node, node_data in graph.nodes_iter(data=True):
            if node_data['kind'] in allowed_kinds:
                yield node

    def is_success(self):
        """Checks if all atoms in the execution graph are in 'happy' state."""
        atoms = list(self.iterate_nodes(com.ATOMS))
        atom_states = self._storage.get_atoms_states(atom.name
                                                     for atom in atoms)
        for atom in atoms:
            atom_state, _atom_intention = atom_states[atom.name]
            if atom_state == st.IGNORE:
                continue
            if atom_state != st.SUCCESS:
                return False
        return True

    def find_retry(self, node):
        """Returns the retry atom associated to the given node (or none)."""
        graph = self._compilation.execution_graph
        return graph.node[node].get(com.RETRY)

    def reset_atoms(self, atoms, state=st.PENDING, intention=st.EXECUTE):
        """Resets all the provided atoms to the given state and intention."""
        tweaked = []
        for atom in atoms:
            if state or intention:
                tweaked.append((atom, state, intention))
            if state:
                change_state_handler = self._fetch_atom_metadata_entry(
                    atom.name, 'change_state_handler')
                change_state_handler(atom, state)
            if intention:
                self.storage.set_atom_intention(atom.name, intention)
        return tweaked

    def reset_all(self, state=st.PENDING, intention=st.EXECUTE):
        """Resets all atoms to the given state and intention."""
        return self.reset_atoms(self.iterate_nodes(com.ATOMS),
                                state=state, intention=intention)

    def reset_subgraph(self, atom, state=st.PENDING, intention=st.EXECUTE):
        """Resets a atoms subgraph to the given state and intention.

        The subgraph is contained of **all** of the atoms successors.
        """
        execution_graph = self._compilation.execution_graph
        atoms_it = tr.depth_first_iterate(execution_graph, atom,
                                          tr.Direction.FORWARD)
        return self.reset_atoms(atoms_it, state=state, intention=intention)

    def retry_subflow(self, retry):
        """Prepares a retrys + its subgraph for execution.

        This sets the retrys intention to ``EXECUTE`` and resets all of its
        subgraph (its successors) to the ``PENDING`` state with an ``EXECUTE``
        intention.
        """
        tweaked = self.reset_atoms([retry], state=None, intention=st.EXECUTE)
        tweaked.extend(self.reset_subgraph(retry))
        return tweaked
