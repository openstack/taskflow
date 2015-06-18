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

from taskflow.engines.action_engine.actions import retry as ra
from taskflow.engines.action_engine.actions import task as ta
from taskflow.engines.action_engine import analyzer as an
from taskflow.engines.action_engine import completer as co
from taskflow.engines.action_engine import runner as ru
from taskflow.engines.action_engine import scheduler as sched
from taskflow.engines.action_engine import scopes as sc
from taskflow import states as st
from taskflow import task
from taskflow.utils import misc


class Runtime(object):
    """A aggregate of runtime objects, properties, ... used during execution.

    This object contains various utility methods and properties that represent
    the collection of runtime components and functionality needed for an
    action engine to run to completion.
    """

    def __init__(self, compilation, storage, atom_notifier, task_executor):
        self._atom_notifier = atom_notifier
        self._task_executor = task_executor
        self._storage = storage
        self._compilation = compilation
        self._atom_cache = {}

    def compile(self):
        # Build out a cache of commonly used item that are associated
        # with the contained atoms (by name), and are useful to have for
        # quick lookup on...
        change_state_handlers = {
            'task': functools.partial(self.task_action.change_state,
                                      progress=0.0),
            'retry': self.retry_action.change_state,
        }
        schedulers = {
            'retry': self.retry_scheduler,
            'task': self.task_scheduler,
        }
        for atom in self.analyzer.iterate_all_nodes():
            metadata = {}
            walker = sc.ScopeWalker(self.compilation, atom, names_only=True)
            if isinstance(atom, task.BaseTask):
                check_transition_handler = st.check_task_transition
                change_state_handler = change_state_handlers['task']
                scheduler = schedulers['task']
            else:
                check_transition_handler = st.check_retry_transition
                change_state_handler = change_state_handlers['retry']
                scheduler = schedulers['retry']
            metadata['scope_walker'] = walker
            metadata['check_transition_handler'] = check_transition_handler
            metadata['change_state_handler'] = change_state_handler
            metadata['scheduler'] = scheduler
            self._atom_cache[atom.name] = metadata

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
    def runner(self):
        return ru.Runner(self, self._task_executor)

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
                              self._atom_notifier)

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
        return self.reset_nodes(self.analyzer.iterate_all_nodes(),
                                state=state, intention=intention)

    def reset_subgraph(self, atom, state=st.PENDING, intention=st.EXECUTE):
        return self.reset_nodes(self.analyzer.iterate_subgraph(atom),
                                state=state, intention=intention)

    def retry_subflow(self, retry):
        self.storage.set_atom_intention(retry.name, st.EXECUTE)
        self.reset_subgraph(retry)
