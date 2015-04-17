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

from taskflow.engines.action_engine.actions import retry as ra
from taskflow.engines.action_engine.actions import task as ta
from taskflow.engines.action_engine import analyzer as an
from taskflow.engines.action_engine import completer as co
from taskflow.engines.action_engine import runner as ru
from taskflow.engines.action_engine import scheduler as sched
from taskflow.engines.action_engine import scopes as sc
from taskflow import states as st
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
        self._walkers_to_names = {}

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
    def retry_action(self):
        return ra.RetryAction(self._storage,
                              self._atom_notifier)

    @misc.cachedproperty
    def task_action(self):
        return ta.TaskAction(self._storage,
                             self._atom_notifier,
                             self._task_executor)

    def fetch_scopes_for(self, atom_name):
        """Fetches a walker of the visible scopes for the given atom."""
        try:
            return self._walkers_to_names[atom_name]
        except KeyError:
            atom = None
            for node in self.analyzer.iterate_all_nodes():
                if node.name == atom_name:
                    atom = node
                    break
            if atom is not None:
                walker = sc.ScopeWalker(self.compilation, atom,
                                        names_only=True)
                self._walkers_to_names[atom_name] = walker
            else:
                walker = None
            return walker

    # Various helper methods used by the runtime components; not for public
    # consumption...

    def reset_nodes(self, nodes, state=st.PENDING, intention=st.EXECUTE):
        tweaked = []
        node_state_handlers = [
            (self.task_action, {'progress': 0.0}),
            (self.retry_action, {}),
        ]
        for node in nodes:
            if state or intention:
                tweaked.append((node, state, intention))
            if state:
                handled = False
                for h, kwargs in node_state_handlers:
                    if h.handles(node):
                        h.change_state(node, state, **kwargs)
                        handled = True
                        break
                if not handled:
                    raise TypeError("Unknown how to reset state of"
                                    " node '%s' (%s)" % (node, type(node)))
            if intention:
                self.storage.set_atom_intention(node.name, intention)
        return tweaked

    def reset_all(self, state=st.PENDING, intention=st.EXECUTE):
        return self.reset_nodes(self.analyzer.iterate_all_nodes(),
                                state=state, intention=intention)

    def reset_subgraph(self, node, state=st.PENDING, intention=st.EXECUTE):
        return self.reset_nodes(self.analyzer.iterate_subgraph(node),
                                state=state, intention=intention)

    def retry_subflow(self, retry):
        self.storage.set_atom_intention(retry.name, st.EXECUTE)
        self.reset_subgraph(retry)
