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
        self._scopes = {}

    @property
    def compilation(self):
        return self._compilation

    @property
    def storage(self):
        return self._storage

    @misc.cachedproperty
    def analyzer(self):
        return an.Analyzer(self._compilation, self._storage)

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
        return ra.RetryAction(self._storage, self._atom_notifier,
                              self._fetch_scopes_for)

    @misc.cachedproperty
    def task_action(self):
        return ta.TaskAction(self._storage,
                             self._atom_notifier, self._fetch_scopes_for,
                             self._task_executor)

    def _fetch_scopes_for(self, atom):
        """Fetches a tuple of the visible scopes for the given atom."""
        try:
            return self._scopes[atom]
        except KeyError:
            walker = sc.ScopeWalker(self.compilation, atom,
                                    names_only=True)
            visible_to = tuple(walker)
            self._scopes[atom] = visible_to
            return visible_to

    # Various helper methods used by the runtime components; not for public
    # consumption...

    def reset_nodes(self, nodes, state=st.PENDING, intention=st.EXECUTE):
        for node in nodes:
            if state:
                if self.task_action.handles(node):
                    self.task_action.change_state(node, state,
                                                  progress=0.0)
                elif self.retry_action.handles(node):
                    self.retry_action.change_state(node, state)
                else:
                    raise TypeError("Unknown how to reset atom '%s' (%s)"
                                    % (node, type(node)))
            if intention:
                self.storage.set_atom_intention(node.name, intention)

    def reset_all(self, state=st.PENDING, intention=st.EXECUTE):
        self.reset_nodes(self.analyzer.iterate_all_nodes(),
                         state=state, intention=intention)

    def reset_subgraph(self, node, state=st.PENDING, intention=st.EXECUTE):
        self.reset_nodes(self.analyzer.iterate_subgraph(node),
                         state=state, intention=intention)

    def retry_subflow(self, retry):
        self.storage.set_atom_intention(retry.name, st.EXECUTE)
        self.reset_subgraph(retry)
