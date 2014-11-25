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

from taskflow.engines.action_engine import analyzer as an
from taskflow.engines.action_engine import completer as co
from taskflow.engines.action_engine import retry_action as ra
from taskflow.engines.action_engine import runner as ru
from taskflow.engines.action_engine import scheduler as sched
from taskflow.engines.action_engine import scopes as sc
from taskflow.engines.action_engine import task_action as ta
from taskflow import retry as retry_atom
from taskflow import states as st
from taskflow import task as task_atom
from taskflow.utils import misc


class Runtime(object):
    """A aggregate of runtime objects, properties, ... used during execution.

    This object contains various utility methods and properties that represent
    the collection of runtime components and functionality needed for an
    action engine to run to completion.
    """

    def __init__(self, compilation, storage, task_notifier, task_executor):
        self._task_notifier = task_notifier
        self._task_executor = task_executor
        self._storage = storage
        self._compilation = compilation

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
        return ra.RetryAction(self._storage, self._task_notifier,
                              lambda atom: sc.ScopeWalker(self.compilation,
                                                          atom,
                                                          names_only=True))

    @misc.cachedproperty
    def task_action(self):
        return ta.TaskAction(self._storage, self._task_executor,
                             self._task_notifier,
                             lambda atom: sc.ScopeWalker(self.compilation,
                                                         atom,
                                                         names_only=True))

    # Various helper methods used by the runtime components; not for public
    # consumption...

    def reset_nodes(self, nodes, state=st.PENDING, intention=st.EXECUTE):
        for node in nodes:
            if state:
                if isinstance(node, task_atom.BaseTask):
                    self.task_action.change_state(node, state, progress=0.0)
                elif isinstance(node, retry_atom.Retry):
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
