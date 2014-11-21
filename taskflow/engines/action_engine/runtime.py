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

from taskflow.engines.action_engine import analyzer as ca
from taskflow.engines.action_engine import executor as ex
from taskflow.engines.action_engine import retry_action as ra
from taskflow.engines.action_engine import runner as ru
from taskflow.engines.action_engine import scopes as sc
from taskflow.engines.action_engine import task_action as ta
from taskflow import exceptions as excp
from taskflow import retry as retry_atom
from taskflow import states as st
from taskflow import task as task_atom
from taskflow.types import failure
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

    @property
    def compilation(self):
        return self._compilation

    @property
    def storage(self):
        return self._storage

    @misc.cachedproperty
    def analyzer(self):
        return ca.Analyzer(self._compilation, self._storage)

    @misc.cachedproperty
    def runner(self):
        return ru.Runner(self, self._task_executor)

    @misc.cachedproperty
    def completer(self):
        return Completer(self)

    @misc.cachedproperty
    def scheduler(self):
        return Scheduler(self)

    @misc.cachedproperty
    def retry_action(self):
        return ra.RetryAction(self._storage, self._atom_notifier,
                              lambda atom: sc.ScopeWalker(self.compilation,
                                                          atom,
                                                          names_only=True))

    @misc.cachedproperty
    def task_action(self):
        return ta.TaskAction(self._storage, self._task_executor,
                             self._atom_notifier,
                             lambda atom: sc.ScopeWalker(self.compilation,
                                                         atom,
                                                         names_only=True))

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


# Various helper methods used by completer and scheduler.
def _retry_subflow(retry, runtime):
    runtime.storage.set_atom_intention(retry.name, st.EXECUTE)
    runtime.reset_subgraph(retry)


class Completer(object):
    """Completes atoms using actions to complete them."""

    def __init__(self, runtime):
        self._analyzer = runtime.analyzer
        self._retry_action = runtime.retry_action
        self._runtime = runtime
        self._storage = runtime.storage
        self._task_action = runtime.task_action

    def _complete_task(self, task, event, result):
        """Completes the given task, processes task failure."""
        if event == ex.EXECUTED:
            self._task_action.complete_execution(task, result)
        else:
            self._task_action.complete_reversion(task, result)

    def resume(self):
        """Resumes nodes in the contained graph.

        This is done to allow any previously completed or failed nodes to
        be analyzed, there results processed and any potential nodes affected
        to be adjusted as needed.

        This should return a set of nodes which should be the initial set of
        nodes that were previously not finished (due to a RUNNING or REVERTING
        attempt not previously finishing).
        """
        for node in self._analyzer.iterate_all_nodes():
            if self._analyzer.get_state(node) == st.FAILURE:
                self._process_atom_failure(node, self._storage.get(node.name))
        for retry in self._analyzer.iterate_retries(st.RETRYING):
            _retry_subflow(retry, self._runtime)
        unfinished_nodes = set()
        for node in self._analyzer.iterate_all_nodes():
            if self._analyzer.get_state(node) in (st.RUNNING, st.REVERTING):
                unfinished_nodes.add(node)
        return unfinished_nodes

    def complete(self, node, event, result):
        """Performs post-execution completion of a node.

        Returns whether the result should be saved into an accumulator of
        failures or whether this should not be done.
        """
        if isinstance(node, task_atom.BaseTask):
            self._complete_task(node, event, result)
        if isinstance(result, failure.Failure):
            if event == ex.EXECUTED:
                self._process_atom_failure(node, result)
            else:
                return True
        return False

    def _process_atom_failure(self, atom, failure):
        """Processes atom failure & applies resolution strategies.

        On atom failure this will find the atoms associated retry controller
        and ask that controller for the strategy to perform to resolve that
        failure. After getting a resolution strategy decision this method will
        then adjust the needed other atoms intentions, and states, ... so that
        the failure can be worked around.
        """
        retry = self._analyzer.find_atom_retry(atom)
        if retry:
            # Ask retry controller what to do in case of failure
            action = self._retry_action.on_failure(retry, atom, failure)
            if action == retry_atom.RETRY:
                # Prepare subflow for revert
                self._storage.set_atom_intention(retry.name, st.RETRY)
                self._runtime.reset_subgraph(retry, state=None,
                                             intention=st.REVERT)
            elif action == retry_atom.REVERT:
                # Ask parent checkpoint
                self._process_atom_failure(retry, failure)
            elif action == retry_atom.REVERT_ALL:
                # Prepare all flow for revert
                self._revert_all()
        else:
            # Prepare all flow for revert
            self._revert_all()

    def _revert_all(self):
        """Attempts to set all nodes to the REVERT intention."""
        self._runtime.reset_nodes(self._analyzer.iterate_all_nodes(),
                                  state=None, intention=st.REVERT)


class Scheduler(object):
    """Schedules atoms using actions to schedule."""

    def __init__(self, runtime):
        self._analyzer = runtime.analyzer
        self._retry_action = runtime.retry_action
        self._runtime = runtime
        self._storage = runtime.storage
        self._task_action = runtime.task_action

    def _schedule_node(self, node):
        """Schedule a single node for execution."""
        # TODO(harlowja): we need to rework this so that we aren't doing type
        # checking here, type checking usually means something isn't done right
        # and usually will limit extensibility in the future.
        if isinstance(node, task_atom.BaseTask):
            return self._schedule_task(node)
        elif isinstance(node, retry_atom.Retry):
            return self._schedule_retry(node)
        else:
            raise TypeError("Unknown how to schedule atom '%s' (%s)"
                            % (node, type(node)))

    def _schedule_retry(self, retry):
        """Schedules the given retry atom for *future* completion.

        Depending on the atoms stored intention this may schedule the retry
        atom for reversion or execution.
        """
        intention = self._storage.get_atom_intention(retry.name)
        if intention == st.EXECUTE:
            return self._retry_action.execute(retry)
        elif intention == st.REVERT:
            return self._retry_action.revert(retry)
        elif intention == st.RETRY:
            self._retry_action.change_state(retry, st.RETRYING)
            _retry_subflow(retry, self._runtime)
            return self._retry_action.execute(retry)
        else:
            raise excp.ExecutionFailure("Unknown how to schedule retry with"
                                        " intention: %s" % intention)

    def _schedule_task(self, task):
        """Schedules the given task atom for *future* completion.

        Depending on the atoms stored intention this may schedule the task
        atom for reversion or execution.
        """
        intention = self._storage.get_atom_intention(task.name)
        if intention == st.EXECUTE:
            return self._task_action.schedule_execution(task)
        elif intention == st.REVERT:
            return self._task_action.schedule_reversion(task)
        else:
            raise excp.ExecutionFailure("Unknown how to schedule task with"
                                        " intention: %s" % intention)

    def schedule(self, nodes):
        """Schedules the provided nodes for *future* completion.

        This method should schedule a future for each node provided and return
        a set of those futures to be waited on (or used for other similar
        purposes). It should also return any failure objects that represented
        scheduling failures that may have occurred during this scheduling
        process.
        """
        futures = set()
        for node in nodes:
            try:
                futures.add(self._schedule_node(node))
            except Exception:
                # Immediately stop scheduling future work so that we can
                # exit execution early (rather than later) if a single task
                # fails to schedule correctly.
                return (futures, [failure.Failure()])
        return (futures, [])
