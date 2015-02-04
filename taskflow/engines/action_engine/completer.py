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

from taskflow.engines.action_engine import executor as ex
from taskflow import retry as retry_atom
from taskflow import states as st
from taskflow import task as task_atom
from taskflow.types import failure


class Completer(object):
    """Completes atoms using actions to complete them."""

    def __init__(self, runtime):
        self._runtime = runtime
        self._analyzer = runtime.analyzer
        self._retry_action = runtime.retry_action
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
            self._runtime.retry_subflow(retry)
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
        if retry is not None:
            # Ask retry controller what to do in case of failure
            action = self._retry_action.on_failure(retry, atom, failure)
            if action == retry_atom.RETRY:
                # Prepare just the surrounding subflow for revert to be later
                # retried...
                self._storage.set_atom_intention(retry.name, st.RETRY)
                self._runtime.reset_subgraph(retry, state=None,
                                             intention=st.REVERT)
            elif action == retry_atom.REVERT:
                # Ask parent checkpoint.
                self._process_atom_failure(retry, failure)
            elif action == retry_atom.REVERT_ALL:
                # Prepare all flow for revert
                self._revert_all()
            else:
                raise ValueError("Unknown atom failure resolution"
                                 " action '%s'" % action)
        else:
            # Prepare all flow for revert
            self._revert_all()

    def _revert_all(self):
        """Attempts to set all nodes to the REVERT intention."""
        self._runtime.reset_nodes(self._analyzer.iterate_all_nodes(),
                                  state=None, intention=st.REVERT)
