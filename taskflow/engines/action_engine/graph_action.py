# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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
import logging
import threading

from concurrent import futures

from taskflow.engines.action_engine import base_action as base
from taskflow import states as st
from taskflow.utils import misc

LOG = logging.getLogger(__name__)


class GraphAction(base.Action):

    def __init__(self, graph):
        self._graph = graph
        self._action_mapping = {}

    @property
    def graph(self):
        return self._graph

    def add(self, node, action):
        self._action_mapping[node] = action

    def _succ(self, node):
        return self._graph.successors(node)

    def _pred(self, node):
        return self._graph.predecessors(node)

    def _resolve_dependencies(self, node, deps_counter, revert=False):
        to_execute = []
        nodes = self._pred(node) if revert else self._succ(node)
        for next_node in nodes:
            deps_counter[next_node] -= 1
            if not deps_counter[next_node]:
                to_execute.append(next_node)
        return to_execute

    def _browse_nodes_to_execute(self, deps_counter):
        to_execute = []
        for node, deps in deps_counter.items():
            if not deps:
                to_execute.append(node)
        return to_execute

    def _get_nodes_dependencies_count(self, revert=False):
        deps_counter = {}
        for node in self._graph.nodes_iter():
            nodes = self._succ(node) if revert else self._pred(node)
            deps_counter[node] = len(nodes)
        return deps_counter


class SequentialGraphAction(GraphAction):

    def execute(self, engine):
        deps_counter = self._get_nodes_dependencies_count()
        to_execute = self._browse_nodes_to_execute(deps_counter)

        while to_execute and engine.is_running:
            node = to_execute.pop()
            action = self._action_mapping[node]
            action.execute(engine)  # raises on failure
            to_execute += self._resolve_dependencies(node, deps_counter)

        if to_execute:
            return st.SUSPENDED
        return st.SUCCESS

    def revert(self, engine):
        deps_counter = self._get_nodes_dependencies_count(True)
        to_revert = self._browse_nodes_to_execute(deps_counter)

        while to_revert and engine.is_reverting:
            node = to_revert.pop()
            action = self._action_mapping[node]
            action.revert(engine)  # raises on failure
            to_revert += self._resolve_dependencies(node, deps_counter, True)

        if to_revert:
            return st.SUSPENDED
        return st.REVERTED


class ParallelGraphAction(SequentialGraphAction):
    def execute(self, engine):
        """This action executes the provided graph in parallel by selecting
        nodes which can run (those which have there dependencies satisfied
        or those with no dependencies) and submitting them to the executor
        to be ran, and then after running this process will be repeated until
        no more nodes can be ran (or a failure has a occured and all nodes
        were stopped from further running).
        """
        # A deque is a thread safe push/pop/popleft/append implementation
        all_futures = collections.deque()
        executor = engine.executor
        has_failed = threading.Event()
        deps_lock = threading.RLock()
        deps_counter = self._get_nodes_dependencies_count()
        was_suspended = threading.Event()

        def submit_followups(node):
            # Mutating the deps_counter isn't thread safe.
            with deps_lock:
                to_execute = self._resolve_dependencies(node, deps_counter)
            submit_count = 0
            for n in to_execute:
                try:
                    all_futures.append(executor.submit(run_node, n))
                    submit_count += 1
                except RuntimeError:
                    # Someone shutdown the executor while we are still
                    # using it, get out as quickly as we can...
                    has_failed.set()
                    break
            return submit_count

        def run_node(node):
            if has_failed.is_set():
                # Someone failed, don't even bother running.
                return
            action = self._action_mapping[node]
            try:
                if engine.is_running:
                    action.execute(engine)
                else:
                    was_suspended.set()
                    return
            except Exception:
                # Make sure others don't continue working (although they may
                # be already actively working, but u can't stop that anyway).
                has_failed.set()
                raise
            if has_failed.is_set():
                # Someone else failed, don't even bother submitting any
                # followup jobs.
                return
            # NOTE(harlowja): the future itself will not return until after it
            # submits followup tasks, this keeps the parent thread waiting for
            # more results since the all_futures deque will not be empty until
            # everyone stops submitting followups.
            submitted = submit_followups(node)
            LOG.debug("After running %s, %s followup actions were submitted",
                      node, submitted)

        # Nothing to execute in the first place
        if not deps_counter:
            return st.SUCCESS

        # Ensure that we obtain the lock just in-case the functions submitted
        # immediately themselves start submitting there own jobs (which could
        # happen if they are very quick).
        with deps_lock:
            to_execute = self._browse_nodes_to_execute(deps_counter)
            for n in to_execute:
                try:
                    all_futures.append(executor.submit(run_node, n))
                except RuntimeError:
                    # Someone shutdown the executor while we are still using
                    # it, get out as quickly as we can....
                    break

        # Keep on continuing to consume the futures until there are no more
        # futures to consume so that we can get there failures. Notice that
        # results are not captured, as results of tasks go into storage and
        # do not get returned here.
        failures = []
        while len(all_futures):
            # Take in FIFO order, not in LIFO order.
            f = all_futures.popleft()
            try:
                f.result()
            except futures.CancelledError:
                # TODO(harlowja): can we use the cancellation feature to
                # actually achieve cancellation in taskflow??
                pass
            except Exception:
                failures.append(misc.Failure())
        misc.Failure.reraise_if_any(failures)
        if was_suspended.is_set():
            return st.SUSPENDED
        else:
            return st.SUCCESS
