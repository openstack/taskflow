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

import logging

from taskflow import states as st
from taskflow.utils import misc

LOG = logging.getLogger(__name__)


class GraphAction(object):

    def __init__(self, graph):
        self._graph = graph

    @property
    def graph(self):
        return self._graph

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


_WAITING_TIMEOUT = 60  # in seconds


class FutureGraphAction(GraphAction):
    """Graph action build around futures returned by task action.

    This graph action schedules all task it can for execution and than
    waits on returned futures. If task executor is able to execute tasks
    in parallel, this enables parallel flow run and reversion.
    """

    def execute(self, engine):
        was_suspended = self._run(engine, lambda: engine.is_running,
                                  engine.task_action.schedule_execution,
                                  engine.task_action.complete_execution,
                                  revert=False)

        return st.SUSPENDED if was_suspended else st.SUCCESS

    def revert(self, engine):
        was_suspended = self._run(engine, lambda: engine.is_reverting,
                                  engine.task_action.schedule_reversion,
                                  engine.task_action.complete_reversion,
                                  revert=True)
        return st.SUSPENDED if was_suspended else st.REVERTED

    def _run(self, engine, running, schedule_node, complete_node, revert):
        deps_counter = self._get_nodes_dependencies_count(revert)
        not_done = []

        def schedule(nodes):
            for node in nodes:
                future = schedule_node(node)
                if future is not None:
                    not_done.append(future)
                else:
                    schedule(self._resolve_dependencies(
                        node, deps_counter, revert))

        schedule(self._browse_nodes_to_execute(deps_counter))
        failures = []

        was_suspended = False
        while not_done:
            # NOTE(imelnikov): if timeout occurs before any of futures
            # completes, done list will be empty and we'll just go
            # for next iteration
            done, not_done = engine.task_action.wait_for_any(
                not_done, _WAITING_TIMEOUT)

            not_done = list(not_done)
            next_nodes = []
            for future in done:
                node, _event, result = future.result()
                complete_node(node, result)
                if isinstance(result, misc.Failure):
                    failures.append(result)
                else:
                    next_nodes.extend(self._resolve_dependencies(
                        node, deps_counter, revert))

            if next_nodes:
                if running() and not failures:
                    schedule(next_nodes)
                else:
                    # NOTE(imelnikov): engine stopped while there were
                    # still some tasks to do, so we either failed
                    # or were suspended
                    was_suspended = True

        misc.Failure.reraise_if_any(failures)
        return was_suspended
