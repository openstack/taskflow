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

from taskflow.engines.action_engine import base_action as base


class GraphAction(base.Action):

    def __init__(self, graph):
        self._graph = graph
        self._action_mapping = {}

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

        while to_execute:
            node = to_execute.pop()
            action = self._action_mapping[node]
            action.execute(engine)  # raises on failure
            to_execute += self._resolve_dependencies(node, deps_counter)

    def revert(self, engine):
        deps_counter = self._get_nodes_dependencies_count(True)
        to_revert = self._browse_nodes_to_execute(deps_counter)

        while to_revert:
            node = to_revert.pop()
            action = self._action_mapping[node]
            action.revert(engine)  # raises on failure
            to_revert += self._resolve_dependencies(node, deps_counter, True)
