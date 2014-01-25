# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

import six

from taskflow import states as st


class GraphAnalyzer(object):
    """Analyzes a execution graph to get the next nodes for execution or
    reversion by utilizing the graphs nodes and edge relations and comparing
    the node state against the states stored in storage.
    """

    def __init__(self, graph, storage):
        self._graph = graph
        self._storage = storage

    @property
    def execution_graph(self):
        return self._graph

    def browse_nodes_for_execute(self, node=None):
        """Browse next nodes to execute for given node if specified and
        for whole graph otherwise.
        """
        if node:
            nodes = self._graph.successors(node)
        else:
            nodes = self._graph.nodes_iter()

        available_nodes = []
        for node in nodes:
            if self._is_ready_for_execute(node):
                available_nodes.append(node)
        return available_nodes

    def browse_nodes_for_revert(self, node=None):
        """Browse next nodes to revert for given node if specified and
        for whole graph otherwise.
        """
        if node:
            nodes = self._graph.predecessors(node)
        else:
            nodes = self._graph.nodes_iter()

        available_nodes = []
        for node in nodes:
            if self._is_ready_for_revert(node):
                available_nodes.append(node)
        return available_nodes

    def _is_ready_for_execute(self, task):
        """Checks if task is ready to be executed."""

        state = self._storage.get_task_state(task.name)
        if not st.check_task_transition(state, st.RUNNING):
            return False

        task_names = []
        for prev_task in self._graph.predecessors(task):
            task_names.append(prev_task.name)

        task_states = self._storage.get_tasks_states(task_names)
        return all(state == st.SUCCESS
                   for state in six.itervalues(task_states))

    def _is_ready_for_revert(self, task):
        """Checks if task is ready to be reverted."""

        state = self._storage.get_task_state(task.name)
        if not st.check_task_transition(state, st.REVERTING):
            return False

        task_names = []
        for prev_task in self._graph.successors(task):
            task_names.append(prev_task.name)

        task_states = self._storage.get_tasks_states(task_names)
        return all(state in (st.PENDING, st.REVERTED)
                   for state in six.itervalues(task_states))
