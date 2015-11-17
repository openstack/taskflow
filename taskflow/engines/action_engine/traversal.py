# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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
import enum

from taskflow.engines.action_engine import compiler as co


class Direction(enum.Enum):
    """Traversal direction enum."""

    #: Go through successors.
    FORWARD = 1

    #: Go through predecessors.
    BACKWARD = 2


def _extract_connectors(execution_graph, starting_node, direction,
                        through_flows=True, through_retries=True,
                        through_tasks=True):
    if direction == Direction.FORWARD:
        connected_iter = execution_graph.successors_iter
    else:
        connected_iter = execution_graph.predecessors_iter
    connected_to_functors = {}
    if through_flows:
        connected_to_functors[co.FLOW] = connected_iter
        connected_to_functors[co.FLOW_END] = connected_iter
    if through_retries:
        connected_to_functors[co.RETRY] = connected_iter
    if through_tasks:
        connected_to_functors[co.TASK] = connected_iter
    return connected_iter(starting_node), connected_to_functors


def breadth_first_iterate(execution_graph, starting_node, direction,
                          through_flows=True, through_retries=True,
                          through_tasks=True):
    """Iterates connected nodes in execution graph (from starting node).

    Does so in a breadth first manner.

    Jumps over nodes with ``noop`` attribute (does not yield them back).
    """
    initial_nodes_iter, connected_to_functors = _extract_connectors(
        execution_graph, starting_node, direction,
        through_flows=through_flows, through_retries=through_retries,
        through_tasks=through_tasks)
    q = collections.deque(initial_nodes_iter)
    while q:
        node = q.popleft()
        node_attrs = execution_graph.node[node]
        if not node_attrs.get('noop'):
            yield node
        try:
            node_kind = node_attrs['kind']
            connected_to_functor = connected_to_functors[node_kind]
        except KeyError:
            pass
        else:
            q.extend(connected_to_functor(node))


def depth_first_iterate(execution_graph, starting_node, direction,
                        through_flows=True, through_retries=True,
                        through_tasks=True):
    """Iterates connected nodes in execution graph (from starting node).

    Does so in a depth first manner.

    Jumps over nodes with ``noop`` attribute (does not yield them back).
    """
    initial_nodes_iter, connected_to_functors = _extract_connectors(
        execution_graph, starting_node, direction,
        through_flows=through_flows, through_retries=through_retries,
        through_tasks=through_tasks)
    stack = list(initial_nodes_iter)
    while stack:
        node = stack.pop()
        node_attrs = execution_graph.node[node]
        if not node_attrs.get('noop'):
            yield node
        try:
            node_kind = node_attrs['kind']
            connected_to_functor = connected_to_functors[node_kind]
        except KeyError:
            pass
        else:
            stack.extend(connected_to_functor(node))


def depth_first_reverse_iterate(node, start_from_idx=-1):
    """Iterates connected (in reverse) **tree** nodes (from starting node).

    Jumps through nodes with ``noop`` attribute (does not yield them back).
    """
    # Always go left to right, since right to left is the pattern order
    # and we want to go backwards and not forwards through that ordering...
    if start_from_idx == -1:
        # All of them...
        children_iter = node.reverse_iter()
    else:
        children_iter = reversed(node[0:start_from_idx])
    for child in children_iter:
        if child.metadata.get('noop'):
            # Jump through these...
            for grand_child in child.dfs_iter(right_to_left=False):
                if grand_child.metadata['kind'] in co.ATOMS:
                    yield grand_child.item
        else:
            yield child.item
