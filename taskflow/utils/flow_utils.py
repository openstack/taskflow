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

import networkx as nx

from taskflow import exceptions
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow import task
from taskflow.utils import graph_utils as gu
from taskflow.utils import misc


# Use the 'flatten' reason as the need to add an edge here, which is useful for
# doing later analysis of the edges (to determine why the edges were created).
FLATTEN_REASON = 'flatten'
FLATTEN_EDGE_DATA = {
    'reason': FLATTEN_REASON,
}


def _graph_name(flow):
    return "F:%s:%s" % (flow.name, flow.uuid)


def _flatten_linear(flow, flattened):
    graph = nx.DiGraph(name=_graph_name(flow))
    previous_nodes = []
    for f in flow:
        subgraph = _flatten(f, flattened)
        graph = gu.merge_graphs([graph, subgraph])
        # Find nodes that have no predecessor, make them have a predecessor of
        # the previous nodes so that the linearity ordering is maintained. Find
        # the ones with no successors and use this list to connect the next
        # subgraph (if any).
        for n in gu.get_no_predecessors(subgraph):
            graph.add_edges_from(((n2, n, FLATTEN_EDGE_DATA)
                                  for n2 in previous_nodes
                                  if not graph.has_edge(n2, n)))
        # There should always be someone without successors, otherwise we have
        # a cycle A -> B -> A situation, which should not be possible.
        previous_nodes = list(gu.get_no_successors(subgraph))
    return graph


def _flatten_unordered(flow, flattened):
    graph = nx.DiGraph(name=_graph_name(flow))
    for f in flow:
        graph = gu.merge_graphs([graph, _flatten(f, flattened)])
    return graph


def _flatten_task(task):
    graph = nx.DiGraph(name='T:%s' % (task))
    graph.add_node(task)
    return graph


def _flatten_graph(flow, flattened):
    graph = nx.DiGraph(name=_graph_name(flow))
    subgraph_map = {}
    # Flatten all nodes
    for n in flow.graph.nodes_iter():
        subgraph = _flatten(n, flattened)
        subgraph_map[n] = subgraph
        graph = gu.merge_graphs([graph, subgraph])
    # Reconnect all nodes to there corresponding subgraphs
    for (u, v) in flow.graph.edges_iter():
        u_no_succ = list(gu.get_no_successors(subgraph_map[u]))
        # Connect the ones with no predecessors in v to the ones with no
        # successors in u (thus maintaining the edge dependency).
        for n in gu.get_no_predecessors(subgraph_map[v]):
            graph.add_edges_from(((n2, n, FLATTEN_EDGE_DATA)
                                  for n2 in u_no_succ
                                  if not graph.has_edge(n2, n)))
    return graph


def _flatten(item, flattened):
    """Flattens a item (task/flow+subflows) into an execution graph."""
    if item in flattened:
        raise ValueError("Already flattened item: %s" % (item))
    if isinstance(item, lf.Flow):
        f = _flatten_linear(item, flattened)
    elif isinstance(item, uf.Flow):
        f = _flatten_unordered(item, flattened)
    elif isinstance(item, gf.Flow):
        f = _flatten_graph(item, flattened)
    elif isinstance(item, task.BaseTask):
        f = _flatten_task(item)
    else:
        raise TypeError("Unknown item: %r, %s" % (type(item), item))
    flattened.add(item)
    return f


def flatten(item, freeze=True):
    graph = _flatten(item, set())

    dup_names = misc.get_duplicate_keys(graph.nodes_iter(),
                                        key=lambda node: node.name)
    if dup_names:
        raise exceptions.InvariantViolationException(
            "Tasks with duplicate names found: %s"
            % ', '.join(sorted(dup_names)))

    if freeze:
        # Frozen graph can't be modified...
        return nx.freeze(graph)
    return graph
