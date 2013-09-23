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

import six

import networkx as nx
from networkx import algorithms


def get_edge_attrs(graph, u, v):
    """Gets the dictionary of edge attributes between u->v (or none)."""
    if not graph.has_edge(u, v):
        return None
    return dict(graph.adj[u][v])


def merge_graphs(graphs, allow_overlaps=False):
    if not graphs:
        return None
    graph = graphs[0]
    for g in graphs[1:]:
        # This should ensure that the nodes to be merged do not already exist
        # in the graph that is to be merged into. This could be problematic if
        # there are duplicates.
        if not allow_overlaps:
            # Attempt to induce a subgraph using the to be merged graphs nodes
            # and see if any graph results.
            overlaps = graph.subgraph(g.nodes_iter())
            if len(overlaps):
                raise ValueError("Can not merge graph %s into %s since there "
                                 "are %s overlapping nodes" (g, graph,
                                                             len(overlaps)))
        # Keep the target graphs name.
        name = graph.name
        graph = algorithms.compose(graph, g)
        graph.name = name
    return graph


def get_no_successors(graph):
    """Returns an iterator for all nodes with no successors"""
    for n in graph.nodes_iter():
        if not len(graph.successors(n)):
            yield n


def get_no_predecessors(graph):
    """Returns an iterator for all nodes with no predecessors"""
    for n in graph.nodes_iter():
        if not len(graph.predecessors(n)):
            yield n


def pformat(graph):
    """Pretty formats your graph into a string representation that includes
    details about your graph, including; name, type, frozeness, node count,
    nodes, edge count, edges, graph density and graph cycles (if any).
    """
    lines = []
    lines.append("Name: %s" % graph.name)
    lines.append("Type: %s" % type(graph).__name__)
    lines.append("Frozen: %s" % nx.is_frozen(graph))
    lines.append("Nodes: %s" % graph.number_of_nodes())
    for n in graph.nodes_iter():
        lines.append("  - %s" % n)
    lines.append("Edges: %s" % graph.number_of_edges())
    for (u, v, e_data) in graph.edges_iter(data=True):
        if e_data:
            lines.append("  %s -> %s (%s)" % (u, v, e_data))
        else:
            lines.append("  %s -> %s" % (u, v))
    lines.append("Density: %0.3f" % nx.density(graph))
    cycles = list(nx.cycles.recursive_simple_cycles(graph))
    lines.append("Cycles: %s" % len(cycles))
    for cycle in cycles:
        buf = six.StringIO()
        buf.write(str(cycle[0]))
        for i in range(1, len(cycle)):
            buf.write(" --> %s" % (cycle[i]))
        buf.write(" --> %s" % (cycle[0]))
        lines.append("  %s" % buf.getvalue())
    return "\n".join(lines)


def export_graph_to_dot(graph):
    """Exports the graph to a dot format (requires pydot library)"""
    return nx.to_pydot(graph).to_string()
