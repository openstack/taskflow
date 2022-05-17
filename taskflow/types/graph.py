# -*- coding: utf-8 -*-

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
import io
import os

import networkx as nx
from networkx.drawing import nx_pydot


def _common_format(g, edge_notation):
    lines = []
    lines.append("Name: %s" % g.name)
    lines.append("Type: %s" % type(g).__name__)
    lines.append("Frozen: %s" % nx.is_frozen(g))
    lines.append("Density: %0.3f" % nx.density(g))
    lines.append("Nodes: %s" % g.number_of_nodes())
    for n, n_data in g.nodes(data=True):
        if n_data:
            lines.append("  - %s (%s)" % (n, n_data))
        else:
            lines.append("  - %s" % n)
    lines.append("Edges: %s" % g.number_of_edges())
    for (u, v, e_data) in g.edges(data=True):
        if e_data:
            lines.append("  %s %s %s (%s)" % (u, edge_notation, v, e_data))
        else:
            lines.append("  %s %s %s" % (u, edge_notation, v))
    return lines


class Graph(nx.Graph):
    """A graph subclass with useful utility functions."""

    def __init__(self, incoming_graph_data=None, name=''):
        super(Graph, self).__init__(incoming_graph_data=incoming_graph_data,
                                    name=name)
        self.frozen = False

    def freeze(self):
        """Freezes the graph so that no more mutations can occur."""
        if not self.frozen:
            nx.freeze(self)
        return self

    def export_to_dot(self):
        """Exports the graph to a dot format (requires pydot library)."""
        return nx_pydot.to_pydot(self).to_string()

    def pformat(self):
        """Pretty formats your graph into a string."""
        return os.linesep.join(_common_format(self, "<->"))

    def add_edge(self, u, v, attr_dict=None, **attr):
        """Add an edge between u and v."""
        if attr_dict is not None:
            return super(Graph, self).add_edge(u, v, **attr_dict)
        return super(Graph, self).add_edge(u, v, **attr)

    def add_node(self, n, attr_dict=None, **attr):
        """Add a single node n and update node attributes."""
        if attr_dict is not None:
            return super(Graph, self).add_node(n, **attr_dict)
        return super(Graph, self).add_node(n, **attr)

    def fresh_copy(self):
        """Return a fresh copy graph with the same data structure.

        A fresh copy has no nodes, edges or graph attributes. It is
        the same data structure as the current graph. This method is
        typically used to create an empty version of the graph.
        """
        return Graph()


class DiGraph(nx.DiGraph):
    """A directed graph subclass with useful utility functions."""

    def __init__(self, incoming_graph_data=None, name=''):
        super(DiGraph, self).__init__(incoming_graph_data=incoming_graph_data,
                                      name=name)
        self.frozen = False

    def freeze(self):
        """Freezes the graph so that no more mutations can occur."""
        if not self.frozen:
            nx.freeze(self)
        return self

    def get_edge_data(self, u, v, default=None):
        """Returns a *copy* of the edge attribute dictionary between (u, v).

        NOTE(harlowja): this differs from the networkx get_edge_data() as that
        function does not return a copy (but returns a reference to the actual
        edge data).
        """
        try:
            return dict(self.adj[u][v])
        except KeyError:
            return default

    def topological_sort(self):
        """Return a list of nodes in this graph in topological sort order."""
        return nx.topological_sort(self)

    def pformat(self):
        """Pretty formats your graph into a string.

        This pretty formatted string representation includes many useful
        details about your graph, including; name, type, frozeness, node count,
        nodes, edge count, edges, graph density and graph cycles (if any).
        """
        lines = _common_format(self, "->")
        cycles = list(nx.cycles.recursive_simple_cycles(self))
        lines.append("Cycles: %s" % len(cycles))
        for cycle in cycles:
            buf = io.StringIO()
            buf.write("%s" % (cycle[0]))
            for i in range(1, len(cycle)):
                buf.write(" --> %s" % (cycle[i]))
            buf.write(" --> %s" % (cycle[0]))
            lines.append("  %s" % buf.getvalue())
        return os.linesep.join(lines)

    def export_to_dot(self):
        """Exports the graph to a dot format (requires pydot library)."""
        return nx_pydot.to_pydot(self).to_string()

    def is_directed_acyclic(self):
        """Returns if this graph is a DAG or not."""
        return nx.is_directed_acyclic_graph(self)

    def no_successors_iter(self):
        """Returns an iterator for all nodes with no successors."""
        for n in self.nodes:
            if not len(list(self.successors(n))):
                yield n

    def no_predecessors_iter(self):
        """Returns an iterator for all nodes with no predecessors."""
        for n in self.nodes:
            if not len(list(self.predecessors(n))):
                yield n

    def bfs_predecessors_iter(self, n):
        """Iterates breadth first over *all* predecessors of a given node.

        This will go through the nodes predecessors, then the predecessor nodes
        predecessors and so on until no more predecessors are found.

        NOTE(harlowja): predecessor cycles (if they exist) will not be iterated
        over more than once (this prevents infinite iteration).
        """
        visited = set([n])
        queue = collections.deque(self.predecessors(n))
        while queue:
            pred = queue.popleft()
            if pred not in visited:
                yield pred
                visited.add(pred)
                for pred_pred in self.predecessors(pred):
                    if pred_pred not in visited:
                        queue.append(pred_pred)

    def add_edge(self, u, v, attr_dict=None, **attr):
        """Add an edge between u and v."""
        if attr_dict is not None:
            return super(DiGraph, self).add_edge(u, v, **attr_dict)
        return super(DiGraph, self).add_edge(u, v, **attr)

    def add_node(self, n, attr_dict=None, **attr):
        """Add a single node n and update node attributes."""
        if attr_dict is not None:
            return super(DiGraph, self).add_node(n, **attr_dict)
        return super(DiGraph, self).add_node(n, **attr)

    def fresh_copy(self):
        """Return a fresh copy graph with the same data structure.

        A fresh copy has no nodes, edges or graph attributes. It is
        the same data structure as the current graph. This method is
        typically used to create an empty version of the graph.
        """
        return DiGraph()


class OrderedDiGraph(DiGraph):
    """A directed graph subclass with useful utility functions.

    This derivative retains node, edge, insertion and iteration
    ordering (so that the iteration order matches the insertion
    order).
    """
    node_dict_factory = collections.OrderedDict
    adjlist_outer_dict_factory = collections.OrderedDict
    adjlist_inner_dict_factory = collections.OrderedDict
    edge_attr_dict_factory = collections.OrderedDict

    def fresh_copy(self):
        """Return a fresh copy graph with the same data structure.

        A fresh copy has no nodes, edges or graph attributes. It is
        the same data structure as the current graph. This method is
        typically used to create an empty version of the graph.
        """
        return OrderedDiGraph()


class OrderedGraph(Graph):
    """A graph subclass with useful utility functions.

    This derivative retains node, edge, insertion and iteration
    ordering (so that the iteration order matches the insertion
    order).
    """
    node_dict_factory = collections.OrderedDict
    adjlist_outer_dict_factory = collections.OrderedDict
    adjlist_inner_dict_factory = collections.OrderedDict
    edge_attr_dict_factory = collections.OrderedDict

    def fresh_copy(self):
        """Return a fresh copy graph with the same data structure.

        A fresh copy has no nodes, edges or graph attributes. It is
        the same data structure as the current graph. This method is
        typically used to create an empty version of the graph.
        """
        return OrderedGraph()


def merge_graphs(graph, *graphs, **kwargs):
    """Merges a bunch of graphs into a new graph.

    If no additional graphs are provided the first graph is
    returned unmodified otherwise the merged graph is returned.
    """
    tmp_graph = graph
    allow_overlaps = kwargs.get('allow_overlaps', False)
    overlap_detector = kwargs.get('overlap_detector')
    if overlap_detector is not None and not callable(overlap_detector):
        raise ValueError("Overlap detection callback expected to be callable")
    elif overlap_detector is None:
        overlap_detector = (lambda to_graph, from_graph:
                            len(to_graph.subgraph(from_graph.nodes)))
    for g in graphs:
        # This should ensure that the nodes to be merged do not already exist
        # in the graph that is to be merged into. This could be problematic if
        # there are duplicates.
        if not allow_overlaps:
            # Attempt to induce a subgraph using the to be merged graphs nodes
            # and see if any graph results.
            overlaps = overlap_detector(graph, g)
            if overlaps:
                raise ValueError("Can not merge graph %s into %s since there "
                                 "are %s overlapping nodes (and we do not "
                                 "support merging nodes)" % (g, graph,
                                                             overlaps))
        graph = nx.algorithms.compose(graph, g)
    # Keep the first graphs name.
    if graphs:
        graph.name = tmp_graph.name
    return graph
