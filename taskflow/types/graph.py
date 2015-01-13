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
import os

import networkx as nx
import six


class DiGraph(nx.DiGraph):
    """A directed graph subclass with useful utility functions."""
    def __init__(self, data=None, name=''):
        super(DiGraph, self).__init__(name=name, data=data)
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
        lines = []
        lines.append("Name: %s" % self.name)
        lines.append("Type: %s" % type(self).__name__)
        lines.append("Frozen: %s" % nx.is_frozen(self))
        lines.append("Nodes: %s" % self.number_of_nodes())
        for n in self.nodes_iter():
            lines.append("  - %s" % n)
        lines.append("Edges: %s" % self.number_of_edges())
        for (u, v, e_data) in self.edges_iter(data=True):
            if e_data:
                lines.append("  %s -> %s (%s)" % (u, v, e_data))
            else:
                lines.append("  %s -> %s" % (u, v))
        lines.append("Density: %0.3f" % nx.density(self))
        cycles = list(nx.cycles.recursive_simple_cycles(self))
        lines.append("Cycles: %s" % len(cycles))
        for cycle in cycles:
            buf = six.StringIO()
            buf.write("%s" % (cycle[0]))
            for i in range(1, len(cycle)):
                buf.write(" --> %s" % (cycle[i]))
            buf.write(" --> %s" % (cycle[0]))
            lines.append("  %s" % buf.getvalue())
        return os.linesep.join(lines)

    def export_to_dot(self):
        """Exports the graph to a dot format (requires pydot library)."""
        return nx.to_pydot(self).to_string()

    def is_directed_acyclic(self):
        """Returns if this graph is a DAG or not."""
        return nx.is_directed_acyclic_graph(self)

    def no_successors_iter(self):
        """Returns an iterator for all nodes with no successors."""
        for n in self.nodes_iter():
            if not len(self.successors(n)):
                yield n

    def no_predecessors_iter(self):
        """Returns an iterator for all nodes with no predecessors."""
        for n in self.nodes_iter():
            if not len(self.predecessors(n)):
                yield n

    def bfs_predecessors_iter(self, n):
        """Iterates breadth first over *all* predecessors of a given node.

        This will go through the nodes predecessors, then the predecessor nodes
        predecessors and so on until no more predecessors are found.

        NOTE(harlowja): predecessor cycles (if they exist) will not be iterated
        over more than once (this prevents infinite iteration).
        """
        visited = set([n])
        queue = collections.deque(self.predecessors_iter(n))
        while queue:
            pred = queue.popleft()
            if pred not in visited:
                yield pred
                visited.add(pred)
                for pred_pred in self.predecessors_iter(pred):
                    if pred_pred not in visited:
                        queue.append(pred_pred)


def merge_graphs(graphs, allow_overlaps=False):
    """Merges a bunch of graphs into a single graph."""
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
                                 "are %s overlapping nodes (and we do not "
                                 "support merging nodes)" % (g, graph,
                                                             len(overlaps)))
        # Keep the target graphs name.
        name = graph.name
        graph = nx.algorithms.compose(graph, g)
        graph.name = name
    return graph
