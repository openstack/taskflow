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

import logging
import threading

import networkx as nx

from taskflow import exceptions
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow import task
from taskflow.utils import graph_utils as gu
from taskflow.utils import lock_utils as lu
from taskflow.utils import misc

LOG = logging.getLogger(__name__)

# Use the 'flatten' attribute as the need to add an edge here, which is useful
# for doing later analysis of the edges (to determine why the edges were
# created).
FLATTEN_EDGE_DATA = {
    'flatten': True,
}


class Flattener(object):
    def __init__(self, root, freeze=True):
        self._root = root
        self._graph = None
        self._history = set()
        self._freeze = bool(freeze)
        self._lock = threading.Lock()
        self._edge_data = FLATTEN_EDGE_DATA.copy()

    def _add_new_edges(self, graph, nodes_from, nodes_to, edge_attrs=None):
        """Adds new edges from nodes to other nodes in the specified graph,
        with the following edge attributes (defaulting to the class provided
        edge_data if None), if the edge does not already exist.
        """
        if edge_attrs is None:
            edge_attrs = self._edge_data
        else:
            edge_attrs = edge_attrs.copy()
            edge_attrs.update(self._edge_data)
        for u in nodes_from:
            for v in nodes_to:
                if not graph.has_edge(u, v):
                    # NOTE(harlowja): give each edge its own attr copy so that
                    # if it's later modified that the same copy isn't modified.
                    graph.add_edge(u, v, attr_dict=edge_attrs.copy())

    def _flatten(self, item):
        functor = self._find_flattener(item)
        if not functor:
            raise TypeError("Unknown type requested to flatten: %s (%s)"
                            % (item, type(item)))
        self._pre_item_flatten(item)
        graph = functor(item)
        self._post_item_flatten(item, graph)
        return graph

    def _find_flattener(self, item):
        """Locates the flattening function to use to flatten the given item."""
        if isinstance(item, lf.Flow):
            return self._flatten_linear
        elif isinstance(item, uf.Flow):
            return self._flatten_unordered
        elif isinstance(item, gf.Flow):
            return self._flatten_graph
        elif isinstance(item, task.BaseTask):
            return self._flatten_task
        else:
            return None

    def _flatten_linear(self, flow):
        """Flattens a linear flow."""
        graph = nx.DiGraph(name=flow.name)
        previous_nodes = []
        for item in flow:
            subgraph = self._flatten(item)
            graph = gu.merge_graphs([graph, subgraph])
            # Find nodes that have no predecessor, make them have a predecessor
            # of the previous nodes so that the linearity ordering is
            # maintained. Find the ones with no successors and use this list
            # to connect the next subgraph (if any).
            self._add_new_edges(graph,
                                previous_nodes,
                                list(gu.get_no_predecessors(subgraph)))
            # There should always be someone without successors, otherwise we
            # have a cycle A -> B -> A situation, which should not be possible.
            previous_nodes = list(gu.get_no_successors(subgraph))
        return graph

    def _flatten_unordered(self, flow):
        """Flattens a unordered flow."""
        graph = nx.DiGraph(name=flow.name)
        for item in flow:
            # NOTE(harlowja): we do *not* connect the graphs together, this
            # retains that each item (translated to subgraph) is disconnected
            # from each other which will result in unordered execution while
            # running.
            graph = gu.merge_graphs([graph, self._flatten(item)])
        return graph

    def _flatten_task(self, task):
        """Flattens a individual task."""
        graph = nx.DiGraph(name=task.name)
        graph.add_node(task)
        return graph

    def _flatten_graph(self, flow):
        """Flattens a graph flow."""
        graph = nx.DiGraph(name=flow.name)
        # Flatten all nodes into a single subgraph per node.
        subgraph_map = {}
        for item in flow:
            subgraph = self._flatten(item)
            subgraph_map[item] = subgraph
            graph = gu.merge_graphs([graph, subgraph])
        # Reconnect all node edges to there corresponding subgraphs.
        for (u, v) in flow.graph.edges_iter():
            # Retain and update the original edge attributes.
            u_v_attrs = gu.get_edge_attrs(flow.graph, u, v)
            # Connect the ones with no predecessors in v to the ones with no
            # successors in u (thus maintaining the edge dependency).
            self._add_new_edges(graph,
                                list(gu.get_no_successors(subgraph_map[u])),
                                list(gu.get_no_predecessors(subgraph_map[v])),
                                edge_attrs=u_v_attrs)
        return graph

    def _pre_item_flatten(self, item):
        """Called before a item is flattened; any pre-flattening actions."""
        if id(item) in self._history:
            raise ValueError("Already flattened item: %s (%s), recursive"
                             " flattening not supported" % (item, id(item)))
        LOG.debug("Starting to flatten '%s'", item)
        self._history.add(id(item))

    def _post_item_flatten(self, item, graph):
        """Called before a item is flattened; any post-flattening actions."""
        LOG.debug("Finished flattening '%s'", item)
        # NOTE(harlowja): this one can be expensive to calculate (especially
        # the cycle detection), so only do it if we know debugging is enabled
        # and not under all cases.
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Translated '%s' into a graph:", item)
            for line in gu.pformat(graph).splitlines():
                # Indent it so that it's slightly offset from the above line.
                LOG.debug(" %s", line)

    def _pre_flatten(self):
        """Called before the flattening of the item starts."""
        self._history.clear()

    def _post_flatten(self, graph):
        """Called after the flattening of the item finishes successfully."""
        dup_names = misc.get_duplicate_keys(graph.nodes_iter(),
                                            key=lambda node: node.name)
        if dup_names:
            dup_names = ', '.join(sorted(dup_names))
            raise exceptions.InvariantViolation("Tasks with duplicate names "
                                                "found: %s" % (dup_names))
        self._history.clear()

    @lu.locked
    def flatten(self):
        """Flattens a item (a task or flow) into a single execution graph."""
        if self._graph is not None:
            return self._graph
        self._pre_flatten()
        graph = self._flatten(self._root)
        self._post_flatten(graph)
        if self._freeze:
            self._graph = nx.freeze(graph)
        else:
            self._graph = graph
        return self._graph


def flatten(item, freeze=True):
    """Flattens a item (a task or flow) into a single execution graph."""
    return Flattener(item, freeze=freeze).flatten()
