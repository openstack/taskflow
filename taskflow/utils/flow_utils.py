# -*- coding: utf-8 -*-

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

import networkx as nx

from taskflow import exceptions
from taskflow import flow
from taskflow import retry
from taskflow import task
from taskflow.utils import graph_utils as gu
from taskflow.utils import misc


LOG = logging.getLogger(__name__)


RETRY_EDGE_DATA = {
    'retry': True,
}


class Flattener(object):
    def __init__(self, root, freeze=True):
        self._root = root
        self._graph = None
        self._history = set()
        self._freeze = bool(freeze)

    def _add_new_edges(self, graph, nodes_from, nodes_to, edge_attrs):
        """Adds new edges from nodes to other nodes in the specified graph,
        with the following edge attributes (defaulting to the class provided
        edge_data if None), if the edge does not already exist.
        """
        nodes_to = list(nodes_to)
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
        if isinstance(item, flow.Flow):
            return self._flatten_flow
        elif isinstance(item, task.BaseTask):
            return self._flatten_task
        elif isinstance(item, retry.Retry):
            raise TypeError("Retry controller %s (%s) is used not as a flow "
                            "parameter" % (item, type(item)))
        else:
            return None

    def _connect_retry(self, retry, graph):
        graph.add_node(retry)

        # All graph nodes that have no predecessors should depend on its retry
        nodes_to = [n for n in gu.get_no_predecessors(graph) if n != retry]
        self._add_new_edges(graph, [retry], nodes_to, RETRY_EDGE_DATA)

        # Add link to retry for each node of subgraph that hasn't
        # a parent retry
        for n in graph.nodes_iter():
            if n != retry and 'retry' not in graph.node[n]:
                graph.node[n]['retry'] = retry

    def _flatten_task(self, task):
        """Flattens a individual task."""
        graph = nx.DiGraph(name=task.name)
        graph.add_node(task)
        return graph

    def _flatten_flow(self, flow):
        """Flattens a graph flow."""
        graph = nx.DiGraph(name=flow.name)
        # Flatten all nodes into a single subgraph per node.
        subgraph_map = {}
        for item in flow:
            subgraph = self._flatten(item)
            subgraph_map[item] = subgraph
            graph = gu.merge_graphs([graph, subgraph])

        # Reconnect all node edges to their corresponding subgraphs.
        for (u, v, attrs) in flow.iter_links():
            if any(attrs.get(k) for k in ('invariant', 'manual', 'retry')):
                # Connect nodes with no predecessors in v to nodes with
                # no successors in u (thus maintaining the edge dependency).
                self._add_new_edges(graph,
                                    gu.get_no_successors(subgraph_map[u]),
                                    gu.get_no_predecessors(subgraph_map[v]),
                                    edge_attrs=attrs)
            else:
                # This is dependency-only edge, connect corresponding
                # providers and consumers.
                for provider in subgraph_map[u]:
                    for consumer in subgraph_map[v]:
                        reasons = provider.provides & consumer.requires
                        if reasons:
                            graph.add_edge(provider, consumer, reasons=reasons)

        if flow.retry is not None:
            self._connect_retry(flow.retry, graph)
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
            raise exceptions.Duplicate("Tasks with duplicate names "
                                       "found: %s" % (dup_names))
        self._history.clear()

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
