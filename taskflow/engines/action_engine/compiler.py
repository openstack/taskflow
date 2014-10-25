# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

from taskflow import exceptions as exc
from taskflow import flow
from taskflow import retry
from taskflow import task
from taskflow.types import graph as gr
from taskflow.types import tree as tr
from taskflow.utils import lock_utils
from taskflow.utils import misc

LOG = logging.getLogger(__name__)

_RETRY_EDGE_DATA = {
    flow.LINK_RETRY: True,
}
_EDGE_INVARIANTS = (flow.LINK_INVARIANT, flow.LINK_MANUAL, flow.LINK_RETRY)


class Compilation(object):
    """The result of a compilers compile() is this *immutable* object."""

    def __init__(self, execution_graph, hierarchy):
        self._execution_graph = execution_graph
        self._hierarchy = hierarchy

    @property
    def execution_graph(self):
        return self._execution_graph

    @property
    def hierarchy(self):
        return self._hierarchy


class PatternCompiler(object):
    """Compiles a pattern (or task) into a compilation unit."""

    def __init__(self, root, freeze=True):
        self._root = root
        self._history = set()
        self._freeze = freeze
        self._lock = threading.Lock()
        self._compilation = None

    def _add_new_edges(self, graph, nodes_from, nodes_to, edge_attrs):
        """Adds new edges from nodes to other nodes in the specified graph.

        It will connect the nodes_from to the nodes_to if an edge currently
        does *not* exist. When an edge is created the provided edge attributes
        will be applied to the new edge between these two nodes.
        """
        nodes_to = list(nodes_to)
        for u in nodes_from:
            for v in nodes_to:
                if not graph.has_edge(u, v):
                    # NOTE(harlowja): give each edge its own attr copy so that
                    # if it's later modified that the same copy isn't modified.
                    graph.add_edge(u, v, attr_dict=edge_attrs.copy())

    def _flatten(self, item, parent):
        functor = self._find_flattener(item, parent)
        self._pre_item_flatten(item)
        graph, node = functor(item, parent)
        self._post_item_flatten(item, graph, node)
        return graph, node

    def _find_flattener(self, item, parent):
        """Locates the flattening function to use to flatten the given item."""
        if isinstance(item, flow.Flow):
            return self._flatten_flow
        elif isinstance(item, task.BaseTask):
            return self._flatten_task
        elif isinstance(item, retry.Retry):
            if parent is None:
                raise TypeError("Retry controller '%s' (%s) must only be used"
                                " as a flow constructor parameter and not as a"
                                " root component" % (item, type(item)))
            else:
                raise TypeError("Retry controller '%s' (%s) must only be used"
                                " as a flow constructor parameter and not as a"
                                " flow added component" % (item, type(item)))
        else:
            raise TypeError("Unknown item '%s' (%s) requested to flatten"
                            % (item, type(item)))

    def _connect_retry(self, retry, graph):
        graph.add_node(retry)

        # All nodes that have no predecessors should depend on this retry.
        nodes_to = [n for n in graph.no_predecessors_iter() if n is not retry]
        self._add_new_edges(graph, [retry], nodes_to, _RETRY_EDGE_DATA)

        # Add association for each node of graph that has no existing retry.
        for n in graph.nodes_iter():
            if n is not retry and flow.LINK_RETRY not in graph.node[n]:
                graph.node[n][flow.LINK_RETRY] = retry

    def _flatten_task(self, task, parent):
        """Flattens a individual task."""
        graph = gr.DiGraph(name=task.name)
        graph.add_node(task)
        node = tr.Node(task)
        if parent is not None:
            parent.add(node)
        return graph, node

    def _flatten_flow(self, flow, parent):
        """Flattens a flow."""
        graph = gr.DiGraph(name=flow.name)
        node = tr.Node(flow)
        if parent is not None:
            parent.add(node)
        if flow.retry is not None:
            node.add(tr.Node(flow.retry))

        # Flatten all nodes into a single subgraph per item (and track origin
        # item to its newly expanded graph).
        subgraphs = {}
        for item in flow:
            subgraph = self._flatten(item, node)[0]
            subgraphs[item] = subgraph
            graph = gr.merge_graphs([graph, subgraph])

        # Reconnect all items edges to their corresponding subgraphs.
        for (u, v, attrs) in flow.iter_links():
            u_g = subgraphs[u]
            v_g = subgraphs[v]
            if any(attrs.get(k) for k in _EDGE_INVARIANTS):
                # Connect nodes with no predecessors in v to nodes with
                # no successors in u (thus maintaining the edge dependency).
                self._add_new_edges(graph,
                                    u_g.no_successors_iter(),
                                    v_g.no_predecessors_iter(),
                                    edge_attrs=attrs)
            else:
                # This is symbol dependency edge, connect corresponding
                # providers and consumers.
                for provider in u_g:
                    for consumer in v_g:
                        reasons = provider.provides & consumer.requires
                        if reasons:
                            graph.add_edge(provider, consumer, reasons=reasons)

        if flow.retry is not None:
            self._connect_retry(flow.retry, graph)
        return graph, node

    def _pre_item_flatten(self, item):
        """Called before a item is flattened; any pre-flattening actions."""
        if item in self._history:
            raise ValueError("Already flattened item '%s' (%s), recursive"
                             " flattening is not supported" % (item,
                                                               type(item)))
        self._history.add(item)

    def _post_item_flatten(self, item, graph, node):
        """Called after a item is flattened; doing post-flattening actions."""

    def _pre_flatten(self):
        """Called before the flattening of the root starts."""
        self._history.clear()

    def _post_flatten(self, graph, node):
        """Called after the flattening of the root finishes successfully."""
        dup_names = misc.get_duplicate_keys(graph.nodes_iter(),
                                            key=lambda node: node.name)
        if dup_names:
            raise exc.Duplicate(
                "Atoms with duplicate names found: %s" % (sorted(dup_names)))
        if graph.number_of_nodes() == 0:
            raise exc.Empty("Root container '%s' (%s) is empty"
                            % (self._root, type(self._root)))
        self._history.clear()
        # NOTE(harlowja): this one can be expensive to calculate (especially
        # the cycle detection), so only do it if we know debugging is enabled
        # and not under all cases.
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Translated '%s'", self._root)
            LOG.debug("Graph:")
            for line in graph.pformat().splitlines():
                # Indent it so that it's slightly offset from the above line.
                LOG.debug("  %s", line)
            LOG.debug("Hierarchy:")
            for line in node.pformat().splitlines():
                # Indent it so that it's slightly offset from the above line.
                LOG.debug("  %s", line)

    @lock_utils.locked
    def compile(self):
        """Compiles the contained item into a compiled equivalent."""
        if self._compilation is None:
            self._pre_flatten()
            graph, node = self._flatten(self._root, None)
            self._post_flatten(graph, node)
            if self._freeze:
                graph.freeze()
                node.freeze()
            self._compilation = Compilation(graph, node)
        return self._compilation
