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

from taskflow import exceptions as exc
from taskflow import flow
from taskflow import retry
from taskflow import task
from taskflow.types import graph as gr
from taskflow.utils import misc

LOG = logging.getLogger(__name__)


class Compilation(object):
    """The result of a compilers compile() is this *immutable* object.

    For now it is just a execution graph but in the future it will grow to
    include more methods & properties that help the various runtime units
    execute in a more optimal & featureful manner.
    """
    def __init__(self, execution_graph):
        self._execution_graph = execution_graph

    @property
    def execution_graph(self):
        return self._execution_graph


class PatternCompiler(object):
    """Compiles patterns & atoms (potentially nested) into an compilation
    unit with a *logically* equivalent directed acyclic graph representation.

    NOTE(harlowja): during this pattern translation process any nested flows
    will be converted into there equivalent subgraphs. This currently implies
    that contained atoms in those nested flows, post-translation will no longer
    be associated with there previously containing flow but instead will lose
    this identity and what will remain is the logical constraints that there
    contained flow mandated. In the future this may be changed so that this
    association is not lost via the compilation process (since it is sometime
    useful to retain part of this relationship).
    """
    def compile(self, root):
        graph = _Flattener(root).flatten()
        if graph.number_of_nodes() == 0:
            # Try to get a name attribute, otherwise just use the object
            # string representation directly if that attribute does not exist.
            name = getattr(root, 'name', root)
            raise exc.Empty("Root container '%s' (%s) is empty."
                            % (name, type(root)))
        return Compilation(graph)


_RETRY_EDGE_DATA = {
    'retry': True,
}


class _Flattener(object):
    """Flattens a root item (task/flow) into a execution graph."""

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
        nodes_to = [n for n in graph.no_predecessors_iter() if n != retry]
        self._add_new_edges(graph, [retry], nodes_to, _RETRY_EDGE_DATA)

        # Add link to retry for each node of subgraph that hasn't
        # a parent retry
        for n in graph.nodes_iter():
            if n != retry and 'retry' not in graph.node[n]:
                graph.node[n]['retry'] = retry

    def _flatten_task(self, task):
        """Flattens a individual task."""
        graph = gr.DiGraph(name=task.name)
        graph.add_node(task)
        return graph

    def _flatten_flow(self, flow):
        """Flattens a graph flow."""
        graph = gr.DiGraph(name=flow.name)

        # Flatten all nodes into a single subgraph per node.
        subgraph_map = {}
        for item in flow:
            subgraph = self._flatten(item)
            subgraph_map[item] = subgraph
            graph = gr.merge_graphs([graph, subgraph])

        # Reconnect all node edges to their corresponding subgraphs.
        for (u, v, attrs) in flow.iter_links():
            u_g = subgraph_map[u]
            v_g = subgraph_map[v]
            if any(attrs.get(k) for k in ('invariant', 'manual', 'retry')):
                # Connect nodes with no predecessors in v to nodes with
                # no successors in u (thus maintaining the edge dependency).
                self._add_new_edges(graph,
                                    u_g.no_successors_iter(),
                                    v_g.no_predecessors_iter(),
                                    edge_attrs=attrs)
            else:
                # This is dependency-only edge, connect corresponding
                # providers and consumers.
                for provider in u_g:
                    for consumer in v_g:
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
        self._history.add(id(item))

    def _post_item_flatten(self, item, graph):
        """Called before a item is flattened; any post-flattening actions."""

    def _pre_flatten(self):
        """Called before the flattening of the item starts."""
        self._history.clear()

    def _post_flatten(self, graph):
        """Called after the flattening of the item finishes successfully."""
        dup_names = misc.get_duplicate_keys(graph.nodes_iter(),
                                            key=lambda node: node.name)
        if dup_names:
            dup_names = ', '.join(sorted(dup_names))
            raise exc.Duplicate("Atoms with duplicate names "
                                "found: %s" % (dup_names))
        self._history.clear()
        # NOTE(harlowja): this one can be expensive to calculate (especially
        # the cycle detection), so only do it if we know debugging is enabled
        # and not under all cases.
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Translated '%s' into a graph:", self._root)
            for line in graph.pformat().splitlines():
                # Indent it so that it's slightly offset from the above line.
                LOG.debug(" %s", line)

    def flatten(self):
        """Flattens a item (a task or flow) into a single execution graph."""
        if self._graph is not None:
            return self._graph
        self._pre_flatten()
        graph = self._flatten(self._root)
        self._post_flatten(graph)
        self._graph = graph
        if self._freeze:
            self._graph.freeze()
        return self._graph
