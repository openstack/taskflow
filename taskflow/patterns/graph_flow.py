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

from networkx.algorithms import traversal

from taskflow import exceptions as exc
from taskflow import flow
from taskflow.types import graph as gr


class Flow(flow.Flow):
    """Graph flow pattern.

    Contained *flows/tasks* will be executed according to their dependencies
    which will be resolved by using the *flows/tasks* provides and requires
    mappings or by following manually created dependency links.

    From dependencies directed graph is build. If it has edge A -> B, this
    means B depends on A.

    Note: Cyclic dependencies are not allowed.
    """

    def __init__(self, name, retry=None):
        super(Flow, self).__init__(name, retry)
        self._graph = gr.DiGraph()
        self._graph.freeze()

    def link(self, u, v):
        """Link existing node u as a runtime dependency of existing node v."""
        if not self._graph.has_node(u):
            raise ValueError('Item %s not found to link from' % (u))
        if not self._graph.has_node(v):
            raise ValueError('Item %s not found to link to' % (v))
        self._swap(self._link(u, v, manual=True))
        return self

    def _link(self, u, v, graph=None, reason=None, manual=False):
        mutable_graph = True
        if graph is None:
            graph = self._graph
            mutable_graph = False
        # NOTE(harlowja): Add an edge to a temporary copy and only if that
        # copy is valid then do we swap with the underlying graph.
        attrs = graph.get_edge_data(u, v)
        if not attrs:
            attrs = {}
        if manual:
            attrs['manual'] = True
        if reason is not None:
            if 'reasons' not in attrs:
                attrs['reasons'] = set()
            attrs['reasons'].add(reason)
        if not mutable_graph:
            graph = gr.DiGraph(graph)
        graph.add_edge(u, v, **attrs)
        return graph

    def _swap(self, graph):
        """Validates the replacement graph and then swaps the underlying graph
        with a frozen version of the replacement graph (this maintains the
        invariant that the underlying graph is immutable).
        """
        if not graph.is_directed_acyclic():
            raise exc.DependencyFailure("No path through the items in the"
                                        " graph produces an ordering that"
                                        " will allow for correct dependency"
                                        " resolution")
        self._graph = graph
        self._graph.freeze()

    def add(self, *items):
        """Adds a given task/tasks/flow/flows to this flow."""
        items = [i for i in items if not self._graph.has_node(i)]
        if not items:
            return self

        requirements = collections.defaultdict(list)
        provided = {}

        def update_requirements(node):
            for value in node.requires:
                requirements[value].append(node)

        for node in self:
            update_requirements(node)
            for value in node.provides:
                provided[value] = node

        if self.retry:
            update_requirements(self.retry)
            provided.update(dict((k, self.retry)
                                 for k in self.retry.provides))

        # NOTE(harlowja): Add items and edges to a temporary copy of the
        # underlying graph and only if that is successful added to do we then
        # swap with the underlying graph.
        tmp_graph = gr.DiGraph(self._graph)
        for item in items:
            tmp_graph.add_node(item)
            update_requirements(item)
            for value in item.provides:
                if value in provided:
                    raise exc.DependencyFailure(
                        "%(item)s provides %(value)s but is already being"
                        " provided by %(flow)s and duplicate producers"
                        " are disallowed"
                        % dict(item=item.name,
                               flow=provided[value].name,
                               value=value))
                if self.retry and value in self.retry.requires:
                    raise exc.DependencyFailure(
                        "Flows retry controller %(retry)s requires %(value)s "
                        "but item %(item)s being added to the flow produces "
                        "that item, this creates a cyclic dependency and is "
                        "disallowed"
                        % dict(item=item.name,
                               retry=self.retry.name,
                               value=value))
                provided[value] = item

            for value in item.requires:
                if value in provided:
                    self._link(provided[value], item,
                               graph=tmp_graph, reason=value)

            for value in item.provides:
                if value in requirements:
                    for node in requirements[value]:
                        self._link(item, node,
                                   graph=tmp_graph, reason=value)

        self._swap(tmp_graph)
        return self

    def _get_subgraph(self):
        """Get the active subgraph of _graph.

        Descendants may override this to make only part of self._graph
        visible.
        """
        return self._graph

    def __len__(self):
        return self._get_subgraph().number_of_nodes()

    def __iter__(self):
        for n in self._get_subgraph().nodes_iter():
            yield n

    def iter_links(self):
        for (u, v, e_data) in self._get_subgraph().edges_iter(data=True):
            yield (u, v, e_data)


class TargetedFlow(Flow):
    """Graph flow with a target.

    Adds possibility to execute a flow up to certain graph node
    (task or subflow).
    """

    def __init__(self, *args, **kwargs):
        super(TargetedFlow, self).__init__(*args, **kwargs)
        self._subgraph = None
        self._target = None

    def set_target(self, target_item):
        """Set target for the flow.

        Any items (tasks or subflows) not needed for the target
        item will not be executed.
        """
        if not self._graph.has_node(target_item):
            raise ValueError('Item %s not found' % target_item)
        self._target = target_item
        self._subgraph = None

    def reset_target(self):
        """Reset target for the flow.

        All items of the flow will be executed.
        """

        self._target = None
        self._subgraph = None

    def add(self, *items):
        """Adds a given task/tasks/flow/flows to this flow."""
        super(TargetedFlow, self).add(*items)
        # reset cached subgraph, in case it was affected
        self._subgraph = None
        return self

    def link(self, u, v):
        """Link existing node u as a runtime dependency of existing node v."""
        super(TargetedFlow, self).link(u, v)
        # reset cached subgraph, in case it was affected
        self._subgraph = None
        return self

    def _get_subgraph(self):
        if self._subgraph is not None:
            return self._subgraph
        if self._target is None:
            return self._graph
        nodes = [self._target]
        nodes.extend(dst for _src, dst in
                     traversal.dfs_edges(self._graph.reverse(), self._target))
        self._subgraph = self._graph.subgraph(nodes)
        self._subgraph.freeze()
        return self._subgraph
