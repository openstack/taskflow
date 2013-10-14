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

import collections

import networkx as nx

from taskflow import exceptions as exc
from taskflow import flow
from taskflow.utils import graph_utils


class Flow(flow.Flow):
    """Graph flow pattern

    Contained *flows/tasks* will be executed according to their dependencies
    which will be resolved by using the *flows/tasks* provides and requires
    mappings or by following manually created dependency links.

    Note: Cyclic dependencies are not allowed.
    """

    def __init__(self, name):
        super(Flow, self).__init__(name)
        self._graph = nx.freeze(nx.DiGraph())

    def _validate(self, graph=None):
        if graph is None:
            graph = self._graph
        # Ensure that there is a valid topological ordering.
        if not nx.is_directed_acyclic_graph(graph):
            raise exc.DependencyFailure("No path through the items in the"
                                        " graph produces an ordering that"
                                        " will allow for correct dependency"
                                        " resolution")

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
        attrs = graph_utils.get_edge_attrs(graph, u, v)
        if not attrs:
            attrs = {}
        if manual:
            attrs['manual'] = True
        if reason is not None:
            if 'reasons' not in attrs:
                attrs['reasons'] = set()
            attrs['reasons'].add(reason)
        if not mutable_graph:
            graph = nx.DiGraph(graph)
        graph.add_edge(u, v, **attrs)
        return graph

    def _swap(self, replacement_graph):
        """Validates the replacement graph and then swaps the underlying graph
        with a frozen version of the replacement graph (this maintains the
        invariant that the underlying graph is immutable).
        """
        self._validate(replacement_graph)
        self._graph = nx.freeze(replacement_graph)

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

        # NOTE(harlowja): Add items and edges to a temporary copy of the
        # underlying graph and only if that is successful added to do we then
        # swap with the underlying graph.
        tmp_graph = nx.DiGraph(self._graph)
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

    def __len__(self):
        return self._graph.number_of_nodes()

    def __iter__(self):
        for n in self._graph.nodes_iter():
            yield n

    @property
    def provides(self):
        provides = set()
        for subflow in self:
            provides.update(subflow.provides)
        return provides

    @property
    def requires(self):
        requires = set()
        for subflow in self:
            requires.update(subflow.requires)
        return requires - self.provides

    @property
    def graph(self):
        return self._graph
