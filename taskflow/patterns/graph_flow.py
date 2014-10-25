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

from taskflow import exceptions as exc
from taskflow import flow
from taskflow.types import graph as gr


def _unsatisfied_requires(node, graph, *additional_provided):
    """Extracts the unsatisified symbol requirements of a single node."""
    requires = set(node.requires)
    if not requires:
        return requires
    for provided in additional_provided:
        requires = requires - provided
        if not requires:
            return requires
    for pred in graph.bfs_predecessors_iter(node):
        requires = requires - pred.provides
        if not requires:
            return requires
    return requires


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
            attrs[flow.LINK_MANUAL] = True
        if reason is not None:
            if flow.LINK_REASONS not in attrs:
                attrs[flow.LINK_REASONS] = set()
            attrs[flow.LINK_REASONS].add(reason)
        if not mutable_graph:
            graph = gr.DiGraph(graph)
        graph.add_edge(u, v, **attrs)
        return graph

    def _swap(self, graph):
        """Validates the replacement graph and then swaps the underlying graph.

        After swapping occurs the underlying graph will be frozen so that the
        immutability invariant is maintained (we may be able to relax this
        constraint in the future since our exposed public api does not allow
        direct access to the underlying graph).
        """
        if not graph.is_directed_acyclic():
            raise exc.DependencyFailure("No path through the items in the"
                                        " graph produces an ordering that"
                                        " will allow for logical"
                                        " edge traversal")
        self._graph = graph.freeze()

    def add(self, *items, **kwargs):
        """Adds a given task/tasks/flow/flows to this flow.

        :param items: items to add to the flow
        :param kwargs: keyword arguments, the two keyword arguments
                       currently processed are:

                        * ``resolve_requires`` a boolean that when true (the
                          default) implies that when items are added their
                          symbol requirements will be matched to existing items
                          and links will be automatically made to those
                          providers. If multiple possible providers exist
                          then a AmbiguousDependency exception will be raised.
                        * ``resolve_existing``, a boolean that when true (the
                          default) implies that on addition of a new item that
                          existing items will have their requirements scanned
                          for symbols that this newly added item can provide.
                          If a match is found a link is automatically created
                          from the newly added item to the requiree.
        """
        items = [i for i in items if not self._graph.has_node(i)]
        if not items:
            return self

        # This syntax will *hopefully* be better in future versions of python.
        #
        # See: http://legacy.python.org/dev/peps/pep-3102/ (python 3.0+)
        resolve_requires = bool(kwargs.get('resolve_requires', True))
        resolve_existing = bool(kwargs.get('resolve_existing', True))

        # Figure out what the existing nodes *still* require and what they
        # provide so we can do this lookup later when inferring.
        required = collections.defaultdict(list)
        provided = collections.defaultdict(list)

        retry_provides = set()
        if self._retry is not None:
            for value in self._retry.requires:
                required[value].append(self._retry)
            for value in self._retry.provides:
                retry_provides.add(value)
                provided[value].append(self._retry)

        for item in self._graph.nodes_iter():
            for value in _unsatisfied_requires(item, self._graph,
                                               retry_provides):
                required[value].append(item)
            for value in item.provides:
                provided[value].append(item)

        # NOTE(harlowja): Add items and edges to a temporary copy of the
        # underlying graph and only if that is successful added to do we then
        # swap with the underlying graph.
        tmp_graph = gr.DiGraph(self._graph)
        for item in items:
            tmp_graph.add_node(item)

            # Try to find a valid provider.
            if resolve_requires:
                for value in _unsatisfied_requires(item, tmp_graph,
                                                   retry_provides):
                    if value in provided:
                        providers = provided[value]
                        if len(providers) > 1:
                            provider_names = [n.name for n in providers]
                            raise exc.AmbiguousDependency(
                                "Resolution error detected when"
                                " adding %(item)s, multiple"
                                " providers %(providers)s found for"
                                " required symbol '%(value)s'"
                                % dict(item=item.name,
                                       providers=sorted(provider_names),
                                       value=value))
                        else:
                            self._link(providers[0], item,
                                       graph=tmp_graph, reason=value)
                    else:
                        required[value].append(item)

            for value in item.provides:
                provided[value].append(item)

            # See if what we provide fulfills any existing requiree.
            if resolve_existing:
                for value in item.provides:
                    if value in required:
                        for requiree in list(required[value]):
                            if requiree is not item:
                                self._link(item, requiree,
                                           graph=tmp_graph, reason=value)
                                required[value].remove(requiree)

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
        for n in self._get_subgraph().topological_sort():
            yield n

    def iter_links(self):
        for (u, v, e_data) in self._get_subgraph().edges_iter(data=True):
            yield (u, v, e_data)

    @property
    def requires(self):
        requires = set()
        retry_provides = set()
        if self._retry is not None:
            requires.update(self._retry.requires)
            retry_provides.update(self._retry.provides)
        g = self._get_subgraph()
        for item in g.nodes_iter():
            requires.update(_unsatisfied_requires(item, g, retry_provides))
        return frozenset(requires)


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
        nodes.extend(self._graph.bfs_predecessors_iter(self._target))
        self._subgraph = self._graph.subgraph(nodes)
        self._subgraph.freeze()
        return self._subgraph
