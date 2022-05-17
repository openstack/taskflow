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
import functools

from taskflow import deciders as de
from taskflow import exceptions as exc
from taskflow import flow
from taskflow.types import graph as gr


def _unsatisfied_requires(node, graph, *additional_provided):
    requires = set(node.requires)
    if not requires:
        return requires
    for provided in additional_provided:
        # This is using the difference() method vs the -
        # operator since the latter doesn't work with frozen
        # or regular sets (when used in combination with ordered
        # sets).
        #
        # If this is not done the following happens...
        #
        # TypeError: unsupported operand type(s)
        # for -: 'set' and 'OrderedSet'
        requires = requires.difference(provided)
        if not requires:
            return requires
    for pred in graph.bfs_predecessors_iter(node):
        requires = requires.difference(pred.provides)
        if not requires:
            return requires
    return requires


class Flow(flow.Flow):
    """Graph flow pattern.

    Contained *flows/tasks* will be executed according to their dependencies
    which will be resolved by using the *flows/tasks* provides and requires
    mappings or by following manually created dependency links.

    From dependencies a `directed graph`_ is built. If it has edge ``A -> B``,
    this means ``B`` depends on ``A`` (and that the execution of ``B`` must
    wait until ``A`` has finished executing, on reverting this means that the
    reverting of ``A`` must wait until ``B`` has finished reverting).

    Note: `cyclic`_ dependencies are not allowed.

    .. _directed graph: https://en.wikipedia.org/wiki/Directed_graph
    .. _cyclic: https://en.wikipedia.org/wiki/Cycle_graph
    """

    def __init__(self, name, retry=None):
        super(Flow, self).__init__(name, retry)
        self._graph = gr.DiGraph(name=name)
        self._graph.freeze()

    #: Extracts the unsatisified symbol requirements of a single node.
    _unsatisfied_requires = staticmethod(_unsatisfied_requires)

    def link(self, u, v, decider=None, decider_depth=None):
        """Link existing node u as a runtime dependency of existing node v.

        Note that if the addition of these edges creates a `cyclic`_ graph
        then a :class:`~taskflow.exceptions.DependencyFailure` will be
        raised and the provided changes will be discarded. If the nodes
        that are being requested to link do not exist in this graph than a
        :class:`ValueError` will be raised.

        :param u: task or flow to create a link from (must exist already)
        :param v: task or flow to create a link to (must exist already)
        :param decider: A callback function that will be expected to decide
                        at runtime whether ``v`` should be allowed to
                        execute (or whether the execution of ``v`` should be
                        ignored, and therefore not executed). It is expected
                        to take as single keyword argument ``history`` which
                        will be the execution results of all ``u`` decidable
                        links that have ``v`` as a target. It is expected to
                        return a single boolean (``True`` to allow ``v``
                        execution or ``False`` to not).
        :param decider_depth: One of the :py:class:`~taskflow.deciders.Depth`
                              enumerations (or a string version of) that will
                              be used to influence what atoms are ignored
                              when the decider provided results false. If
                              not provided (and a valid decider is provided
                              then this defaults to
                              :py:attr:`~taskflow.deciders.Depth.ALL`).

        .. _cyclic: https://en.wikipedia.org/wiki/Cycle_graph
        """
        if not self._graph.has_node(u):
            raise ValueError("Node '%s' not found to link from" % (u))
        if not self._graph.has_node(v):
            raise ValueError("Node '%s' not found to link to" % (v))
        if decider is not None:
            if not callable(decider):
                raise ValueError("Decider boolean callback must be callable")
        self._swap(self._link(u, v, manual=True,
                              decider=decider, decider_depth=decider_depth))
        return self

    def _link(self, u, v, graph=None,
              reason=None, manual=False, decider=None,
              decider_depth=None):
        mutable_graph = True
        if graph is None:
            graph = self._graph
            mutable_graph = False
        # NOTE(harlowja): Add an edge to a temporary copy and only if that
        # copy is valid then do we swap with the underlying graph.
        attrs = graph.get_edge_data(u, v)
        if not attrs:
            attrs = {}
        if decider is not None:
            attrs[flow.LINK_DECIDER] = decider
            try:
                # Remove existing decider depth, if one existed.
                del attrs[flow.LINK_DECIDER_DEPTH]
            except KeyError:
                pass
        if decider_depth is not None:
            if decider is None:
                raise ValueError("Decider depth requires a decider to be"
                                 " provided along with it")
            else:
                decider_depth = de.Depth.translate(decider_depth)
                attrs[flow.LINK_DECIDER_DEPTH] = decider_depth
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
            raise exc.DependencyFailure("No path through the node(s) in the"
                                        " graph produces an ordering that"
                                        " will allow for logical"
                                        " edge traversal")
        self._graph = graph.freeze()

    def add(self, *nodes, **kwargs):
        """Adds a given task/tasks/flow/flows to this flow.

        Note that if the addition of these nodes (and any edges) creates
        a `cyclic`_ graph then
        a :class:`~taskflow.exceptions.DependencyFailure` will be
        raised and the applied changes will be discarded.

        :param nodes: node(s) to add to the flow
        :param kwargs: keyword arguments, the two keyword arguments
                       currently processed are:

                        * ``resolve_requires`` a boolean that when true (the
                          default) implies that when node(s) are added their
                          symbol requirements will be matched to existing
                          node(s) and links will be automatically made to those
                          providers. If multiple possible providers exist
                          then a
                          :class:`~taskflow.exceptions.AmbiguousDependency`
                          exception will be raised and the provided additions
                          will be discarded.
                        * ``resolve_existing``, a boolean that when true (the
                          default) implies that on addition of a new node that
                          existing node(s) will have their requirements scanned
                          for symbols that this newly added node can provide.
                          If a match is found a link is automatically created
                          from the newly added node to the requiree.

        .. _cyclic: https://en.wikipedia.org/wiki/Cycle_graph
        """

        # Let's try to avoid doing any work if we can; since the below code
        # after this filter can create more temporary graphs that aren't needed
        # if the nodes already exist...
        nodes = [i for i in nodes if not self._graph.has_node(i)]
        if not nodes:
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

        for node in self._graph.nodes:
            for value in self._unsatisfied_requires(node, self._graph,
                                                    retry_provides):
                required[value].append(node)
            for value in node.provides:
                provided[value].append(node)

        # NOTE(harlowja): Add node(s) and edge(s) to a temporary copy of the
        # underlying graph and only if that is successful added to do we then
        # swap with the underlying graph.
        tmp_graph = gr.DiGraph(self._graph)
        for node in nodes:
            tmp_graph.add_node(node)

            # Try to find a valid provider.
            if resolve_requires:
                for value in self._unsatisfied_requires(node, tmp_graph,
                                                        retry_provides):
                    if value in provided:
                        providers = provided[value]
                        if len(providers) > 1:
                            provider_names = [n.name for n in providers]
                            raise exc.AmbiguousDependency(
                                "Resolution error detected when"
                                " adding '%(node)s', multiple"
                                " providers %(providers)s found for"
                                " required symbol '%(value)s'"
                                % dict(node=node.name,
                                       providers=sorted(provider_names),
                                       value=value))
                        else:
                            self._link(providers[0], node,
                                       graph=tmp_graph, reason=value)
                    else:
                        required[value].append(node)

            for value in node.provides:
                provided[value].append(node)

            # See if what we provide fulfills any existing requiree.
            if resolve_existing:
                for value in node.provides:
                    if value in required:
                        for requiree in list(required[value]):
                            if requiree is not node:
                                self._link(node, requiree,
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
        for n, _n_data in self.iter_nodes():
            yield n

    def iter_links(self):
        return self._get_subgraph().edges(data=True)

    def iter_nodes(self):
        g = self._get_subgraph()
        for n in g.topological_sort():
            yield n, g.nodes[n]

    @property
    def requires(self):
        requires = set()
        retry_provides = set()
        if self._retry is not None:
            requires.update(self._retry.requires)
            retry_provides.update(self._retry.provides)
        g = self._get_subgraph()
        for node in g.nodes:
            requires.update(self._unsatisfied_requires(node, g,
                                                       retry_provides))
        return frozenset(requires)


def _reset_cached_subgraph(func):
    """Resets cached subgraph after execution, in case it was affected."""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        self._subgraph = None
        return result

    return wrapper


class TargetedFlow(Flow):
    """Graph flow with a target.

    Adds possibility to execute a flow up to certain graph node
    (task or subflow).
    """

    def __init__(self, *args, **kwargs):
        super(TargetedFlow, self).__init__(*args, **kwargs)
        self._subgraph = None
        self._target = None

    def set_target(self, target_node):
        """Set target for the flow.

        Any node(s) (tasks or subflows) not needed for the target
        node will not be executed.
        """
        if not self._graph.has_node(target_node):
            raise ValueError("Node '%s' not found" % target_node)
        self._target = target_node
        self._subgraph = None

    def reset_target(self):
        """Reset target for the flow.

        All node(s) of the flow will be executed.
        """
        self._target = None
        self._subgraph = None

    add = _reset_cached_subgraph(Flow.add)

    link = _reset_cached_subgraph(Flow.link)

    def _get_subgraph(self):
        if self._subgraph is not None:
            return self._subgraph
        if self._target is None:
            return self._graph
        nodes = [self._target]
        nodes.extend(self._graph.bfs_predecessors_iter(self._target))
        self._subgraph = gr.DiGraph(
            incoming_graph_data=self._graph.subgraph(nodes))
        self._subgraph.freeze()
        return self._subgraph
