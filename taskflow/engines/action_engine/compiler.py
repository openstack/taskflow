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

import collections
import threading

from taskflow import exceptions as exc
from taskflow import flow
from taskflow import logging
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
_EDGE_REASONS = flow.LINK_REASONS


class Compilation(object):
    """The result of a compilers compile() is this *immutable* object."""

    def __init__(self, execution_graph, hierarchy):
        self._execution_graph = execution_graph
        self._hierarchy = hierarchy

    @property
    def execution_graph(self):
        """The execution ordering of atoms (as a graph structure)."""
        return self._execution_graph

    @property
    def hierarchy(self):
        """The hierachy of patterns (as a tree structure)."""
        return self._hierarchy


def _add_update_edges(graph, nodes_from, nodes_to, attr_dict=None):
    """Adds/updates edges from nodes to other nodes in the specified graph.

    It will connect the 'nodes_from' to the 'nodes_to' if an edge currently
    does *not* exist (if it does already exist then the edges attributes
    are just updated instead). When an edge is created the provided edge
    attributes dictionary will be applied to the new edge between these two
    nodes.
    """
    # NOTE(harlowja): give each edge its own attr copy so that if it's
    # later modified that the same copy isn't modified...
    for u in nodes_from:
        for v in nodes_to:
            if not graph.has_edge(u, v):
                if attr_dict:
                    graph.add_edge(u, v, attr_dict=attr_dict.copy())
                else:
                    graph.add_edge(u, v)
            else:
                # Just update the attr_dict (if any).
                if attr_dict:
                    graph.add_edge(u, v, attr_dict=attr_dict.copy())


class Linker(object):
    """Compiler helper that adds pattern(s) constraints onto a graph."""

    @staticmethod
    def _is_not_empty(graph):
        # Returns true if the given graph is *not* empty...
        return graph.number_of_nodes() > 0

    @staticmethod
    def _find_first_decomposed(node, priors,
                               decomposed_members, decomposed_filter):
        # How this works; traverse backwards and find only the predecessor
        # items that are actually connected to this entity, and avoid any
        # linkage that is not directly connected. This is guaranteed to be
        # valid since we always iter_links() over predecessors before
        # successors in all currently known patterns; a queue is used here
        # since it is possible for a node to have 2+ different predecessors so
        # we must search back through all of them in a reverse BFS order...
        #
        # Returns the first decomposed graph of those nodes (including the
        # passed in node) that passes the provided filter
        # function (returns none if none match).
        frontier = collections.deque([node])
        # NOTE(harowja): None is in this initial set since the first prior in
        # the priors list has None as its predecessor (which we don't want to
        # look for a decomposed member of).
        visited = set([None])
        while frontier:
            node = frontier.popleft()
            if node in visited:
                continue
            node_graph = decomposed_members[node]
            if decomposed_filter(node_graph):
                return node_graph
            visited.add(node)
            # TODO(harlowja): optimize this more to avoid searching through
            # things already searched...
            for (u, v) in reversed(priors):
                if node == v:
                    # Queue its predecessor to be searched in the future...
                    frontier.append(u)
        else:
            return None

    def apply_constraints(self, graph, flow, decomposed_members):
        # This list is used to track the links that have been previously
        # iterated over, so that when we are trying to find a entry to
        # connect to that we iterate backwards through this list, finding
        # connected nodes to the current target (lets call it v) and find
        # the first (u_n, or u_n - 1, u_n - 2...) that was decomposed into
        # a non-empty graph. We also retain all predecessors of v so that we
        # can correctly locate u_n - 1 if u_n turns out to have decomposed into
        # an empty graph (and so on).
        priors = []
        # NOTE(harlowja): u, v are flows/tasks (also graph terminology since
        # we are compiling things down into a flattened graph), the meaning
        # of this link iteration via iter_links() is that u -> v (with the
        # provided dictionary attributes, if any).
        for (u, v, attr_dict) in flow.iter_links():
            if not priors:
                priors.append((None, u))
            v_g = decomposed_members[v]
            if not v_g.number_of_nodes():
                priors.append((u, v))
                continue
            invariant = any(attr_dict.get(k) for k in _EDGE_INVARIANTS)
            if not invariant:
                # This is a symbol *only* dependency, connect
                # corresponding providers and consumers to allow the consumer
                # to be executed immediately after the provider finishes (this
                # is an optimization for these types of dependencies...)
                u_g = decomposed_members[u]
                if not u_g.number_of_nodes():
                    # This must always exist, but incase it somehow doesn't...
                    raise exc.CompilationFailure(
                        "Non-invariant link being created from '%s' ->"
                        " '%s' even though the target '%s' was found to be"
                        " decomposed into an empty graph" % (v, u, u))
                for u in u_g.nodes_iter():
                    for v in v_g.nodes_iter():
                        depends_on = u.provides & v.requires
                        if depends_on:
                            _add_update_edges(graph,
                                              [u], [v],
                                              attr_dict={
                                                  _EDGE_REASONS: depends_on,
                                              })
            else:
                # Connect nodes with no predecessors in v to nodes with no
                # successors in the *first* non-empty predecessor of v (thus
                # maintaining the edge dependency).
                match = self._find_first_decomposed(u, priors,
                                                    decomposed_members,
                                                    self._is_not_empty)
                if match is not None:
                    _add_update_edges(graph,
                                      match.no_successors_iter(),
                                      list(v_g.no_predecessors_iter()),
                                      attr_dict=attr_dict)
            priors.append((u, v))


class PatternCompiler(object):
    """Compiles a pattern (or task) into a compilation unit.

    Let's dive into the basic idea for how this works:

    The compiler here is provided a 'root' object via its __init__ method,
    this object could be a task, or a flow (one of the supported patterns),
    the end-goal is to produce a :py:class:`.Compilation` object as the result
    with the needed components. If this is not possible a
    :py:class:`~.taskflow.exceptions.CompilationFailure` will be raised (or
    in the case where a unknown type is being requested to compile
    a ``TypeError`` will be raised).

    The complexity of this comes into play when the 'root' is a flow that
    contains itself other nested flows (and so-on); to compile this object and
    its contained objects into a graph that *preserves* the constraints the
    pattern mandates we have to go through a recursive algorithm that creates
    subgraphs for each nesting level, and then on the way back up through
    the recursion (now with a decomposed mapping from contained patterns or
    atoms to there corresponding subgraph) we have to then connect the
    subgraphs (and the atom(s) there-in) that were decomposed for a pattern
    correctly into a new graph (using a :py:class:`.Linker` object to ensure
    the pattern mandated constraints are retained) and then return to the
    caller (and they will do the same thing up until the root node, which by
    that point one graph is created with all contained atoms in the
    pattern/nested patterns mandated ordering).

    Also maintained in the :py:class:`.Compilation` object is a hierarchy of
    the nesting of items (which is also built up during the above mentioned
    recusion, via a much simpler algorithm); this is typically used later to
    determine the prior atoms of a given atom when looking up values that can
    be provided to that atom for execution (see the scopes.py file for how this
    works). Note that although you *could* think that the graph itself could be
    used for this, which in some ways it can (for limited usage) the hierarchy
    retains the nested structure (which is useful for scoping analysis/lookup)
    to be able to provide back a iterator that gives back the scopes visible
    at each level (the graph does not have this information once flattened).

    Let's take an example:

    Given the pattern ``f(a(b, c), d)`` where ``f`` is a
    :py:class:`~taskflow.patterns.linear_flow.Flow` with items ``a(b, c)``
    where ``a`` is a :py:class:`~taskflow.patterns.linear_flow.Flow` composed
    of tasks ``(b, c)`` and task ``d``.

    The algorithm that will be performed (mirroring the above described logic)
    will go through the following steps (the tree hierachy building is left
    out as that is more obvious)::

        Compiling f
          - Decomposing flow f with no parent (must be the root)
          - Compiling a
              - Decomposing flow a with parent f
              - Compiling b
                  - Decomposing task b with parent a
                  - Decomposed b into:
                    Name: b
                    Nodes: 1
                      - b
                    Edges: 0
              - Compiling c
                  - Decomposing task c with parent a
                  - Decomposed c into:
                    Name: c
                    Nodes: 1
                      - c
                    Edges: 0
              - Relinking decomposed b -> decomposed c
              - Decomposed a into:
                Name: a
                Nodes: 2
                  - b
                  - c
                Edges: 1
                  b -> c ({'invariant': True})
          - Compiling d
              - Decomposing task d with parent f
              - Decomposed d into:
                Name: d
                Nodes: 1
                  - d
                Edges: 0
          - Relinking decomposed a -> decomposed d
          - Decomposed f into:
            Name: f
            Nodes: 3
              - c
              - b
              - d
            Edges: 2
              c -> d ({'invariant': True})
              b -> c ({'invariant': True})
    """

    def __init__(self, root, freeze=True):
        self._root = root
        self._history = set()
        self._linker = Linker()
        self._freeze = freeze
        self._lock = threading.Lock()
        self._compilation = None

    def _flatten(self, item, parent):
        """Flattens a item (pattern, task) into a graph + tree node."""
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
        if nodes_to:
            _add_update_edges(graph, [retry], nodes_to,
                              attr_dict=_RETRY_EDGE_DATA)

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

    def _decompose_flow(self, flow, parent):
        """Decomposes a flow into a graph, tree node + decomposed subgraphs."""
        graph = gr.DiGraph(name=flow.name)
        node = tr.Node(flow)
        if parent is not None:
            parent.add(node)
        if flow.retry is not None:
            node.add(tr.Node(flow.retry))
        decomposed_members = {}
        for item in flow:
            subgraph, _subnode = self._flatten(item, node)
            decomposed_members[item] = subgraph
            if subgraph.number_of_nodes():
                graph = gr.merge_graphs([graph, subgraph])
        return graph, node, decomposed_members

    def _flatten_flow(self, flow, parent):
        """Flattens a flow."""
        graph, node, decomposed_members = self._decompose_flow(flow, parent)
        self._linker.apply_constraints(graph, flow, decomposed_members)
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
        # the cycle detection), so only do it if we know BLATHER is enabled
        # and not under all cases.
        if LOG.isEnabledFor(logging.BLATHER):
            LOG.blather("Translated '%s'", self._root)
            LOG.blather("Graph:")
            for line in graph.pformat().splitlines():
                # Indent it so that it's slightly offset from the above line.
                LOG.blather("  %s", line)
            LOG.blather("Hierarchy:")
            for line in node.pformat().splitlines():
                # Indent it so that it's slightly offset from the above line.
                LOG.blather("  %s", line)

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
