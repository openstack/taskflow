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

import threading

import fasteners
from oslo_utils import excutils
import six

from taskflow import flow
from taskflow import logging
from taskflow import task
from taskflow.types import graph as gr
from taskflow.types import tree as tr
from taskflow.utils import iter_utils
from taskflow.utils import misc

from taskflow.flow import (LINK_INVARIANT, LINK_RETRY)  # noqa

LOG = logging.getLogger(__name__)

# Constants attached to node attributes in the execution graph (and tree
# node metadata), provided as constants here and constants in the compilation
# class (so that users will not have to import this file to access them); but
# provide them as module constants so that internal code can more
# easily access them...
TASK = 'task'
RETRY = 'retry'
FLOW = 'flow'
FLOW_END = 'flow_end'

# Quite often used together, so make a tuple everyone can share...
ATOMS = (TASK, RETRY)
FLOWS = (FLOW, FLOW_END)


class Terminator(object):
    """Flow terminator class."""

    def __init__(self, flow):
        self._flow = flow
        self._name = "%s[$]" % (self._flow.name,)

    @property
    def flow(self):
        """The flow which this terminator signifies/marks the end of."""
        return self._flow

    @property
    def name(self):
        """Useful name this end terminator has (derived from flow name)."""
        return self._name

    def __str__(self):
        return "%s[$]" % (self._flow,)


class Compilation(object):
    """The result of a compilers ``compile()`` is this *immutable* object."""

    #: Task nodes will have a ``kind`` metadata key with this value.
    TASK = TASK

    #: Retry nodes will have a ``kind`` metadata key with this value.
    RETRY = RETRY

    FLOW = FLOW
    """
    Flow **entry** nodes will have a ``kind`` metadata key with
    this value.
    """

    FLOW_END = FLOW_END
    """
    Flow **exit** nodes will have a ``kind`` metadata key with
    this value (only applicable for compilation execution graph, not currently
    used in tree hierarchy).
    """

    def __init__(self, execution_graph, hierarchy):
        self._execution_graph = execution_graph
        self._hierarchy = hierarchy

    @property
    def execution_graph(self):
        """The execution ordering of atoms (as a graph structure)."""
        return self._execution_graph

    @property
    def hierarchy(self):
        """The hierarchy of patterns (as a tree structure)."""
        return self._hierarchy


def _overlap_occurrence_detector(to_graph, from_graph):
    """Returns how many nodes in 'from' graph are in 'to' graph (if any)."""
    return iter_utils.count(node for node in from_graph.nodes_iter()
                            if node in to_graph)


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


class TaskCompiler(object):
    """Non-recursive compiler of tasks."""

    def compile(self, task, parent=None):
        graph = gr.DiGraph(name=task.name)
        graph.add_node(task, kind=TASK)
        node = tr.Node(task, kind=TASK)
        if parent is not None:
            parent.add(node)
        return graph, node


class FlowCompiler(object):
    """Recursive compiler of flows."""

    def __init__(self, deep_compiler_func):
        self._deep_compiler_func = deep_compiler_func

    def compile(self, flow, parent=None):
        """Decomposes a flow into a graph and scope tree hierarchy."""
        graph = gr.DiGraph(name=flow.name)
        graph.add_node(flow, kind=FLOW, noop=True)
        tree_node = tr.Node(flow, kind=FLOW, noop=True)
        if parent is not None:
            parent.add(tree_node)
        if flow.retry is not None:
            tree_node.add(tr.Node(flow.retry, kind=RETRY))
        decomposed = dict(
            (child, self._deep_compiler_func(child, parent=tree_node)[0])
            for child in flow)
        decomposed_graphs = list(six.itervalues(decomposed))
        graph = gr.merge_graphs(graph, *decomposed_graphs,
                                overlap_detector=_overlap_occurrence_detector)
        for u, v, attr_dict in flow.iter_links():
            u_graph = decomposed[u]
            v_graph = decomposed[v]
            _add_update_edges(graph, u_graph.no_successors_iter(),
                              list(v_graph.no_predecessors_iter()),
                              attr_dict=attr_dict)
        # Insert the flow(s) retry if needed, and always make sure it
        # is the **immediate** successor of the flow node itself.
        if flow.retry is not None:
            graph.add_node(flow.retry, kind=RETRY)
            _add_update_edges(graph, [flow], [flow.retry],
                              attr_dict={LINK_INVARIANT: True})
            for node in graph.nodes_iter():
                if node is not flow.retry and node is not flow:
                    graph.node[node].setdefault(RETRY, flow.retry)
            from_nodes = [flow.retry]
            attr_dict = {LINK_INVARIANT: True, LINK_RETRY: True}
        else:
            from_nodes = [flow]
            attr_dict = {LINK_INVARIANT: True}
        # Ensure all nodes with no predecessors are connected to this flow
        # or its retry node (so that the invariant that the flow node is
        # traversed through before its contents is maintained); this allows
        # us to easily know when we have entered a flow (when running) and
        # do special and/or smart things such as only traverse up to the
        # start of a flow when looking for node deciders.
        _add_update_edges(graph, from_nodes, [
            node for node in graph.no_predecessors_iter()
            if node is not flow
        ], attr_dict=attr_dict)
        # Connect all nodes with no successors into a special terminator
        # that is used to identify the end of the flow and ensure that all
        # execution traversals will traverse over this node before executing
        # further work (this is especially useful for nesting and knowing
        # when we have exited a nesting level); it allows us to do special
        # and/or smart things such as applying deciders up to (but not
        # beyond) a flow termination point.
        #
        # Do note that in a empty flow this will just connect itself to
        # the flow node itself... and also note we can not use the flow
        # object itself (primarily because the underlying graph library
        # uses hashing to identify node uniqueness and we can easily create
        # a loop if we don't do this correctly, so avoid that by just
        # creating this special node and tagging it with a special kind); we
        # may be able to make this better in the future with a multidigraph
        # that networkx provides??
        flow_term = Terminator(flow)
        graph.add_node(flow_term, kind=FLOW_END, noop=True)
        _add_update_edges(graph, [
            node for node in graph.no_successors_iter()
            if node is not flow_term
        ], [flow_term], attr_dict={LINK_INVARIANT: True})
        return graph, tree_node


class PatternCompiler(object):
    """Compiles a flow pattern (or task) into a compilation unit.

    Let's dive into the basic idea for how this works:

    The compiler here is provided a 'root' object via its __init__ method,
    this object could be a task, or a flow (one of the supported patterns),
    the end-goal is to produce a :py:class:`.Compilation` object as the result
    with the needed components. If this is not possible a
    :py:class:`~.taskflow.exceptions.CompilationFailure` will be raised.
    In the case where a **unknown** type is being requested to compile
    a ``TypeError`` will be raised and when a duplicate object (one that
    has **already** been compiled) is encountered a ``ValueError`` is raised.

    The complexity of this comes into play when the 'root' is a flow that
    contains itself other nested flows (and so-on); to compile this object and
    its contained objects into a graph that *preserves* the constraints the
    pattern mandates we have to go through a recursive algorithm that creates
    subgraphs for each nesting level, and then on the way back up through
    the recursion (now with a decomposed mapping from contained patterns or
    atoms to there corresponding subgraph) we have to then connect the
    subgraphs (and the atom(s) there-in) that were decomposed for a pattern
    correctly into a new graph and then ensure the pattern mandated
    constraints are retained. Finally we then return to the
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
    will go through the following steps (the tree hierarchy building is left
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
        self._freeze = freeze
        self._lock = threading.Lock()
        self._compilation = None
        self._matchers = [
            (flow.Flow, FlowCompiler(self._compile)),
            (task.Task, TaskCompiler()),
        ]
        self._level = 0

    def _compile(self, item, parent=None):
        """Compiles a item (pattern, task) into a graph + tree node."""
        item_compiler = misc.match_type(item, self._matchers)
        if item_compiler is not None:
            self._pre_item_compile(item)
            graph, node = item_compiler.compile(item, parent=parent)
            self._post_item_compile(item, graph, node)
            return graph, node
        else:
            raise TypeError("Unknown object '%s' (%s) requested to compile"
                            % (item, type(item)))

    def _pre_item_compile(self, item):
        """Called before a item is compiled; any pre-compilation actions."""
        if item in self._history:
            raise ValueError("Already compiled item '%s' (%s), duplicate"
                             " and/or recursive compiling is not"
                             " supported" % (item, type(item)))
        self._history.add(item)
        if LOG.isEnabledFor(logging.TRACE):
            LOG.trace("%sCompiling '%s'", "  " * self._level, item)
        self._level += 1

    def _post_item_compile(self, item, graph, node):
        """Called after a item is compiled; doing post-compilation actions."""
        self._level -= 1
        if LOG.isEnabledFor(logging.TRACE):
            prefix = '  ' * self._level
            LOG.trace("%sDecomposed '%s' into:", prefix, item)
            prefix = '  ' * (self._level + 1)
            LOG.trace("%sGraph:", prefix)
            for line in graph.pformat().splitlines():
                LOG.trace("%s  %s", prefix, line)
            LOG.trace("%sHierarchy:", prefix)
            for line in node.pformat().splitlines():
                LOG.trace("%s  %s", prefix, line)

    def _pre_compile(self):
        """Called before the compilation of the root starts."""
        self._history.clear()
        self._level = 0

    def _post_compile(self, graph, node):
        """Called after the compilation of the root finishes successfully."""
        self._history.clear()
        self._level = 0

    @fasteners.locked
    def compile(self):
        """Compiles the contained item into a compiled equivalent."""
        if self._compilation is None:
            self._pre_compile()
            try:
                graph, node = self._compile(self._root, parent=None)
            except Exception:
                with excutils.save_and_reraise_exception():
                    # Always clear the history, to avoid retaining junk
                    # in memory that isn't needed to be in memory if
                    # compilation fails...
                    self._history.clear()
            else:
                self._post_compile(graph, node)
                if self._freeze:
                    graph.freeze()
                    node.freeze()
                self._compilation = Compilation(graph, node)
        return self._compilation
