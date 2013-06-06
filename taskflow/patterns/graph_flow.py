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
import logging

from networkx.algorithms import dag
from networkx.classes import digraph
from networkx import exception as g_exc

from taskflow import decorators
from taskflow import exceptions as exc
from taskflow.patterns import linear_flow
from taskflow import utils

LOG = logging.getLogger(__name__)


class Flow(linear_flow.Flow):
    """A extension of the linear flow which will run the associated tasks in
    a linear topological ordering (and reverse using the same linear
    topological order)"""

    def __init__(self, name, parents=None):
        super(Flow, self).__init__(name, parents)
        self._graph = digraph.DiGraph()

    @decorators.locked
    def add(self, task):
        # Only insert the node to start, connect all the edges
        # together later after all nodes have been added since if we try
        # to infer the edges at this stage we likely will fail finding
        # dependencies from nodes that don't exist.
        assert isinstance(task, collections.Callable)
        r = utils.Runner(task)
        self._graph.add_node(r, uuid=r.uuid)
        self._runners = []
        return r.uuid

    def _add_dependency(self, provider, requirer):
        if not self._graph.has_edge(provider, requirer):
            self._graph.add_edge(provider, requirer)

    def __str__(self):
        lines = ["GraphFlow: %s" % (self.name)]
        lines.append("  Number of tasks: %s" % (self._graph.number_of_nodes()))
        lines.append("  Number of dependencies: %s"
                     % (self._graph.number_of_edges()))
        lines.append("  State: %s" % (self.state))
        return "\n".join(lines)

    @decorators.locked
    def remove(self, task_uuid):
        remove_nodes = []
        for r in self._graph.nodes_iter():
            if r.uuid == task_uuid:
                remove_nodes.append(r)
        if not remove_nodes:
            raise IndexError("No task found with uuid %s" % (task_uuid))
        else:
            for r in remove_nodes:
                self._graph.remove_node(r)
                self._runners = []

    def _ordering(self):
        try:
            return self._connect()
        except g_exc.NetworkXUnfeasible:
            raise exc.InvalidStateException("Unable to correctly determine "
                                            "the path through the provided "
                                            "flow which will satisfy the "
                                            "tasks needed inputs and outputs.")

    def _connect(self):
        """Connects the nodes & edges of the graph together by examining who
        the requirements of each node and finding another node that will
        create said dependency."""
        if len(self._graph) == 0:
            return []
        if self._runners:
            return self._runners

        # Link providers to requirers.
        #
        # TODO(harlowja): allow for developers to manually establish these
        # connections instead of automatically doing it for them??
        for n in self._graph.nodes_iter():
            n_requires = set(utils.get_attr(n.task, 'requires', []))
            LOG.debug("Finding providers of %s for %s", n_requires, n)
            for p in self._graph.nodes_iter():
                if not n_requires:
                    break
                if n is p:
                    continue
                p_provides = set(utils.get_attr(p.task, 'provides', []))
                p_satisfies = n_requires & p_provides
                if p_satisfies:
                    # P produces for N so thats why we link P->N and not N->P
                    self._add_dependency(p, n)
                    for k in p_satisfies:
                        n.providers[k] = p
                    LOG.debug("Found provider of %s from %s", p_satisfies, p)
                    n_requires = n_requires - p_satisfies
            if n_requires:
                raise exc.MissingDependencies(n, sorted(n_requires))

        # Now figure out the order so that we can give the runners there
        # optional item providers as well as figure out the topological run
        # order.
        run_order = dag.topological_sort(self._graph)
        run_stack = []
        for r in run_order:
            r.runs_before = list(reversed(run_stack))
            run_stack.append(r)
        self._runners = run_order
        return run_order
