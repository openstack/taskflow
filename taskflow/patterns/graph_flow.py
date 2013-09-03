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

import logging

from networkx.algorithms import dag
from networkx.classes import digraph
from networkx import exception as g_exc

from taskflow import decorators
from taskflow import exceptions as exc
from taskflow.patterns import linear_flow
from taskflow.utils import flow_utils
from taskflow.utils import graph_utils

LOG = logging.getLogger(__name__)


class Flow(linear_flow.Flow):
    """A extension of the linear flow which will run the associated tasks in
    a linear topological ordering (and reverse using the same linear
    topological order).
    """

    def __init__(self, name, parents=None, uuid=None):
        super(Flow, self).__init__(name, parents, uuid)
        self._graph = digraph.DiGraph()

    @decorators.locked
    def add(self, task, infer=True):
        # Only insert the node to start, connect all the edges
        # together later after all nodes have been added since if we try
        # to infer the edges at this stage we likely will fail finding
        # dependencies from nodes that don't exist.
        r = flow_utils.AOTRunner(task)
        self._graph.add_node(r, uuid=r.uuid, infer=infer)
        self._reset_internals()
        return r.uuid

    def _find_uuid(self, uuid):
        runner = None
        for r in self._graph.nodes_iter():
            if r.uuid == uuid:
                runner = r
                break
        return runner

    def __len__(self):
        return len(self._graph)

    @decorators.locked
    def add_dependency(self, provider_uuid, requirer_uuid):
        """Connects provider to requirer where provider will now be required
        to run before requirer does.
        """
        if provider_uuid == requirer_uuid:
            raise ValueError("Unable to link %s to itself" % provider_uuid)
        provider = self._find_uuid(provider_uuid)
        if not provider:
            raise ValueError("No provider found with uuid %s" % provider_uuid)
        requirer = self._find_uuid(requirer_uuid)
        if not requirer:
            raise ValueError("No requirer found with uuid %s" % requirer_uuid)
        self._add_dependency(provider, requirer, reason='manual')
        self._reset_internals()

    def _add_dependency(self, provider, requirer, reason):
        self._graph.add_edge(provider, requirer, reason=reason)

    def __str__(self):
        lines = ["GraphFlow: %s" % (self.name)]
        lines.append("%s" % (self.uuid))
        lines.append("%s" % (self._graph.number_of_nodes()))
        lines.append("%s" % (self._graph.number_of_edges()))
        lines.append("%s" % (len(self.parents)))
        lines.append("%s" % (self.state))
        return "; ".join(lines)

    def _reset_internals(self):
        super(Flow, self)._reset_internals()
        self._runners = []

    @decorators.locked
    def remove(self, uuid):
        runner = self._find_uuid(uuid)
        if not runner:
            raise ValueError("No uuid %s found" % (uuid))
        else:
            self._graph.remove_node(runner)
            self._reset_internals()

    def _ordering(self):
        try:
            return iter(self._connect())
        except g_exc.NetworkXUnfeasible:
            raise exc.InvalidStateException("Unable to correctly determine "
                                            "the path through the provided "
                                            "flow which will satisfy the "
                                            "tasks needed inputs and outputs.")

    def _connect(self):
        """Connects the nodes & edges of the graph together by examining who
        the requirements of each node and finding another node that will
        create said dependency.
        """
        if len(self._graph) == 0:
            return []
        if self._connected:
            return self._runners

        # Clear out all automatically added edges since we want to do a fresh
        # connections. Leave the manually connected ones intact so that users
        # still retain the dependencies they established themselves.
        def discard_edge_func(u, v, e_data):
            if e_data and e_data.get('reason') != 'manual':
                return True
            return False

        # Link providers to requirers.
        graph_utils.connect(self._graph, discard_func=discard_edge_func)

        # Now figure out the order so that we can give the runners there
        # optional item providers as well as figure out the topological run
        # order.
        run_order = dag.topological_sort(self._graph)
        run_stack = []
        for r in run_order:
            r.runs_before = list(reversed(run_stack))
            run_stack.append(r)
        self._runners = run_order
        self._connected = True
        return run_order
