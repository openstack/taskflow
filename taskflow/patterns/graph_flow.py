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

from taskflow import exceptions as exc
from taskflow.patterns import linear_flow
from taskflow import utils

LOG = logging.getLogger(__name__)


class Flow(linear_flow.Flow):
    """A extension of the linear flow which will run the associated tasks in
    a linear topological ordering (and reverse using the same linear
    topological order)"""

    def __init__(self, name, parents=None, allow_same_inputs=True):
        super(Flow, self).__init__(name, parents)
        self._graph = digraph.DiGraph()
        self._connected = False
        self._allow_same_inputs = allow_same_inputs

    def add(self, task):
        # Only insert the node to start, connect all the edges
        # together later after all nodes have been added since if we try
        # to infer the edges at this stage we likely will fail finding
        # dependencies from nodes that don't exist.
        assert isinstance(task, collections.Callable)
        if not self._graph.has_node(task):
            self._graph.add_node(task)
            self._connected = False

    def add_many(self, tasks):
        for t in tasks:
            self.add(t)

    def __str__(self):
        lines = ["GraphFlow: %s" % (self.name)]
        lines.append("  Number of tasks: %s" % (self._graph.number_of_nodes()))
        lines.append("  Number of dependencies: %s"
                     % (self._graph.number_of_edges()))
        lines.append("  State: %s" % (self.state))
        return "\n".join(lines)

    def _fetch_task_inputs(self, task):

        def extract_inputs(place_where, would_like, is_optional=False):
            for n in would_like:
                for (them, there_result) in self.results:
                    they_provide = utils.get_attr(them, 'provides', [])
                    if n not in set(they_provide):
                        continue
                    if ((not is_optional and
                         not self._graph.has_edge(them, task))):
                        continue
                    if there_result and n in there_result:
                        place_where[n].append(there_result[n])
                        if is_optional:
                            # Take the first task that provides this optional
                            # item.
                            break
                    elif not is_optional:
                        place_where[n].append(None)

        required_inputs = set(utils.get_attr(task, 'requires', []))
        optional_inputs = set(utils.get_attr(task, 'optional', []))
        optional_inputs = optional_inputs - required_inputs

        task_inputs = collections.defaultdict(list)
        extract_inputs(task_inputs, required_inputs)
        extract_inputs(task_inputs, optional_inputs, is_optional=True)

        def collapse_functor(k_v):
            (k, v) = k_v
            if len(v) == 1:
                v = v[0]
            return (k, v)

        return dict(map(collapse_functor, task_inputs.iteritems()))

    def _ordering(self):
        self._connect()
        try:
            return dag.topological_sort(self._graph)
        except g_exc.NetworkXUnfeasible:
            raise exc.InvalidStateException("Unable to correctly determine "
                                            "the path through the provided "
                                            "flow which will satisfy the "
                                            "tasks needed inputs and outputs.")

    def _connect(self):
        """Connects the nodes & edges of the graph together."""
        if self._connected or len(self._graph) == 0:
            return

        # Figure out the provider of items and the requirers of items.
        provides_what = collections.defaultdict(list)
        requires_what = collections.defaultdict(list)
        for t in self._graph.nodes_iter():
            for r in utils.get_attr(t, 'requires', []):
                requires_what[r].append(t)
            for p in utils.get_attr(t, 'provides', []):
                provides_what[p].append(t)

        def get_providers(node, want_what):
            providers = []
            for (producer, me) in self._graph.in_edges_iter(node):
                providing_what = self._graph.get_edge_data(producer, me)
                if want_what in providing_what:
                    providers.append(producer)
            return providers

        # Link providers to consumers of items.
        for (want_what, who_wants) in requires_what.iteritems():
            who_provided = 0
            for p in provides_what[want_what]:
                # P produces for N so thats why we link P->N and not N->P
                for n in who_wants:
                    if p is n:
                        # No self-referencing allowed.
                        continue
                    if ((len(get_providers(n, want_what))
                         and not self._allow_same_inputs)):
                        msg = "Multiple providers of %s not allowed."
                        raise exc.InvalidStateException(msg % (want_what))
                    self._graph.add_edge(p, n, attr_dict={
                        want_what: True,
                    })
                    who_provided += 1
            if not who_provided:
                who_wants = ", ".join([str(a) for a in who_wants])
                raise exc.InvalidStateException("%s requires input %s "
                                                "but no other task produces "
                                                "said output." % (who_wants,
                                                                  want_what))

        self._connected = True
