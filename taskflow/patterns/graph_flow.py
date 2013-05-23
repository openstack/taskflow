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

from networkx import exception as g_exc
from networkx.algorithms import dag
from networkx.classes import digraph

from taskflow import exceptions as exc
from taskflow.patterns import ordered_flow

LOG = logging.getLogger(__name__)


class Flow(ordered_flow.Flow):
    """A flow which will analyze the attached tasks input requirements and
    determine who provides said input and order the task so that said providing
    task will be ran before."""

    def __init__(self, name, tolerant=False, parents=None):
        super(Flow, self).__init__(name, tolerant, parents)
        self._graph = digraph.DiGraph()
        self._connected = False

    def add(self, task):
        # Do something with the task, either store it for later
        # or add it to the graph right now...
        #
        # Only insert the node to start, connect all the edges
        # together later after all nodes have been added.
        self._graph.add_node(task)
        self._connected = False

    def _fetch_task_inputs(self, task):
        inputs = {}
        for n in task.requires():
            for (them, there_result) in self.results:
                if (not self._graph.has_edge(them, task) or
                    not n in them.provides() or not there_result):
                    continue
                if n in there_result:
                    # NOTE(harlowja): later results overwrite
                    # prior results for the same keys, which is
                    # typically desired.
                    inputs[n] = there_result[n]
        return inputs

    def order(self):
        self.connect()
        try:
            return dag.topological_sort(self._graph)
        except g_exc.NetworkXUnfeasible:
            raise exc.InvalidStateException("Unable to correctly determine "
                                            "the path through the provided "
                                            "flow which will satisfy the "
                                            "tasks needed inputs and outputs.")

    def connect(self):
        """Connects the nodes & edges of the graph together."""
        if self._connected or len(self._graph) == 0:
            return

        # Figure out the provider of items and the requirers of items.
        provides_what = collections.defaultdict(list)
        requires_what = collections.defaultdict(list)
        for t in self._graph.nodes_iter():
            for r in t.requires():
                requires_what[r].append(t)
            for p in t.provides():
                provides_what[p].append(t)

        # Link providers to consumers of items.
        for (want_what, who_wants) in requires_what.iteritems():
            who_provided = 0
            for p in provides_what[want_what]:
                # P produces for N so thats why we link P->N and not N->P
                for n in who_wants:
                    if p is n:
                        # No self-referencing allowed.
                        continue
                    why = {
                        want_what: True,
                    }
                    self._graph.add_edge(p, n, why)
                    who_provided += 1
            if not who_provided:
                who_wants = ", ".join([str(a) for a in who_wants])
                raise exc.InvalidStateException("%s requires input %s "
                                                "but no other task produces "
                                                "said output." % (who_wants,
                                                                  want_what))

        self._connected = True
