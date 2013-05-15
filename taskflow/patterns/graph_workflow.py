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

from collections import defaultdict

import logging

from networkx import exception as g_exc
from networkx.algorithms import dag
from networkx.classes import digraph

from taskflow import exceptions as exc
from taskflow import ordered_workflow

LOG = logging.getLogger(__name__)


class Workflow(ordered_workflow.Workflow):
    """A workflow which will analyze the attached tasks input requirements and
    determine who provides said input and order the task so that said providing
    task will be ran before."""

    def __init__(self, name, tolerant=False, parents=None):
        super(Workflow, self).__init__(name, tolerant, parents)
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

    def run(self, context, *args, **kwargs):
        self.connect()
        return super(Workflow, self).run(context, *args, **kwargs)

    def order(self):
        self.connect()
        try:
            return dag.topological_sort(self._graph)
        except g_exc.NetworkXUnfeasible:
            raise exc.InvalidStateException("Unable to correctly determine "
                                            "the path through the provided "
                                            "workflow which will satisfy the "
                                            "tasks needed inputs and outputs.")

    def connect(self):
        """Connects the edges of the graph together."""
        if self._connected:
            return
        provides_what = defaultdict(list)
        requires_what = defaultdict(list)
        for t in self._graph.nodes_iter():
            for r in t.requires:
                requires_what[r].append(t)
            for p in t.provides:
                provides_what[p].append(t)
        for (i_want, n) in requires_what.items():
            if i_want not in provides_what:
                raise exc.InvalidStateException("Task %s requires input %s "
                                                "but no other task produces "
                                                "said output" % (n, i_want))
            for p in provides_what[i_want]:
                # P produces for N so thats why we link P->N and not N->P
                self._graph.add_edge(p, n)

        self._connected = True
