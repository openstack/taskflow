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

from taskflow import flow
from taskflow.types import graph as gr


class Flow(flow.Flow):
    """Linear flow pattern.

    A linear (potentially nested) flow of *tasks/flows* that can be
    applied in order as one unit and rolled back as one unit using
    the reverse order that the *tasks/flows* have been applied in.
    """

    _no_last_item = object()
    """Sentinel object used to denote no last item has been assigned.

    This is used to track no last item being added, since at creation there
    is no last item, but since the :meth:`.add` routine can take any object
    including none, we have to use a different object to be able to
    distinguish the lack of any last item...
    """

    def __init__(self, name, retry=None):
        super(Flow, self).__init__(name, retry)
        self._graph = gr.OrderedDiGraph(name=name)
        self._last_item = self._no_last_item

    def add(self, *items):
        """Adds a given task/tasks/flow/flows to this flow."""
        for item in items:
            if not self._graph.has_node(item):
                self._graph.add_node(item)
                if self._last_item is not self._no_last_item:
                    self._graph.add_edge(self._last_item, item,
                                         attr_dict={flow.LINK_INVARIANT: True})
                self._last_item = item
        return self

    def __len__(self):
        return len(self._graph)

    def __iter__(self):
        for item in self._graph.nodes_iter():
            yield item

    @property
    def requires(self):
        requires = set()
        prior_provides = set()
        if self._retry is not None:
            requires.update(self._retry.requires)
            prior_provides.update(self._retry.provides)
        for item in self:
            requires.update(item.requires - prior_provides)
            prior_provides.update(item.provides)
        return frozenset(requires)

    def iter_nodes(self):
        for (n, n_data) in self._graph.nodes_iter(data=True):
            yield (n, n_data)

    def iter_links(self):
        for (u, v, e_data) in self._graph.edges_iter(data=True):
            yield (u, v, e_data)
