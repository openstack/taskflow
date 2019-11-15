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
    """Unordered flow pattern.

    A unordered (potentially nested) flow of *tasks/flows* that can be
    executed in any order as one unit and rolled back as one unit.
    """

    def __init__(self, name, retry=None):
        super(Flow, self).__init__(name, retry)
        self._graph = gr.Graph(name=name)

    def add(self, *items):
        """Adds a given task/tasks/flow/flows to this flow."""
        for item in items:
            if not self._graph.has_node(item):
                self._graph.add_node(item)
        return self

    def __len__(self):
        return len(self._graph)

    def __iter__(self):
        for item in self._graph:
            yield item

    def iter_links(self):
        for (u, v, e_data) in self._graph.edges(data=True):
            yield (u, v, e_data)

    def iter_nodes(self):
        for n, n_data in self._graph.nodes(data=True):
            yield (n, n_data)

    @property
    def requires(self):
        requires = set()
        retry_provides = set()
        if self._retry is not None:
            requires.update(self._retry.requires)
            retry_provides.update(self._retry.provides)
        for item in self:
            item_requires = item.requires - retry_provides
            requires.update(item_requires)
        return frozenset(requires)
