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


_LINK_METADATA = {flow.LINK_INVARIANT: True}


class Flow(flow.Flow):
    """Linear flow pattern.

    A linear (potentially nested) flow of *tasks/flows* that can be
    applied in order as one unit and rolled back as one unit using
    the reverse order that the *tasks/flows* have been applied in.
    """

    def __init__(self, name, retry=None):
        super(Flow, self).__init__(name, retry)
        self._children = []

    def add(self, *items):
        """Adds a given task/tasks/flow/flows to this flow."""
        items = [i for i in items if i not in self._children]
        self._children.extend(items)
        return self

    def __len__(self):
        return len(self._children)

    def __iter__(self):
        for child in self._children:
            yield child

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

    def iter_links(self):
        for src, dst in zip(self._children[:-1], self._children[1:]):
            yield (src, dst, _LINK_METADATA.copy())
