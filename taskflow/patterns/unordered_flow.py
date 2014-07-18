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


class Flow(flow.Flow):
    """Unordered flow pattern.

    A unordered (potentially nested) flow of *tasks/flows* that can be
    executed in any order as one unit and rolled back as one unit.
    """

    def __init__(self, name, retry=None):
        super(Flow, self).__init__(name, retry)
        # NOTE(imelnikov): A unordered flow is unordered, so we use
        # set instead of list to save children, children so that
        # people using it don't depend on the ordering.
        self._children = set()

    def add(self, *items):
        """Adds a given task/tasks/flow/flows to this flow."""
        self._children.update(items)
        return self

    def __len__(self):
        return len(self._children)

    def __iter__(self):
        for child in self._children:
            yield child

    def iter_links(self):
        # NOTE(imelnikov): children in unordered flow have no dependencies
        # between each other due to invariants retained during construction.
        return iter(())

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
