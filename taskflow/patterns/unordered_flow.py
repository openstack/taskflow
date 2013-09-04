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

from taskflow import flow


class Flow(flow.Flow):
    """"Unordered Flow pattern.

    A unordered (potentially nested) flow of *tasks/flows* that can be
    executed in any order as one unit and rolled back as one unit.

    NOTE(harlowja): Since the flow is unordered there can *not* be any
    dependency between task inputs and task outputs.
    """

    def __init__(self, name, uuid=None):
        super(Flow, self).__init__(name, uuid)
        # A unordered flow is unordered so use a dict that is indexed by
        # names instead of a list so that people using this flow don't depend
        # on the ordering.
        self._children = collections.defaultdict(list)
        self._count = 0

    def add(self, *items):
        """Adds a given task/tasks/flow/flows to this flow."""
        for e in [self._extract_item(item) for item in items]:
            self._children[e.name].append(e)
            self._count += 1
        return self

    def __len__(self):
        return self._count

    def __iter__(self):
        for _n, group in self._children.iteritems():
            for g in group:
                yield g
