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

from taskflow import exceptions
from taskflow import flow


_LINK_METADATA = {'invariant': True}


class Flow(flow.Flow):
    """Linear Flow pattern.

    A linear (potentially nested) flow of *tasks/flows* that can be
    applied in order as one unit and rolled back as one unit using
    the reverse order that the *tasks/flows* have been applied in.

    NOTE(imelnikov): Tasks/flows contained in this linear flow must not
    depend on outputs (provided names/values) of tasks/flows that follow it.
    """

    def __init__(self, name, retry=None):
        super(Flow, self).__init__(name, retry)
        self._children = []

    def add(self, *items):
        """Adds a given task/tasks/flow/flows to this flow."""
        if not items:
            return self

        # NOTE(imelnikov): we add item to the end of flow, so it should
        # not provide anything previous items of the flow require.
        requires = self.requires
        provides = self.provides
        for item in items:
            requires |= item.requires
            out_of_order = requires & item.provides
            if out_of_order:
                raise exceptions.DependencyFailure(
                    "%(item)s provides %(oo)s that are required "
                    "by previous item(s) of linear flow %(flow)s"
                    % dict(item=item.name, flow=self.name,
                           oo=sorted(out_of_order)))
            same_provides = provides & item.provides
            if same_provides:
                raise exceptions.DependencyFailure(
                    "%(item)s provides %(value)s but is already being"
                    " provided by %(flow)s and duplicate producers"
                    " are disallowed"
                    % dict(item=item.name, flow=self.name,
                           value=sorted(same_provides)))
            provides |= item.provides

        self._children.extend(items)
        return self

    def __len__(self):
        return len(self._children)

    def __iter__(self):
        for child in self._children:
            yield child

    def iter_links(self):
        for src, dst in zip(self._children[:-1],
                            self._children[1:]):
            yield (src, dst, _LINK_METADATA.copy())
