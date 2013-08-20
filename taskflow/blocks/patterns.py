
# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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


from taskflow.blocks import base


class Pattern(base.Block):
    """Base class for patterns that can contain nested blocks

    Patterns put child blocks into *structure*.
    """

    def __init__(self):
        super(Pattern, self).__init__()
        self._children = []

    @property
    def children(self):
        return self._children

    def add(self, *children):
        self._children.extend(children)
        return self


class LinearFlow(Pattern):
    """Linear (sequential) pattern

    Children of this pattern should be executed one after another,
    in order. Every child implicitly depends on all the children
    before it.
    """


class ParallelFlow(Pattern):
    """Parallel (unordered) pattern

    Children of this pattern are independent, and thus can be
    executed in any order or in parallel.
    """
