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

import abc
import six

from taskflow.utils import reflection


class Flow(six.with_metaclass(abc.ABCMeta)):
    """The base abstract class of all flow implementations.

    A flow is a structure that defines relationships between tasks. You can
    add tasks and other flows (as subflows) to the flow, and the flow provides
    a way to implicitly or explicitly define how they are interdependent.
    Exact structure of the relationships is defined by concrete
    implementation, while this class defines common interface and adds
    human-readable (not necessary unique) name.

    NOTE(harlowja): if a flow is placed in another flow as a subflow, a desired
    way to compose flows together, then it is valid and permissible that during
    execution the subflow & parent flow may be flattened into a new flow. Since
    a flow is just a 'structuring' concept this is typically a behavior that
    should not be worried about (as it is not visible to the user), but it is
    worth mentioning here.

    Flows are expected to provide the following methods/properties:

    - add
    - __len__
    - requires
    - provides
    """

    def __init__(self, name):
        self._name = str(name)

    @property
    def name(self):
        """A non-unique name for this flow (human readable)"""
        return self._name

    @abc.abstractmethod
    def __len__(self):
        """Returns how many items are in this flow."""

    def __str__(self):
        lines = ["%s: %s" % (reflection.get_class_name(self), self.name)]
        lines.append("%s" % (len(self)))
        return "; ".join(lines)

    @abc.abstractmethod
    def add(self, *items):
        """Adds a given item/items to this flow."""

    @abc.abstractproperty
    def requires(self):
        """Browse argument requirement names this flow requires to run."""

    @abc.abstractproperty
    def provides(self):
        """Browse argument names provided by the flow."""
