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

import abc

import six

from taskflow.utils import reflection


@six.add_metaclass(abc.ABCMeta)
class Flow(object):
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
    """

    def __init__(self, name, retry=None):
        self._name = six.text_type(name)
        self._retry = retry
        # NOTE(akarpinska): if retry doesn't have a name,
        # the name of its owner will be assigned
        if self._retry and self._retry.name is None:
            self._retry.name = self.name + "_retry"

    @property
    def name(self):
        """A non-unique name for this flow (human readable)."""
        return self._name

    @property
    def retry(self):
        """A retry object that will affect control how (and if) this flow
        retries while execution is underway.
        """
        return self._retry

    @abc.abstractmethod
    def add(self, *items):
        """Adds a given item/items to this flow."""

    @abc.abstractmethod
    def __len__(self):
        """Returns how many items are in this flow."""

    @abc.abstractmethod
    def __iter__(self):
        """Iterates over the children of the flow."""

    @abc.abstractmethod
    def iter_links(self):
        """Iterates over dependency links between children of the flow.

        Iterates over 3-tuples ``(A, B, meta)``, where
            * ``A`` is a child (atom or subflow) link starts from;
            * ``B`` is a child (atom or subflow) link points to; it is
              said that ``B`` depends on ``A`` or ``B`` requires ``A``;
            * ``meta`` is link metadata, a dictionary.
        """

    def __str__(self):
        lines = ["%s: %s" % (reflection.get_class_name(self), self.name)]
        lines.append("%s" % (len(self)))
        return "; ".join(lines)

    @property
    def provides(self):
        """Set of result names provided by the flow.

        Includes names of all the outputs provided by atoms of this flow.
        """
        provides = set()
        if self._retry:
            provides.update(self._retry.provides)
        for subflow in self:
            provides.update(subflow.provides)
        return provides

    @property
    def requires(self):
        """Set of argument names required by the flow.

        Includes names of all the inputs required by atoms of this
        flow, but not provided within the flow itself.
        """
        requires = set()
        if self._retry:
            requires.update(self._retry.requires)
        for subflow in self:
            requires.update(subflow.requires)
        return requires - self.provides
