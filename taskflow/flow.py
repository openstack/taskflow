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

from oslo_utils import reflection
import six

# Link metadata keys that have inherent/special meaning.
#
# This key denotes the link is an invariant that ensures the order is
# correctly preserved.
LINK_INVARIANT = 'invariant'
# This key denotes the link is a manually/user-specified.
LINK_MANUAL = 'manual'
# This key denotes the link was created when resolving/compiling retries.
LINK_RETRY = 'retry'
# This key denotes the link was created due to symbol constraints and the
# value will be a set of names that the constraint ensures are satisfied.
LINK_REASONS = 'reasons'
#
# This key denotes a callable that will determine if the target is visited.
LINK_DECIDER = 'decider'

# Chop off full module names of patterns that are built-in to taskflow...
_CHOP_PAT = "taskflow.patterns."
_CHOP_PAT_LEN = len(_CHOP_PAT)

# This key denotes the depth the decider will apply (defaulting to all).
LINK_DECIDER_DEPTH = 'decider_depth'


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
    compilation the subflow & parent flow *may* be flattened into a new flow.
    """

    def __init__(self, name, retry=None):
        self._name = six.text_type(name)
        self._retry = retry
        # NOTE(akarpinska): if retry doesn't have a name,
        # the name of its owner will be assigned
        if self._retry is not None and self._retry.name is None:
            self._retry.name = self.name + "_retry"

    @property
    def name(self):
        """A non-unique name for this flow (human readable)."""
        return self._name

    @property
    def retry(self):
        """The associated flow retry controller.

        This retry controller object will affect & control how (and if) this
        flow and its contained components retry when execution is underway and
        a failure occurs.
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

    @abc.abstractmethod
    def iter_nodes(self):
        """Iterate over nodes of the flow.

        Iterates over 2-tuples ``(A, meta)``, where
            * ``A`` is a child (atom or subflow) of current flow;
            * ``meta`` is link metadata, a dictionary.
        """

    def __str__(self):
        cls_name = reflection.get_class_name(self)
        if cls_name.startswith(_CHOP_PAT):
            cls_name = cls_name[_CHOP_PAT_LEN:]
        return "%s: %s(len=%d)" % (cls_name, self.name, len(self))

    @property
    def provides(self):
        """Set of symbol names provided by the flow."""
        provides = set()
        if self._retry is not None:
            provides.update(self._retry.provides)
        for item in self:
            provides.update(item.provides)
        return frozenset(provides)

    @abc.abstractproperty
    def requires(self):
        """Set of *unsatisfied* symbol names required by the flow."""
