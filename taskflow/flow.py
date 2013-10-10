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

from taskflow.openstack.common import uuidutils
from taskflow.utils import reflection


class Flow(six.with_metaclass(abc.ABCMeta)):
    """The base abstract class of all flow implementations.

    It provides a name and an identifier (uuid or other) to the flow so that
    it can be uniquely identifed among many flows.

    Flows are expected to provide the following methods:
    - add
    - __len__
    """

    def __init__(self, name, uuid=None):
        self._name = str(name)
        if uuid:
            self._id = str(uuid)
        else:
            self._id = uuidutils.generate_uuid()

    @property
    def name(self):
        """A non-unique name for this flow (human readable)"""
        return self._name

    @property
    def uuid(self):
        return self._id

    @abc.abstractmethod
    def __len__(self):
        """Returns how many items are in this flow."""
        raise NotImplementedError()

    def __str__(self):
        lines = ["%s: %s" % (reflection.get_class_name(self), self.name)]
        lines.append("%s" % (self.uuid))
        lines.append("%s" % (len(self)))
        return "; ".join(lines)

    @abc.abstractmethod
    def add(self, *items):
        """Adds a given item/items to this flow."""
        raise NotImplementedError()

    @abc.abstractproperty
    def requires(self):
        """Browse flow requirements."""

    @abc.abstractproperty
    def provides(self):
        """Browse values provided by the flow."""
