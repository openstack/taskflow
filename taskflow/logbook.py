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

from datetime import datetime


class Entry(object):
    """A logbook entry has the bare minimum of these fields."""

    def __init__(self, name, metadata=None):
        self.created_on = datetime.utcnow()
        self.name = name
        self.metadata = metadata


class LogBook(object):
    """Base class for what a logbook (distributed or local or in-between)
    should provide"""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def add(self, entry):
        """Atomically adds a new entry to the given logbook."""
        raise NotImplementedError()

    def search_by_name(self, name):
        """Yields entries with the given name. The order will be in the same
        order that they were added."""
        for e in self:
            if e.name == name:
                yield e

    @abc.abstractmethod
    def __contains__(self, name):
        """Determines if any entry with the given name exists in this
        logbook."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __iter__(self):
        """Iterates over all entries. The order will be in the same order that
        they were added."""
        raise NotImplementedError()

    @abc.abstractmethod
    def erase(self, name):
        """Erases any entries with given name."""
        raise NotImplementedError()

    def close(self):
        """Allows the logbook to free any resources that it has."""
        pass
