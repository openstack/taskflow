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

from oslo.config import cfg

"""Define APIs for the logbook providers."""

from nova.openstack.common import importutils
from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class RecordNotFound(Exception):
    pass


class LogBook(object):
    """Base class for what a logbook (distributed or local or in-between)
    should provide"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, resource_uri):
        self.uri = resource_uri

    @abc.abstractmethod
    def add_record(self, name, metadata=None):
        """Atomically adds a new entry to the given logbook with the supplied
        metadata (if any)."""
        raise NotImplementedError()

    @abc.abstractmethod
    def fetch_record(self, name):
        """Fetchs a record with the given name and returns any metadata about
        said record."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __contains__(self, name):
        """Determines if any entry with the given name exists in this
        logbook."""
        raise NotImplementedError()

    @abc.abstractmethod
    def mark(self, name, metadata, merge_functor=None):
        """Marks the given logbook entry (which must exist) with the given
        metadata, if said entry already exists then the provided merge functor
        or a default function, will be activated to merge the existing metadata
        with the supplied metadata."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __iter__(self):
        """Iterates over all names and metadata and provides back both of these
        via a (name, metadata) tuple. The order will be in the same order that
        they were added."""
        raise NotImplementedError()

    def close(self):
        """Allows the job board provider to free any resources that it has."""
        pass


class DBLogBook(LogBook):
    """Base class for a logbook impl that uses a backing database."""

    def __init__(self, context, job):
        super(DBLogBook, self).__init__(job.uri)
        self.context = context
        self.job = job

    def close(self):
        # Free the db connection
        pass


class MemoryLogBook(LogBook):
    pass