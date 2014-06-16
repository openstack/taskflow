# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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


@six.add_metaclass(abc.ABCMeta)
class Job(object):
    """A abstraction that represents a named and trackable unit of work.

    A job connects a logbook, a owner, last modified and created on dates and
    any associated state that the job has. Since it is a connector to a
    logbook, which are each associated with a set of factories that can create
    set of flows, it is the current top-level container for a piece of work
    that can be owned by an entity (typically that entity will read those
    logbooks and run any contained flows).

    Only one entity will be allowed to own and operate on the flows contained
    in a job at a given time (for the foreseeable future).

    NOTE(harlowja): It is the object that will be transferred to another
    entity on failure so that the contained flows ownership can be
    transferred to the secondary entity/owner for resumption, continuation,
    reverting...
    """

    def __init__(self, name, uuid=None, details=None):
        if uuid:
            self._uuid = uuid
        else:
            self._uuid = uuidutils.generate_uuid()
        self._name = name
        if not details:
            details = {}
        self._details = details

    @abc.abstractproperty
    def last_modified(self):
        """The datetime the job was last modified."""
        pass

    @abc.abstractproperty
    def created_on(self):
        """The datetime the job was created on."""
        pass

    @abc.abstractproperty
    def board(self):
        """The board this job was posted on or was created from."""

    @abc.abstractproperty
    def state(self):
        """The current state of this job."""

    @abc.abstractproperty
    def book(self):
        """Logbook associated with this job.

        If no logbook is associated with this job, this property is None.
        """

    @abc.abstractproperty
    def book_uuid(self):
        """UUID of logbook associated with this job.

        If no logbook is associated with this job, this property is None.
        """

    @abc.abstractproperty
    def book_name(self):
        """Name of logbook associated with this job.

        If no logbook is associated with this job, this property is None.
        """

    @property
    def uuid(self):
        """The uuid of this job."""
        return self._uuid

    @property
    def details(self):
        """A dictionary of any details associated with this job."""
        return self._details

    @property
    def name(self):
        """The non-uniquely identifying name of this job."""
        return self._name

    def __str__(self):
        """Pretty formats the job into something *more* meaningful."""
        return "%s %s (%s): %s" % (type(self).__name__,
                                   self.name, self.uuid, self.details)
