# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import threading

from taskflow.openstack.common import uuidutils
from taskflow.utils import lock_utils


class Job(object):
    """A job is a higher level abstraction over a set of flows as well as the
    *ownership* of those flows, it is the highest piece of work that can be
    owned by an entity performing those flows.

    Only one entity will be operating on the flows contained in a job at a
    given time (for the foreseeable future).

    It is the object that should be transferred to another entity on failure of
    so that the contained flows ownership can be transferred to the secondary
    entity for resumption/continuation/reverting.
    """

    def __init__(self, name, uuid=None):
        if uuid:
            self._uuid = uuid
        else:
            self._uuid = uuidutils.generate_uuid()
        self._name = name
        self._lock = threading.RLock()
        self._flows = []
        self.owner = None
        self.state = None
        self.book = None

    @lock_utils.locked
    def add(self, *flows):
        self._flows.extend(flows)

    @lock_utils.locked
    def remove(self, flow):
        j = -1
        for i, f in enumerate(self._flows):
            if f.uuid == flow.uuid:
                j = i
                break
        if j == -1:
            raise ValueError("Could not find %r to remove" % (flow))
        self._flows.pop(j)

    def __contains__(self, flow):
        for f in self:
            if f.uuid == flow.uuid:
                return True
        return False

    @property
    def uuid(self):
        """The uuid of this job"""
        return self._uuid

    @property
    def name(self):
        """The non-uniquely identifying name of this job"""
        return self._name

    def __iter__(self):
        # Don't iterate while holding the lock
        with self._lock:
            flows = list(self._flows)
        for f in flows:
            yield f
