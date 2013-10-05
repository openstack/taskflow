# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#    Copyright (C) 2013 Rackspace Hosting All Rights Reserved.
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

import logging

from taskflow.openstack.common import uuidutils

LOG = logging.getLogger(__name__)


class LogBook(object):
    """This class that contains an append-only list of flow detail
    entries for a given *job* so that the job can track what 'work' has been
    completed for resumption/reverting and miscellaneous tracking purposes.

    The data contained within this class need *not* backed by the backend
    storage in real time. The data in this class will only be guaranteed to be
    persisted when a save occurs via some backend connection.
    """
    def __init__(self, name, uuid=None, updated_at=None, created_at=None):
        if uuid:
            self._uuid = uuid
        else:
            self._uuid = uuidutils.generate_uuid()
        self._name = name
        self._flowdetails = []
        self._updated_at = updated_at
        self._created_at = created_at
        self.meta = None

    @property
    def created_at(self):
        return self._created_at

    @property
    def updated_at(self):
        return self._updated_at

    def add(self, flow_detail):
        """Adds a new entry to the underlying logbook.

        Does not *guarantee* that the details will be immediatly saved.
        """
        self._flowdetails.append(flow_detail)

    def find(self, flow_uuid):
        for fd in self._flowdetails:
            if fd.uuid == flow_uuid:
                return fd
        return None

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name

    def __iter__(self):
        for fd in self._flowdetails:
            yield fd

    def __len__(self):
        return len(self._flowdetails)


class FlowDetail(object):
    """This class contains an append-only list of task detail entries for a
    given flow along with any metadata associated with that flow.

    The data contained within this class need *not* backed by the backend
    storage in real time. The data in this class will only be guaranteed to be
    persisted when a save/update occurs via some backend connection.
    """
    def __init__(self, name, uuid):
        self._uuid = uuid
        self._name = name
        self._taskdetails = []
        self.state = None
        # Any other metadata to include about this flow while storing. For
        # example timing information could be stored here, other misc. flow
        # related items (edge connections)...
        self.meta = None

    def update(self, fd):
        """Updates the objects state to be the same as the given one."""
        if fd is self:
            return
        self._taskdetails = list(fd._taskdetails)
        self.state = fd.state
        self.meta = fd.meta

    def add(self, td):
        self._taskdetails.append(td)

    def find(self, td_uuid):
        for self_td in self:
            if self_td.uuid == td_uuid:
                return self_td
        return None

    def find_by_name(self, td_name):
        for self_td in self:
            if self_td.name == td_name:
                return self_td
        return None

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name

    def __iter__(self):
        for td in self._taskdetails:
            yield td

    def __len__(self):
        return len(self._taskdetails)


class TaskDetail(object):
    """This class contains an entry that contains the persistance of a task
    after or before (or during) it is running including any results it may have
    produced, any state that it may be in (failed for example), any exception
    that occured when running and any associated stacktrace that may have
    occuring during that exception being thrown and any other metadata that
    should be stored along-side the details about this task.

    The data contained within this class need *not* backed by the backend
    storage in real time. The data in this class will only be guaranteed to be
    persisted when a save/update occurs via some backend connection.
    """
    def __init__(self, name, uuid):
        self._uuid = uuid
        self._name = name
        # TODO(harlowja): decide if these should be passed in and therefore
        # immutable or let them be assigned?
        #
        # The state the task was last in.
        self.state = None
        # The results it may have produced (useful for reverting).
        self.results = None
        # An Failure object that holds exception the task may have thrown
        # (or part of it), useful for knowing what failed.
        self.failure = None
        # Any other metadata to include about this task while storing. For
        # example timing information could be stored here, other misc. task
        # related items.
        self.meta = None
        # The version of the task this task details was associated with which
        # is quite useful for determining what versions of tasks this detail
        # information can be associated with.
        self.version = None

    def update(self, td):
        """Updates the objects state to be the same as the given one."""
        if td is self:
            return
        self.state = td.state
        self.meta = td.meta
        self.failure = td.failure
        self.results = td.results
        self.version = td.version

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name
