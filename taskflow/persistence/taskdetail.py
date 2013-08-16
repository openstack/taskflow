# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
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

from taskflow.openstack.common import uuidutils
from taskflow.persistence.backends import api as b_api


class TaskDetail(object):
    """This class contains an entry that contains the persistance of a task
    after or before (or during) it is running including any results it may have
    produced, any state that it may be in (failed for example), any exception
    that occured when running and any associated stacktrace that may have
    occuring during that exception being thrown and any other metadata that
    should be stored along-side the details about this task.

    The data contained within this class need *not* backed by the backend
    storage in real time. The data in this class will only be guaranteed to be
    persisted when the logbook that contains this task detail is saved or when
    the save() method is called directly.
    """
    def __init__(self, name, task=None, uuid=None, backend='memory'):
        if uuid:
            self._uuid = uuid
        else:
            self._uuid = uuidutils.generate_uuid()
        self._name = name
        # TODO(harlowja): decide if these should be passed in and therefore
        # immutable or let them be assigned?
        #
        # The state the task was last in.
        self.state = None
        # The results it may have produced (useful for reverting).
        self.results = None
        # An exception that it may have thrown (or part of it), useful for
        # knowing what failed.
        self.exception = None
        # Any stack trace the exception may have had, useful for debugging or
        # examining the failure in more depth.
        self.stacktrace = None
        # Any other metadata to include about this task while storing. For
        # example timing information could be stored here, other misc. task
        # related items.
        self.meta = None
        # The version of the task this task details was associated with which
        # is quite useful for determining what versions of tasks this detail
        # information can be associated with.
        self.version = None
        if task and task.version:
            if isinstance(task.version, basestring):
                self.version = str(task.version)
            elif isinstance(task.version, (tuple, list)):
                self.version = '.'.join([str(p) for p in task.version])
        self.backend = backend

    def save(self):
        """Saves *most* of the components of this given object.

        This will immediately and atomically save the attributes of this task
        details object to a backing store providing by the backing api.

        The underlying storage must contain an existing task details that this
        save operation will merge with and then reflect those changes in this
        object.
        """
        backend = self._get_backend()
        if not backend:
            raise NotImplementedError("No saving backend provided")
        td_u = backend.taskdetails_save(self)
        if td_u is self:
            return
        self.meta = td_u.meta
        self.exception = td_u.exception
        self.results = td_u.results
        self.stacktrace = td_u.stacktrace
        self.state = td_u.state

    def _get_backend(self):
        if not self.backend:
            return None
        return b_api.fetch(self.backend)

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name
