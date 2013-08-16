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


class LogBook(object):
    """This class that contains an append-only list of flow detail
    entries for a given *job* so that the job can track what 'work' has been
    completed for resumption/reverting and miscellaneous tracking purposes.

    The data contained within this class need *not* backed by the backend
    storage in real time. The data in this class will only be guaranteed to be
    persisted when the logbook is saved.
    """
    def __init__(self, name, uuid=None, updated_at=None, created_at=None,
                 backend='memory'):
        if uuid:
            self._uuid = uuid
        else:
            self._uuid = uuidutils.generate_uuid()
        self._name = name
        self._flowdetails = []
        self._updated_at = updated_at
        self._created_at = created_at
        self.backend = backend
        self.meta = None

    def _get_backend(self):
        if not self.backend:
            return None
        return b_api.fetch(self.backend)

    @property
    def created_at(self):
        return self._created_at

    @property
    def updated_at(self):
        return self._updated_at

    def save(self):
        """Saves all the components of the given logbook.

        This will immediately and atomically save all the entries of the given
        logbook, including any flow details, and any associated task details,
        that may be contained in this logbook to a backing store providing by
        the backing api.

        If the logbook is the underlying storage contains an existing logbook
        then this save operation will merge the different flow details objects
        to that logbook and then reflect those changes in this logbook.
        """
        backend = self._get_backend()
        if not backend:
            raise NotImplementedError("No saving backend provided")
        s_book = backend.logbook_save(self)
        if s_book is self:
            return
        # Alter the internal variables to reflect what was saved (which may
        # have new additions if there was a merge with pre-existing data).
        self._name = s_book._name
        self._flowdetails = s_book._flowdetails
        self._updated_at = s_book._updated_at
        self._created_at = s_book._created_at
        self.meta = s_book.meta

    def delete(self):
        """Deletes all traces of the given logbook.

        This will delete the logbook entry, any associated flow detail entries
        and any associated task detail entries associated with those flow
        detail entries immediately via the backing api (using a atomic
        transaction).
        """
        backend = self._get_backend()
        if not backend:
            raise NotImplementedError("No deleting backend provided")
        backend.logbook_destroy(self.uuid)

    def add(self, flow_detail):
        """Adds a new entry to the underlying logbook.

        Does not *guarantee* that the details will be immediatly saved.
        """
        self._flowdetails.append(flow_detail)
        # When added the backend that the flow details (and any owned task
        # details) is using will be automatically switched to whatever backend
        # this logbook is using.
        if flow_detail.backend != self.backend:
            flow_detail.backend = self.backend
        for task_detail in flow_detail:
            if task_detail.backend != self.backend:
                task_detail.backend = self.backend

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


def load(lb_id, backend='memory'):
    """Loads a given logbook (if it exists) from the given backend type."""
    return b_api.fetch(backend).logbook_get(lb_id)
