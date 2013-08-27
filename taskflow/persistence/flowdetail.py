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

from taskflow.persistence.backends import api as b_api


class FlowDetail(object):
    """This class contains an append-only list of task detail entries for a
    given flow along with any metadata associated with that flow.

    The data contained within this class need *not* backed by the backend
    storage in real time. The data in this class will only be guaranteed to be
    persisted when the logbook that contains this flow detail is saved or when
    the save() method is called directly.
    """
    def __init__(self, name, uuid, backend='memory'):
        self._uuid = uuid
        self._name = name
        self._taskdetails = []
        self.state = None
        # Any other metadata to include about this flow while storing. For
        # example timing information could be stored here, other misc. flow
        # related items (edge connections)...
        self.meta = None
        self.backend = backend

    def _get_backend(self):
        if not self.backend:
            return None
        return b_api.fetch(self.backend)

    def add(self, td):
        self._taskdetails.append(td)
        # When added the backend that the task details is using will be
        # automatically switched to whatever backend this flow details is
        # using.
        if td.backend != self.backend:
            td.backend = self.backend

    def find(self, td_uuid):
        for self_td in self:
            if self_td.uuid == td_uuid:
                return self_td
        return None

    def save(self):
        """Saves *most* of the components of this given object.

        This will immediately and atomically save the attributes of this flow
        details object to a backing store providing by the backing api.

        The underlying storage must contain an existing flow details that this
        save operation will merge with and then reflect those changes in this
        object.
        """
        backend = self._get_backend()
        if not backend:
            raise NotImplementedError("No saving backend provided")
        fd_u = backend.flowdetails_save(self)
        if fd_u is self:
            return
        self.meta = fd_u.meta
        self.state = fd_u.state
        self._taskdetails = fd_u._taskdetails

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
