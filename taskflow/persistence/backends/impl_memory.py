# -*- coding: utf-8 -*-

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

import six

from taskflow import exceptions as exc
from taskflow.persistence.backends import base
from taskflow.persistence import logbook

LOG = logging.getLogger(__name__)


class MemoryBackend(base.Backend):
    """A in-memory (non-persistent) backend.

    This backend writes logbooks, flow details, and atom details to in-memory
    dictionaries and retrieves from those dictionaries as needed.
    """
    def __init__(self, conf=None):
        super(MemoryBackend, self).__init__(conf)
        self._log_books = {}
        self._flow_details = {}
        self._atom_details = {}

    @property
    def log_books(self):
        return self._log_books

    @property
    def flow_details(self):
        return self._flow_details

    @property
    def atom_details(self):
        return self._atom_details

    def get_connection(self):
        return Connection(self)

    def close(self):
        pass


class Connection(base.Connection):
    def __init__(self, backend):
        self._backend = backend

    def upgrade(self):
        pass

    def validate(self):
        pass

    @property
    def backend(self):
        return self._backend

    def close(self):
        pass

    def clear_all(self):
        count = 0
        for book_uuid in list(six.iterkeys(self.backend.log_books)):
            self.destroy_logbook(book_uuid)
            count += 1
        return count

    def destroy_logbook(self, book_uuid):
        try:
            # Do the same cascading delete that the sql layer does.
            lb = self.backend.log_books.pop(book_uuid)
            for fd in lb:
                self.backend.flow_details.pop(fd.uuid, None)
                for ad in fd:
                    self.backend.atom_details.pop(ad.uuid, None)
        except KeyError:
            raise exc.NotFound("No logbook found with id: %s" % book_uuid)

    def update_atom_details(self, atom_detail):
        try:
            e_ad = self.backend.atom_details[atom_detail.uuid]
        except KeyError:
            raise exc.NotFound("No atom details found with id: %s"
                               % atom_detail.uuid)
        return e_ad.merge(atom_detail, deep_copy=True)

    def _save_flowdetail_atoms(self, e_fd, flow_detail):
        for atom_detail in flow_detail:
            e_ad = e_fd.find(atom_detail.uuid)
            if e_ad is None:
                e_fd.add(atom_detail)
                self.backend.atom_details[atom_detail.uuid] = atom_detail
            else:
                e_ad.merge(atom_detail, deep_copy=True)

    def update_flow_details(self, flow_detail):
        try:
            e_fd = self.backend.flow_details[flow_detail.uuid]
        except KeyError:
            raise exc.NotFound("No flow details found with id: %s"
                               % flow_detail.uuid)
        e_fd.merge(flow_detail, deep_copy=True)
        self._save_flowdetail_atoms(e_fd, flow_detail)
        return e_fd

    def save_logbook(self, book):
        # Get a existing logbook model (or create it if it isn't there).
        try:
            e_lb = self.backend.log_books[book.uuid]
        except KeyError:
            e_lb = logbook.LogBook(book.name, uuid=book.uuid)
            self.backend.log_books[e_lb.uuid] = e_lb

        e_lb.merge(book, deep_copy=True)
        # Add anything in to the new logbook that isn't already in the existing
        # logbook.
        for flow_detail in book:
            try:
                e_fd = self.backend.flow_details[flow_detail.uuid]
            except KeyError:
                e_fd = logbook.FlowDetail(flow_detail.name, flow_detail.uuid)
                e_lb.add(e_fd)
                self.backend.flow_details[e_fd.uuid] = e_fd
            e_fd.merge(flow_detail, deep_copy=True)
            self._save_flowdetail_atoms(e_fd, flow_detail)
        return e_lb

    def get_logbook(self, book_uuid):
        try:
            return self.backend.log_books[book_uuid]
        except KeyError:
            raise exc.NotFound("No logbook found with id: %s" % book_uuid)

    def get_logbooks(self):
        for lb in list(six.itervalues(self.backend.log_books)):
            yield lb
