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

"""Implementation of in-memory backend."""

import logging
import threading

from taskflow import exceptions as exc
from taskflow.openstack.common import timeutils
from taskflow.persistence.backends import base
from taskflow.persistence import logbook
from taskflow.utils import lock_utils
from taskflow.utils import persistence_utils as p_utils

LOG = logging.getLogger(__name__)


class MemoryBackend(base.Backend):
    def __init__(self, conf):
        super(MemoryBackend, self).__init__(conf)
        self._log_books = {}
        self._flow_details = {}
        self._task_details = {}
        self._save_lock = threading.RLock()
        self._read_lock = threading.RLock()
        self._read_save_order = (self._read_lock, self._save_lock)

    @property
    def log_books(self):
        return self._log_books

    @property
    def flow_details(self):
        return self._flow_details

    @property
    def task_details(self):
        return self._task_details

    @property
    def read_locks(self):
        return (self._read_lock,)

    @property
    def save_locks(self):
        return self._read_save_order

    def get_connection(self):
        return Connection(self)

    def close(self):
        pass


class Connection(base.Connection):
    def __init__(self, backend):
        self._read_locks = backend.read_locks
        self._save_locks = backend.save_locks
        self._backend = backend

    def upgrade(self):
        pass

    @property
    def backend(self):
        return self._backend

    def close(self):
        pass

    @lock_utils.locked(lock="_save_locks")
    def clear_all(self):
        count = 0
        for uuid in list(self.backend.log_books.keys()):
            self.destroy_logbook(uuid)
            count += 1
        return count

    @lock_utils.locked(lock="_save_locks")
    def destroy_logbook(self, book_uuid):
        try:
            # Do the same cascading delete that the sql layer does.
            lb = self.backend.log_books.pop(book_uuid)
            for fd in lb:
                self.backend.flow_details.pop(fd.uuid, None)
                for td in fd:
                    self.backend.task_details.pop(td.uuid, None)
        except KeyError:
            raise exc.NotFound("No logbook found with id: %s" % book_uuid)

    @lock_utils.locked(lock="_save_locks")
    def update_task_details(self, task_detail):
        try:
            e_td = self.backend.task_details[task_detail.uuid]
        except KeyError:
            raise exc.NotFound("No task details found with id: %s"
                               % task_detail.uuid)
        return p_utils.task_details_merge(e_td, task_detail, deep_copy=True)

    def _save_flowdetail_tasks(self, e_fd, flow_detail):
        for task_detail in flow_detail:
            e_td = e_fd.find(task_detail.uuid)
            if e_td is None:
                e_td = logbook.TaskDetail(name=task_detail.name,
                                          uuid=task_detail.uuid)
                e_fd.add(e_td)
            if task_detail.uuid not in self.backend.task_details:
                self.backend.task_details[task_detail.uuid] = e_td
            p_utils.task_details_merge(e_td, task_detail, deep_copy=True)

    @lock_utils.locked(lock="_save_locks")
    def update_flow_details(self, flow_detail):
        try:
            e_fd = self.backend.flow_details[flow_detail.uuid]
        except KeyError:
            raise exc.NotFound("No flow details found with id: %s"
                               % flow_detail.uuid)
        p_utils.flow_details_merge(e_fd, flow_detail, deep_copy=True)
        self._save_flowdetail_tasks(e_fd, flow_detail)
        return e_fd

    @lock_utils.locked(lock="_save_locks")
    def save_logbook(self, book):
        # Get a existing logbook model (or create it if it isn't there).
        try:
            e_lb = self.backend.log_books[book.uuid]
        except KeyError:
            e_lb = logbook.LogBook(book.name, book.uuid,
                                   updated_at=book.updated_at,
                                   created_at=timeutils.utcnow())
            self.backend.log_books[e_lb.uuid] = e_lb
        else:
            # TODO(harlowja): figure out a better way to set this property
            # without actually setting a 'private' property.
            e_lb._updated_at = timeutils.utcnow()

        p_utils.logbook_merge(e_lb, book, deep_copy=True)
        # Add anything in to the new logbook that isn't already
        # in the existing logbook.
        for flow_detail in book:
            try:
                e_fd = self.backend.flow_details[flow_detail.uuid]
            except KeyError:
                e_fd = logbook.FlowDetail(name=flow_detail.name,
                                          uuid=flow_detail.uuid)
                e_lb.add(flow_detail)
                self.backend.flow_details[flow_detail.uuid] = e_fd
            p_utils.flow_details_merge(e_fd, flow_detail, deep_copy=True)
            self._save_flowdetail_tasks(e_fd, flow_detail)
        return e_lb

    @lock_utils.locked(lock='_read_locks')
    def get_logbook(self, book_uuid):
        try:
            return self.backend.log_books[book_uuid]
        except KeyError:
            raise exc.NotFound("No logbook found with id: %s" % book_uuid)

    @lock_utils.locked(lock='_read_locks')
    def _get_logbooks(self):
        return list(self.backend.log_books.values())

    def get_logbooks(self):
        # NOTE(harlowja): don't hold the lock while iterating
        for lb in self._get_logbooks():
            yield lb
