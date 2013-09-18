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

import copy
import logging
import threading
import weakref

from taskflow import decorators
from taskflow import exceptions as exc
from taskflow.openstack.common import timeutils
from taskflow.persistence.backends import base
from taskflow.utils import persistence_utils as p_utils

LOG = logging.getLogger(__name__)

# TODO(harlowja): we likely need to figure out a better place to put these
# rather than globals.
_LOG_BOOKS = {}
_FLOW_DETAILS = {}
_TASK_DETAILS = {}

# For now this will be a pretty big lock, since it is not expected that saves
# will be that frequent this seems ok for the time being. I imagine that this
# can be done better but it will require much more careful usage of a dict as
# a key/value map. Aka I wish python had a concurrent dict that was safe and
# known good to use.
_SAVE_LOCK = threading.RLock()
_READ_LOCK = threading.RLock()
_READ_SAVE_ORDER = (_READ_LOCK, _SAVE_LOCK)


def _copy(obj):
    return copy.deepcopy(obj)


class MemoryBackend(base.Backend):
    def get_connection(self):
        return Connection(self)

    def close(self):
        pass


class Connection(base.Connection):
    def __init__(self, backend):
        self._read_lock = _READ_LOCK
        self._save_locks = _READ_SAVE_ORDER
        self._backend = weakref.proxy(backend)

    def upgrade(self):
        pass

    @property
    def backend(self):
        return self._backend

    def close(self):
        pass

    @decorators.locked(lock="_save_locks")
    def clear_all(self):
        count = 0
        for uuid in list(_LOG_BOOKS.iterkeys()):
            self.destroy_logbook(uuid)
            count += 1
        return count

    @decorators.locked(lock="_save_locks")
    def destroy_logbook(self, book_uuid):
        try:
            # Do the same cascading delete that the sql layer does.
            lb = _LOG_BOOKS.pop(book_uuid)
            for fd in lb:
                _FLOW_DETAILS.pop(fd.uuid, None)
                for td in fd:
                    _TASK_DETAILS.pop(td.uuid, None)
        except KeyError:
            raise exc.NotFound("No logbook found with id: %s" % book_uuid)

    @decorators.locked(lock="_save_locks")
    def update_task_details(self, task_detail):
        try:
            return p_utils.task_details_merge(_TASK_DETAILS[task_detail.uuid],
                                              task_detail)
        except KeyError:
            raise exc.NotFound("No task details found with id: %s"
                               % task_detail.uuid)

    @decorators.locked(lock="_save_locks")
    def update_flow_details(self, flow_detail):
        try:
            e_fd = p_utils.flow_details_merge(_FLOW_DETAILS[flow_detail.uuid],
                                              flow_detail)
            for task_detail in flow_detail:
                if e_fd.find(task_detail.uuid) is None:
                    _TASK_DETAILS[task_detail.uuid] = _copy(task_detail)
                    e_fd.add(task_detail)
                if task_detail.uuid not in _TASK_DETAILS:
                    _TASK_DETAILS[task_detail.uuid] = _copy(task_detail)
                task_detail.update(self.update_task_details(task_detail))
            return e_fd
        except KeyError:
            raise exc.NotFound("No flow details found with id: %s"
                               % flow_detail.uuid)

    @decorators.locked(lock="_save_locks")
    def save_logbook(self, book):
        # Get a existing logbook model (or create it if it isn't there).
        try:
            e_lb = p_utils.logbook_merge(_LOG_BOOKS[book.uuid], book)
            # Add anything in to the new logbook that isn't already
            # in the existing logbook.
            for flow_detail in book:
                if e_lb.find(flow_detail.uuid) is None:
                    _FLOW_DETAILS[flow_detail.uuid] = _copy(flow_detail)
                    e_lb.add(flow_detail)
                if flow_detail.uuid not in _FLOW_DETAILS:
                    _FLOW_DETAILS[flow_detail.uuid] = _copy(flow_detail)
                flow_detail.update(self.update_flow_details(flow_detail))
            # TODO(harlowja): figure out a better way to set this property
            # without actually setting a 'private' property.
            e_lb._updated_at = timeutils.utcnow()
        except KeyError:
            # Ok the one given is now the one we will save
            e_lb = _copy(book)
            # TODO(harlowja): figure out a better way to set this property
            # without actually setting a 'private' property.
            e_lb._created_at = timeutils.utcnow()
            # Record all the pieces as being saved.
            _LOG_BOOKS[e_lb.uuid] = e_lb
            for flow_detail in e_lb:
                _FLOW_DETAILS[flow_detail.uuid] = _copy(flow_detail)
                flow_detail.update(self.update_flow_details(flow_detail))
        return e_lb

    @decorators.locked(lock='_read_lock')
    def get_logbook(self, book_uuid):
        try:
            return _LOG_BOOKS[book_uuid]
        except KeyError:
            raise exc.NotFound("No logbook found with id: %s" % book_uuid)

    def get_logbooks(self):
        # NOTE(harlowja): don't hold the lock while iterating
        with self._read_lock:
            books = list(_LOG_BOOKS.values())
        for lb in books:
            yield lb
