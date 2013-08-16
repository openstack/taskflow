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
import sys
import threading

from taskflow import exceptions as exc
from taskflow.openstack.common import timeutils
from taskflow import utils

LOG = logging.getLogger(__name__)

# TODO(harlowja): we likely need to figure out a better place to put these
# rather than globals.
LOG_BOOKS = {}
FLOW_DETAILS = {}
TASK_DETAILS = {}

# For now this will be a pretty big lock, since it is not expected that saves
# will be that frequent this seems ok for the time being. I imagine that this
# can be done better but it will require much more careful usage of a dict as
# a key/value map. Aka I wish python had a concurrent dict that was safe and
# known good to use.
SAVE_LOCK = threading.RLock()
READ_LOCK = threading.RLock()
READ_SAVE_ORDER = (READ_LOCK, SAVE_LOCK,)


def get_backend():
    """The backend is this module itself."""
    return sys.modules[__name__]


def _taskdetails_merge(td_e, td_new):
    """Merges an existing taskdetails with a new one."""
    if td_e.state != td_new.state:
        td_e.state = td_new.state
    if td_e.results != td_new.results:
        td_e.results = td_new.results
    if td_e.exception != td_new.exception:
        td_e.exception = td_new.exception
    if td_e.stacktrace != td_new.stacktrace:
        td_e.stacktrace = td_new.stacktrace
    if td_e.meta != td_new.meta:
        td_e.meta = td_new.meta
    return td_e


def taskdetails_save(td):
    with utils.MultiLock(READ_SAVE_ORDER):
        try:
            return _taskdetails_merge(TASK_DETAILS[td.uuid], td)
        except KeyError:
            raise exc.NotFound("No task details found with id: %s" % td.uuid)


def flowdetails_save(fd):
    try:
        with utils.MultiLock(READ_SAVE_ORDER):
            e_fd = FLOW_DETAILS[fd.uuid]
            if e_fd.meta != fd.meta:
                e_fd.meta = fd.meta
            if e_fd.state != fd.state:
                e_fd.state = fd.state
            for td in fd:
                if td not in e_fd:
                    td = copy.deepcopy(td)
                    TASK_DETAILS[td.uuid] = td
                    e_fd.add(td)
                else:
                    # Previously added but not saved into the taskdetails
                    # 'permanent' storage.
                    if td.uuid not in TASK_DETAILS:
                        TASK_DETAILS[td.uuid] = copy.deepcopy(td)
                    taskdetails_save(td)
            return e_fd
    except KeyError:
        raise exc.NotFound("No flow details found with id: %s" % fd.uuid)


def clear_all():
    with utils.MultiLock(READ_SAVE_ORDER):
        count = 0
        for lb_id in list(LOG_BOOKS.iterkeys()):
            logbook_destroy(lb_id)
            count += 1
        return count


def logbook_get(lb_id):
    try:
        with READ_LOCK:
            return LOG_BOOKS[lb_id]
    except KeyError:
        raise exc.NotFound("No logbook found with id: %s" % lb_id)


def logbook_destroy(lb_id):
    try:
        with utils.MultiLock(READ_SAVE_ORDER):
            # Do the same cascading delete that the sql layer does.
            lb = LOG_BOOKS.pop(lb_id)
            for fd in lb:
                FLOW_DETAILS.pop(fd.uuid, None)
                for td in fd:
                    TASK_DETAILS.pop(td.uuid, None)
    except KeyError:
        raise exc.NotFound("No logbook found with id: %s" % lb_id)


def logbook_save(lb):
    # Acquire all the locks that will be needed to perform this operation with
    # out being affected by other threads doing it at the same time.
    with utils.MultiLock(READ_SAVE_ORDER):
        # Get a existing logbook model (or create it if it isn't there).
        try:
            backing_lb = LOG_BOOKS[lb.uuid]
            if backing_lb.meta != lb.meta:
                backing_lb.meta = lb.meta
            # Add anything on to the existing loaded logbook that isn't already
            # in the existing logbook.
            for fd in lb:
                if fd not in backing_lb:
                    FLOW_DETAILS[fd.uuid] = copy.deepcopy(fd)
                    backing_lb.add(flowdetails_save(fd))
                else:
                    # Previously added but not saved into the flowdetails
                    # 'permanent' storage.
                    if fd.uuid not in FLOW_DETAILS:
                        FLOW_DETAILS[fd.uuid] = copy.deepcopy(fd)
                    flowdetails_save(fd)
            # TODO(harlowja): figure out a better way to set this property
            # without actually letting others set it external.
            backing_lb._updated_at = timeutils.utcnow()
        except KeyError:
            backing_lb = copy.deepcopy(lb)
            # TODO(harlowja): figure out a better way to set this property
            # without actually letting others set it external.
            backing_lb._created_at = timeutils.utcnow()
            # Record all the pieces as being saved.
            LOG_BOOKS[lb.uuid] = backing_lb
            for fd in backing_lb:
                FLOW_DETAILS[fd.uuid] = fd
                for td in fd:
                    TASK_DETAILS[td.uuid] = td
        return backing_lb
