# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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

import contextlib
import copy
import logging

from taskflow.openstack.common import uuidutils
from taskflow.persistence import logbook

LOG = logging.getLogger(__name__)


def temporary_log_book(backend=None):
    """Creates a temporary logbook for temporary usage in the given backend.

    Mainly useful for tests and other use cases where a temporary logbook
    is needed for a short-period of time.
    """
    book = logbook.LogBook('tmp')
    if backend is not None:
        with contextlib.closing(backend.get_connection()) as conn:
            conn.save_logbook(book)
    return book


def temporary_flow_detail(backend=None):
    """Creates a temporary flow detail and logbook for temporary usage in
    the given backend.

    Mainly useful for tests and other use cases where a temporary flow detail
    is needed for a short-period of time.
    """
    flow_id = uuidutils.generate_uuid()
    book = temporary_log_book(backend)
    book.add(logbook.FlowDetail(name='tmp-flow-detail', uuid=flow_id))
    if backend is not None:
        with contextlib.closing(backend.get_connection()) as conn:
            conn.save_logbook(book)
    # Return the one from the saved logbook instead of the local one so
    # that the freshest version is given back.
    return book, book.find(flow_id)


def create_flow_detail(flow, book=None, backend=None):
    """Creates a flow detail for the given flow and adds it to the provided
    logbook (if provided) and then uses the given backend (if provided) to
    save the logbook then returns the created flow detail.
    """
    try:
        flow_name = getattr(flow, 'name')
    except AttributeError:
        LOG.warn("Flow %s does not have a name attribute, creating one.", flow)
        flow_name = uuidutils.generate_uuid()
    try:
        flow_id = getattr(flow, 'uuid')
    except AttributeError:
        LOG.warn("Flow %s does not have a uuid attribute, creating one.", flow)
        flow_id = uuidutils.generate_uuid()
    flow_detail = logbook.FlowDetail(name=flow_name, uuid=flow_id)
    if book is not None:
        book.add(flow_detail)
        if backend is not None:
            with contextlib.closing(backend.get_connection()) as conn:
                conn.save_logbook(book)
        # Return the one from the saved logbook instead of the local one so
        # that the freshest version is given back
        return book.find(flow_id)
    else:
        if backend is not None:
            LOG.warn("Can not save %s without a provided logbook", flow)
        return flow_detail


def _copy_functon(deep_copy):
    if deep_copy:
        return copy.deepcopy
    else:
        return lambda x: x


def task_details_merge(td_e, td_new, deep_copy=False):
    """Merges an existing task details with a new task details object.

    The new task details fields, if they differ will replace the existing
    objects fields (except name, version, uuid which can not be replaced).

    If 'deep_copy' is True, fields are copied deeply (by value) if possible.
    """
    if td_e is td_new:
        return td_e

    copy_fn = _copy_functon(deep_copy)
    if td_e.state != td_new.state:
        # NOTE(imelnikov): states are just strings, no need to copy
        td_e.state = td_new.state
    if td_e.results != td_new.results:
        td_e.results = copy_fn(td_new.results)
    if td_e.exception != td_new.exception:
        td_e.exception = copy_fn(td_new.exception)
    if td_e.stacktrace != td_new.stacktrace:
        td_e.stacktrace = copy_fn(td_new.stacktrace)
    if td_e.meta != td_new.meta:
        td_e.meta = copy_fn(td_new.meta)
    return td_e


def flow_details_merge(fd_e, fd_new, deep_copy=False):
    """Merges an existing flow details with a new flow details object.

    The new flow details fields, if they differ will replace the existing
    objects fields (except name and uuid which can not be replaced).

    If 'deep_copy' is True, fields are copied deeply (by value) if possible.
    """
    if fd_e is fd_new:
        return fd_e

    copy_fn = _copy_functon(deep_copy)
    if fd_e.meta != fd_new.meta:
        fd_e.meta = copy_fn(fd_new.meta)
    if fd_e.state != fd_new.state:
        # NOTE(imelnikov): states are just strings, no need to copy
        fd_e.state = fd_new.state
    return fd_e


def logbook_merge(lb_e, lb_new, deep_copy=False):
    """Merges an existing logbook with a new logbook object.

    The new logbook fields, if they differ will replace the existing
    objects fields (except name and uuid which can not be replaced).

    If 'deep_copy' is True, fields are copied deeply (by value) if possible.
    """
    if lb_e is lb_new:
        return lb_e

    copy_fn = _copy_functon(deep_copy)
    if lb_e.meta != lb_new.meta:
        lb_e.meta = copy_fn(lb_new.meta)
    return lb_e
