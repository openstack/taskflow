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

from taskflow.openstack.common import timeutils
from taskflow.openstack.common import uuidutils
from taskflow.persistence import logbook
from taskflow.utils import misc

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


def create_flow_detail(flow, book=None, backend=None, meta=None):
    """Creates a flow detail for the given flow and adds it to the provided
    logbook (if provided) and then uses the given backend (if provided) to
    save the logbook then returns the created flow detail.
    """
    flow_id = uuidutils.generate_uuid()
    flow_name = getattr(flow, 'name', None)
    if flow_name is None:
        LOG.warn("No name provided for flow %s (id %s)" % (flow, flow_id))
        flow_name = flow_id

    flow_detail = logbook.FlowDetail(name=flow_name, uuid=flow_id)
    if meta is not None:
        if flow_detail.meta is None:
            flow_detail.meta = {}
        flow_detail.meta.update(meta)

    if backend is not None and book is None:
        LOG.warn("No logbook provided for flow %s, creating one.", flow)
        book = temporary_log_book(backend)

    if book is not None:
        book.add(flow_detail)
        if backend is not None:
            with contextlib.closing(backend.get_connection()) as conn:
                conn.save_logbook(book)
        # Return the one from the saved logbook instead of the local one so
        # that the freshest version is given back
        return book.find(flow_id)
    else:
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
    if td_e.failure != td_new.failure:
        # NOTE(imelnikov): we can't just deep copy Failures, as they
        # contain tracebacks, which are not copyable.
        if deep_copy:
            td_e.failure = td_new.failure.copy()
        else:
            td_e.failure = td_new.failure
    if td_e.meta != td_new.meta:
        td_e.meta = copy_fn(td_new.meta)
    if td_e.version != td_new.version:
        td_e.version = copy_fn(td_new.version)
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


def failure_to_dict(failure):
    """Convert misc.Failure object to JSON-serializable dict"""
    if not failure:
        return None
    if not isinstance(failure, misc.Failure):
        raise TypeError('Failure object expected, but got %r'
                        % failure)
    return {
        'exception_str': failure.exception_str,
        'traceback_str': failure.traceback_str,
        'exc_type_names': list(failure),
        'version': 1
    }


def failure_from_dict(data):
    """Restore misc.Failure object from dict.

    The dict should be similar to what failure_to_dict() function
    produces.
    """
    if not data:
        return None
    version = data.pop('version', None)
    if version != 1:
        raise ValueError('Invalid version of saved Failure object: %r'
                         % version)
    return misc.Failure(**data)


def _format_meta(metadata, indent):
    """Format the common metadata dictionary in the same manner."""
    if not metadata:
        return []
    lines = [
        '%s- metadata:' % (" " * indent),
    ]
    for (k, v) in metadata.items():
        # Progress for now is a special snowflake and will be formatted
        # in percent format.
        if k == 'progress' and isinstance(v, (float, int, long)):
            v = "%0.2f%%" % (v * 100.0)
        lines.append("%s+ %s = %s" % (" " * (indent + 2), k, v))
    return lines


def _format_shared(obj, indent):
    """Format the common shared attributes in the same manner."""
    if obj is None:
        return []
    lines = []
    for attr_name in ("uuid", "state"):
        if not hasattr(obj, attr_name):
            continue
        lines.append("%s- %s = %s" % (" " * indent, attr_name,
                                      getattr(obj, attr_name)))
    return lines


def pformat_task_detail(task_detail, indent=0):
    """Pretty formats a task detail"""
    lines = ["%sTask: '%s'" % (" " * (indent), task_detail.name)]
    lines.extend(_format_shared(task_detail, indent=indent + 1))
    lines.append("%s- version = %s"
                 % (" " * (indent + 1), misc.get_version_string(task_detail)))
    lines.append("%s- results = %s"
                 % (" " * (indent + 1), task_detail.results))
    lines.append("%s- failure = %s" % (" " * (indent + 1),
                                       bool(task_detail.failure)))
    lines.extend(_format_meta(task_detail.meta, indent=indent + 1))
    return "\n".join(lines)


def pformat_flow_detail(flow_detail, indent=0):
    """Pretty formats a flow detail"""
    lines = ["%sFlow: '%s'" % (" " * indent, flow_detail.name)]
    lines.extend(_format_shared(flow_detail, indent=indent + 1))
    lines.extend(_format_meta(flow_detail.meta, indent=indent + 1))
    for task_detail in flow_detail:
        lines.append(pformat_task_detail(task_detail, indent=indent + 1))
    return "\n".join(lines)


def pformat(book, indent=0):
    """Pretty formats a logbook"""
    lines = ["%sLogbook: '%s'" % (" " * indent, book.name)]
    lines.extend(_format_shared(book, indent=indent + 1))
    lines.extend(_format_meta(book.meta, indent=indent + 1))
    if book.created_at is not None:
        lines.append("%s- created_at = %s"
                     % (" " * (indent + 1),
                        timeutils.isotime(book.created_at)))
    if book.updated_at is not None:
        lines.append("%s- updated_at = %s"
                     % (" " * (indent + 1),
                        timeutils.isotime(book.updated_at)))
    for flow_detail in book:
        lines.append(pformat_flow_detail(flow_detail, indent=indent + 1))
    return "\n".join(lines)
