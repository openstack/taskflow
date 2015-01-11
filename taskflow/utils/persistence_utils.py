# -*- coding: utf-8 -*-

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
import os

from oslo_utils import timeutils
from oslo_utils import uuidutils

from taskflow import logging
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
    """Creates a temporary flow detail and logbook in the given backend.

    Mainly useful for tests and other use cases where a temporary flow detail
    and a temporary logbook is needed for a short-period of time.
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
    """Creates a flow detail for a flow & adds & saves it in a logbook.

    This will create a flow detail for the given flow using the flow name,
    and add it to the provided logbook and then uses the given backend to save
    the logbook and then returns the created flow detail.

    If no book is provided a temporary one will be created automatically (no
    reference to the logbook will be returned, so this should nearly *always*
    be provided or only used in situations where no logbook is needed, for
    example in tests). If no backend is provided then no saving will occur and
    the created flow detail will not be persisted even if the flow detail was
    added to a given (or temporarily generated) logbook.
    """
    flow_id = uuidutils.generate_uuid()
    flow_name = getattr(flow, 'name', None)
    if flow_name is None:
        LOG.warn("No name provided for flow %s (id %s)", flow, flow_id)
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
        # that the freshest version is given back.
        return book.find(flow_id)
    else:
        return flow_detail


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
        if k == 'progress' and isinstance(v, misc.NUMERIC_TYPES):
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


def pformat_atom_detail(atom_detail, indent=0):
    """Pretty formats a atom detail."""
    detail_type = logbook.atom_detail_type(atom_detail)
    lines = ["%s%s: '%s'" % (" " * (indent), detail_type, atom_detail.name)]
    lines.extend(_format_shared(atom_detail, indent=indent + 1))
    lines.append("%s- version = %s"
                 % (" " * (indent + 1), misc.get_version_string(atom_detail)))
    lines.append("%s- results = %s"
                 % (" " * (indent + 1), atom_detail.results))
    lines.append("%s- failure = %s" % (" " * (indent + 1),
                                       bool(atom_detail.failure)))
    lines.extend(_format_meta(atom_detail.meta, indent=indent + 1))
    return os.linesep.join(lines)


def pformat_flow_detail(flow_detail, indent=0):
    """Pretty formats a flow detail."""
    lines = ["%sFlow: '%s'" % (" " * indent, flow_detail.name)]
    lines.extend(_format_shared(flow_detail, indent=indent + 1))
    lines.extend(_format_meta(flow_detail.meta, indent=indent + 1))
    for task_detail in flow_detail:
        lines.append(pformat_atom_detail(task_detail, indent=indent + 1))
    return os.linesep.join(lines)


def pformat(book, indent=0):
    """Pretty formats a logbook."""
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
    return os.linesep.join(lines)
