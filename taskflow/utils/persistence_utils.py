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

from oslo_utils import uuidutils

from taskflow import logging
from taskflow.persistence import models

LOG = logging.getLogger(__name__)


def temporary_log_book(backend=None):
    """Creates a temporary logbook for temporary usage in the given backend.

    Mainly useful for tests and other use cases where a temporary logbook
    is needed for a short-period of time.
    """
    book = models.LogBook('tmp')
    if backend is not None:
        with contextlib.closing(backend.get_connection()) as conn:
            conn.save_logbook(book)
    return book


def temporary_flow_detail(backend=None, meta=None):
    """Creates a temporary flow detail and logbook in the given backend.

    Mainly useful for tests and other use cases where a temporary flow detail
    and a temporary logbook is needed for a short-period of time.
    """
    flow_id = uuidutils.generate_uuid()
    book = temporary_log_book(backend)

    flow_detail = models.FlowDetail(name='tmp-flow-detail', uuid=flow_id)
    if meta is not None:
        if flow_detail.meta is None:
            flow_detail.meta = {}
        flow_detail.meta.update(meta)
    book.add(flow_detail)

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

    flow_detail = models.FlowDetail(name=flow_name, uuid=flow_id)
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
