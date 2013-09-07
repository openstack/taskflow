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
import logging

from taskflow.openstack.common import uuidutils
from taskflow.persistence import logbook

LOG = logging.getLogger(__name__)


def temporary_log_book(backend):
    """Creates a temporary logbook for temporary usage in the given backend.

    Mainly useful for tests and other use cases where a temporary logbook
    is needed for a short-period of time.
    """
    book = logbook.LogBook('tmp')
    with contextlib.closing(backend.get_connection()) as conn:
        conn.save_logbook(book)
        return book


def temporary_flow_detail(backend):
    """Creates a temporary flow detail and logbook for temporary usage in
    the given backend.

    Mainly useful for tests and other use cases where a temporary flow detail
    is needed for a short-period of time.
    """
    flow_id = uuidutils.generate_uuid()
    book = temporary_log_book(backend)
    with contextlib.closing(backend.get_connection()) as conn:
        book.add(logbook.FlowDetail(name='tmp-flow-detail', uuid=flow_id))
        conn.save_logbook(book)
        # Return the one from the saved logbook instead of the local one so
        # that the freshest version is given back.
        return (book, book.find(flow_id))


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
