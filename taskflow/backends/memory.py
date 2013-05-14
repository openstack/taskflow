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

from datetime import datetime

import functools
import logging
import threading

from taskflow import catalog
from taskflow import exceptions as exc
from taskflow import job
from taskflow import jobboard
from taskflow import logbook
from taskflow import utils

LOG = logging.getLogger(__name__)


def check_not_closed(meth):

    @functools.wraps(meth)
    def check(self, *args, **kwargs):
        if self._closed:
            raise exc.ClosedException("Unable to call %s on closed object" %
                                      (meth.__name__))
        return meth(self, *args, **kwargs)

    return check


class MemoryClaimer(job.Claimer):
    def claim(self, job, owner):
        # Straight foward check if already claimed.
        if job.owner is not None:
            raise exc.UnclaimableJobException()
        job.owner = owner


class MemoryCatalog(catalog.Catalog):
    def __init__(self):
        super(MemoryCatalog, self).__init__()
        self._catalogs = []
        self._closed = False
        self._lock = threading.RLock()

    def __contains__(self, job):
        with self._lock:
            for (j, b) in self._catalogs:
                if j == job:
                    return True
        return False

    def close(self):
        self._closed = True

    @check_not_closed
    def create_or_fetch(self, job):
        with self._lock:
            for (j, b) in self._catalogs:
                if j == job:
                    return b
            b = MemoryLogBook()
            self._catalogs.append((j, b))
            return b

    @check_not_closed
    def erase(self, job):
        with self._lock:
            self._catalogs = [(j, b) for (j, b) in self._catalogs if j != job]


class MemoryLogBook(logbook.LogBook):
    def __init__(self):
        super(MemoryLogBook, self).__init__()
        self._entries = []
        self._closed = False

    @check_not_closed
    def add(self, entry):
        self._entries.append(entry)

    @check_not_closed
    def __iter__(self):
        for e in self._entries:
            yield e

    def close(self):
        self._closed = True

    @check_not_closed
    def __contains__(self, name):
        for e in self:
            if e.name == name:
                return True
        return False

    @check_not_closed
    def erase(self, name):
        self._entries = [e for e in self._entries if e.name == name]


class MemoryJobBoard(jobboard.JobBoard):
    def __init__(self):
        super(MemoryJobBoard, self).__init__()
        self._event = threading.Event()
        # Locking to ensure that if there are multiple
        # users posting to the backing board that we only
        # have 1 writer modifying it at a time, but we can
        # have X readers.
        self._lock = utils.ReaderWriterLock()
        self._board = []
        self._closed = False

    def close(self):
        self._closed = True

    def _select_posts(self, date_functor):
        for (d, j) in self._board:
            if date_functor(d):
                yield j

    @check_not_closed
    def post(self, job):
        with self._lock.acquire(read=False):
            self._board.append((datetime.utcnow(), job))
        # Let people know a job is here
        self._notify_posted(job)
        self._event.set()
        # And now that they are notified, wait for another posting.
        self._event.clear()

    @check_not_closed
    def posted_before(self, date_posted=None):
        date_functor = lambda d: True
        if date_posted is not None:
            date_functor = lambda d: d < date_posted

        with self._lock.acquire(read=True):
            return [j for j in self._select_posts(date_functor)]

    @check_not_closed
    def erase(self, job):
        with self._lock.acquire(read=False):
            # Ensure that we even have said job in the first place.
            exists = False
            for (d, j) in self._board:
                if j == job:
                    exists = True
                    break
            if not exists:
                raise exc.JobNotFound()
            self._board = [(d, j) for (d, j) in self._board if j != job]

    @check_not_closed
    def posted_after(self, date_posted=None):
        date_functor = lambda d: True
        if date_posted is not None:
            date_functor = lambda d: d >= date_posted

        with self._lock.acquire(read=True):
            return [j for j in self._select_posts(date_functor)]

    @check_not_closed
    def await(self, timeout=None):
        self._event.wait(timeout)
