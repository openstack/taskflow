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

import logging

from taskflow import exceptions as exc
from taskflow import job
from taskflow import jobboard
from taskflow import logbook

LOG = logging.getLogger(__name__)


class MemoryJob(job.Job):
    pass


class MemoryLogBook(logbook.LogBook):
    def __init__(self, resource_uri):
        super(MemoryLogBook, self).__init__(resource_uri)
        self._entries = {}

    def add_record(self, name, metadata):
        if name in self._entries:
            raise exc.RecordAlreadyExists()
        self._entries[name] = metadata

    def fetch_record(self, name):
        if name not in self._entries:
            raise exc.RecordNotFound()
        return self._entries[name]


class MemoryJobBoard(jobboard.JobBoard):
    pass
