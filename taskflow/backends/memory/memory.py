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

from taskflow.generics import flow
from taskflow.generics import flowdetail
from taskflow.generics import job
from taskflow.generics import jobboard
from taskflow.generics import logbook
from taskflow.generics import task
from taskflow.generics import taskdetail
from taskflow import utils


class MemoryJobBoard(jobboard.JobBoard):
    def __init__(self, name, jb_id=None):
        super(MemoryJobBoard, self).__init__(name, jb_id)
        self._lock = utils.ReaderWriterLock()

    @contextlib.contextmanager
    def acquire_lock(self, read=True):
        try:
            self._lock.acquire(read)
            yield self._lock
        finally:
            self._lock.release()


class MemoryJob(job.Job):
    def __init__(self, name, job_id=None):
        super(MemoryJob, self).__init__(name, job_id)
        self._lock = utils.ReaderWriterLock()

    @contextlib.contextmanager
    def acquire_lock(self, read=True):
        try:
            self._lock.acquire(read)
            yield self._lock
        finally:
            self._lock.release()


class MemoryLogBook(logbook.LogBook):
    def __init__(self, name, lb_id=None):
        super(MemoryLogBook, self).__init__(name, lb_id)
        self._lock = utils.ReaderWriterLock()

    @contextlib.contextmanager
    def acquire_lock(self, read=True):
        try:
            self._lock.acquire(read)
            yield self._lock
        finally:
            self._lock.release()


class MemoryFlow(flow.Flow):
    def __init__(self, name, wf_id=None):
        super(MemoryFlow, self).__init__(name, wf_id)
        self._lock = utils.ReaderWriterLock()
        self.flowdetails = {}

    @contextlib.contextmanager
    def acquire_lock(self, read=True):
        try:
            self._lock.acquire(read)
            yield self._lock
        finally:
            self._lock.release()


class MemoryFlowDetail(flowdetail.FlowDetail):
    def __init__(self, name, wf, fd_id=None):
        super(MemoryFlowDetail, self).__init__(name, wf, fd_id)
        self._lock = utils.ReaderWriterLock()

    @contextlib.contextmanager
    def acquire_lock(self, read=True):
        try:
            self._lock.acquire(read)
            yield self._lock
        finally:
            self._lock.release()


class MemoryTask(task.Task):
    def __init__(self, name, task_id=None):
        super(MemoryTask, self).__init__(name, task_id)
        self._lock = utils.ReaderWriterLock()
        self.taskdetails = {}

    @contextlib.contextmanager
    def acquire_lock(self, read=True):
        try:
            self._lock.acquire(read)
            yield self._lock
        finally:
            self._lock.release()


class MemoryTaskDetail(taskdetail.TaskDetail):
    def __init__(self, name, task, td_id=None):
        super(MemoryTaskDetail, self).__init__(name, task, td_id)
        self._lock = utils.ReaderWriterLock()

    @contextlib.contextmanager
    def acquire_lock(self, read=True):
        try:
            self._lock.acquire(read)
            yield self._lock
        finally:
            self._lock.release()
