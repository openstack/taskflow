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

import abc


class JobBoard(object):
    """Base class for job boards."""

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._listeners = []

    @abc.abstractmethod
    def post(self, job):
        """Posts a job to the job board."""
        raise NotImplementedError()

    @abc.abstractmethod
    def repost(self, job):
        """Reposts a job that already exists on the job board."""
        raise NotImplementedError()

    def _notify_posted(self, job):
        """When a job is received, by whichever mechanism the underlying
        implementation provides, the job should be given to said listeners
        for them to know that a job has arrived."""
        for i in self._listeners:
            i.notify_posted(job)

    def _notify_erased(self, job):
        """When a job is erased, by whichever mechanism the underlying
        implementation provides, the job should be given to said listeners
        for them to know that a job has been erased."""
        for i in self._listeners:
            i.notify_erased(job)

    @abc.abstractmethod
    def posted_after(self, date_posted=None):
        """Gets the jobs posted after (or equal to) the given datetime object
        (or all jobs if none)."""
        raise NotImplementedError()

    @abc.abstractmethod
    def posted_before(self, date_posted=None):
        """Gets the jobs posted before the given datetime object
        (or all jobs if none)."""
        raise NotImplementedError()

    @abc.abstractmethod
    def erase(self, job):
        """Erases the given job from this job board."""
        raise NotImplementedError()

    @abc.abstractmethod
    def await(self, timeout=None):
        """Blocks the current thread until a new job has arrived."""
        raise NotImplementedError()

    def subscribe(self, listener):
        """Adds a new listener who will be notified on job updates/postings."""
        if listener not in self._listeners:
            self._listeners.append(listener)

    def unsubscribe(self, listener):
        """Removes a given listener from notifications about job
        updates/postings."""
        if listener in self._listeners:
            self._listeners.remove(listener)

    def close(self):
        """Allows the job board to free any resources that it has."""
        pass
