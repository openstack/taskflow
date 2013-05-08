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
        raise NotImplementedError()

    def _notify_posted(self, job):
        for i in self._listeners:
            i.notify_posted(job)

    @abc.abstractmethod
    def await(self, blocking=True):
        raise NotImplementedError()

    def subscribe(self, listener):
        self._listeners.append(listener)

    def unsubscribe(self, listener):
        if listener in self._listeners:
            self._listeners.remove(listener)

    def close(self):
        """Allows the job board provider to free any resources that it has."""
        pass


class ProxyJobBoard(JobBoard):
    def post(self, context, job):
        
        raise NotImplementedError()

