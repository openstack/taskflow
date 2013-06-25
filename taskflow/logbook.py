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
import datetime
import weakref


class TaskDetail(object):
    """Task details have the bare minimum of these fields/methods."""

    def __init__(self, name, metadata=None):
        self.date_created = datetime.datetime.utcnow()
        self.name = name
        self.metadata = metadata
        self.date_updated = None

    def __str__(self):
        return "TaskDetail (%s, %s): %s" % (self.name, self.date_created,
                                            self.metadata)


class FlowDetail(object):
    """Flow details have the bare minimum of these fields/methods."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, book, name):
        self.book = weakref.proxy(book)
        self.name = name

    @abc.abstractmethod
    def __iter__(self):
        """Iterates over all task details.

        The order will be in the same order that they were added."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __contains__(self, task_name):
        """Determines if any task details with the given name exists in this
        flow details."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __getitem__(self, task_name):
        """Fetch any task details that match the given task name."""
        raise NotImplementedError()

    @abc.abstractmethod
    def add_task(self, task_name, metadata=None):
        """Atomically creates a new task detail entry to this flows details and
        returns it for further use."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __delitem__(self, task_name):
        """Deletes any task details that match the given task name."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __len__(self):
        """Returns how many task details objects the flow contains."""
        raise NotImplementedError()

    def __str__(self):
        return "FlowDetail (%s): %s entries" % (self.name, len(self))


class LogBook(object):
    """Base class for what a logbook should provide"""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def add_flow(self, flow_name):
        """Atomically adds and returns a new flow details object to the given
        logbook or raises an exception if that flow (or a flow with that name)
        already exists.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def __getitem__(self, flow_name):
        """Fetches the given flow details object for the given flow
        name or raises an exception if that flow name does not exist."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __contains__(self, flow_name):
        """Determines if a flow details object with the given flow name
        exists in this logbook."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __delitem__(self, flow_name):
        """Removes the given flow details object that matches the provided
        flow name or raises an exception if that flow name does not
        exist."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __iter__(self):
        """Iterates over all the contained flow details.

        The order will be in the same order that they were added."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __len__(self):
        """Returns how many flow details the logbook contains."""
        raise NotImplementedError()

    def close(self):
        """Allows the logbook to free any resources that it has."""
        pass
