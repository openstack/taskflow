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
import weakref

from datetime import datetime


class TaskDetail(object):
    """Task details have the bare minimum of these fields/methods."""

    def __init__(self, name, metadata=None):
        self.date_created = datetime.utcnow()
        self.name = name
        self.metadata = metadata

    def __str__(self):
        return "TaskDetail (%s, %s): %s" % (self.name, self.date_created,
                                            self.metadata)


class WorkflowDetail(object):
    """Workflow details have the bare minimum of these fields/methods."""

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
        workflow details."""
        raise NotImplementedError()

    @abc.abstractmethod
    def fetch_tasks(self, task_name):
        """Fetch any task details that match the given task name."""
        raise NotImplementedError()

    @abc.abstractmethod
    def add_task(self, task_details):
        """Adds a task detail entry to this workflow details."""
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_tasks(self, task_name):
        """Deletes any task details that match the given task name."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __len__(self):
        """Returns how many task details objects the workflow contains."""
        raise NotImplementedError()

    def __str__(self):
        return "WorkflowDetail (%s): %s entries" % (self.name, len(self))


class LogBook(object):
    """Base class for what a logbook should provide"""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def add_workflow(self, workflow_name):
        """Atomically adds a new workflow details object to the given logbook
        or raises an exception if that workflow (or a workflow with
        that name) already exists.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def fetch_workflow(self, workflow_name):
        """Fetches the given workflow details object for the given workflow
        name or raises an exception if that workflow name does not exist."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __contains__(self, workflow_name):
        """Determines if a workflow details object with the given workflow name
        exists in this logbook."""
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_workflow(self, workflow_name):
        """Removes the given workflow details object that matches the provided
        workflow name or raises an exception if that workflow name does not
        exist."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __iter__(self):
        """Iterates over all the contained workflow details.

        The order will be in the same order that they were added."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __len__(self):
        """Returns how many workflow details the logbook contains."""
        raise NotImplementedError()

    def close(self):
        """Allows the logbook to free any resources that it has."""
        pass
