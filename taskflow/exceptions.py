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

import StringIO

import traceback


class TaskFlowException(Exception):
    """Base class for exceptions emitted from this library."""
    pass


class Duplicate(TaskFlowException):
    """Raised when a duplicate entry is found."""
    pass


class LinkedException(TaskFlowException):
    """A linked chain of many exceptions."""
    def __init__(self, message, cause, tb):
        super(LinkedException, self).__init__(message)
        self.cause = cause
        self.tb = tb
        self.next = None

    @classmethod
    def link(cls, exc_infos):
        first = None
        previous = None
        for i, exc_info in enumerate(exc_infos):
            if not all(exc_info[0:2]) or len(exc_info) != 3:
                raise ValueError("Invalid exc_info for index %s" % (i))
            buf = StringIO.StringIO()
            traceback.print_exception(exc_info[0], exc_info[1], exc_info[2],
                                      file=buf)
            exc = cls(str(exc_info[1]), exc_info[1], buf.getvalue())
            if previous is not None:
                previous.next = exc
            else:
                first = exc
            previous = exc
        return first


class StorageError(TaskFlowException):
    """Raised when logbook can not be read/saved/deleted."""

    def __init__(self, message, cause=None):
        super(StorageError, self).__init__(message)
        self.cause = cause


class NotFound(TaskFlowException):
    """Raised when some entry in some object doesn't exist."""
    pass


class AlreadyExists(TaskFlowException):
    """Raised when some entry in some object already exists."""
    pass


class ClosedException(TaskFlowException):
    """Raised when an access on a closed object occurs."""
    pass


class InvalidStateException(TaskFlowException):
    """Raised when a task/job/workflow is in an invalid state when an
    operation is attempting to apply to said task/job/workflow.
    """
    pass


class InvariantViolationException(TaskFlowException):
    """Raised when flow invariant violation is attempted."""
    pass


class UnclaimableJobException(TaskFlowException):
    """Raised when a job can not be claimed."""
    pass


class JobNotFound(TaskFlowException):
    """Raised when a job entry can not be found."""
    pass


class MissingDependencies(InvalidStateException):
    """Raised when a entity has dependencies that can not be satisified."""
    message = ("%(who)s requires %(requirements)s but no other entity produces"
               " said requirements")

    def __init__(self, who, requirements):
        message = self.message % {'who': who, 'requirements': requirements}
        super(MissingDependencies, self).__init__(message)
        self.missing_requirements = requirements
