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


class TaskFlowException(Exception):
    """Base class for exceptions emitted from this library."""
    pass


class Duplicate(TaskFlowException):
    """Raised when a duplicate entry is found."""
    pass


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


class DependencyFailure(TaskFlowException):
    """Raised when flow can't resolve dependency."""
    pass


class WrappedFailure(TaskFlowException):
    """Wraps one or several failures

    When exception cannot be re-raised (for example, because
    the value and traceback is lost in serialization) or
    there are several exceptions, we wrap corresponding Failure
    objects into this exception class.
    """

    def __init__(self, causes):
        self._causes = []
        for cause in causes:
            if cause.check(type(self)) and cause.exception:
                # NOTE(imelnikov): flatten wrapped failures
                self._causes.extend(cause.exception)
            else:
                self._causes.append(cause)

    def __iter__(self):
        """Iterate over failures that caused the exception"""
        return iter(self._causes)

    def __len__(self):
        """Return number of wrapped failures"""
        return len(self._causes)

    def check(self, *exc_classes):
        """Check if any of exc_classes caused (part of) the failure.

        Arguments of this method can be exception types or type names
        (stings). If any of wrapped failures were caused by exception
        of given type, the corresponding argument is returned. Else,
        None is returned.
        """
        if not exc_classes:
            return None
        for cause in self:
            result = cause.check(*exc_classes)
            if result is not None:
                return result
        return None

    def __str__(self):
        return 'WrappedFailure: %s' % [str(cause) for cause in self._causes]
