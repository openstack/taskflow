# -*- coding: utf-8 -*-

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

import traceback

import six


class TaskFlowException(Exception):
    """Base class for *most* exceptions emitted from this library.

    NOTE(harlowja): in later versions of python we can likely remove the need
    to have a cause here as PY3+ have implemented PEP 3134 which handles
    chaining in a much more elegant manner.
    """
    def __init__(self, message, cause=None):
        super(TaskFlowException, self).__init__(message)
        self._cause = cause

    @property
    def cause(self):
        return self._cause

    def pformat(self, indent=2, indent_text=" "):
        """Pretty formats a taskflow exception + any connected causes."""
        if indent < 0:
            raise ValueError("indent must be greater than or equal to zero")

        def _format(excp, indent_by):
            lines = []
            for line in traceback.format_exception_only(type(excp), excp):
                # We'll add our own newlines on at the end of formatting.
                if line.endswith("\n"):
                    line = line[0:-1]
                lines.append((indent_text * indent_by) + line)
            try:
                lines.extend(_format(excp.cause, indent_by + indent))
            except AttributeError:
                pass
            return lines

        return "\n".join(_format(self, 0))


# Errors related to storage or operations on storage units.

class StorageFailure(TaskFlowException):
    """Raised when storage backends can not be read/saved/deleted."""


# Conductor related errors.

class ConductorFailure(TaskFlowException):
    """Errors related to conducting activities."""


# Job related errors.

class JobFailure(TaskFlowException):
    """Errors related to jobs or operations on jobs."""


class UnclaimableJob(JobFailure):
    """Raised when a job can not be claimed."""


# Engine/ during execution related errors.

class ExecutionFailure(TaskFlowException):
    """Errors related to engine execution."""


class RequestTimeout(ExecutionFailure):
    """Raised when a worker request was not finished within an allotted
    timeout.
    """


class InvalidState(ExecutionFailure):
    """Raised when a invalid state transition is attempted while executing."""


# Other errors that do not fit the above categories (at the current time).


class DependencyFailure(TaskFlowException):
    """Raised when some type of dependency problem occurs."""


class MissingDependencies(DependencyFailure):
    """Raised when a entity has dependencies that can not be satisfied."""
    MESSAGE_TPL = ("%(who)s requires %(requirements)s but no other entity"
                   " produces said requirements")

    def __init__(self, who, requirements, cause=None):
        message = self.MESSAGE_TPL % {'who': who, 'requirements': requirements}
        super(MissingDependencies, self).__init__(message, cause=cause)
        self.missing_requirements = requirements


class IncompatibleVersion(TaskFlowException):
    """Raised when some type of version incompatibility is found."""


class Duplicate(TaskFlowException):
    """Raised when a duplicate entry is found."""


class NotFound(TaskFlowException):
    """Raised when some entry in some object doesn't exist."""


class Empty(TaskFlowException):
    """Raised when some object is empty when it shouldn't be."""


class MultipleChoices(TaskFlowException):
    """Raised when some decision can't be made due to many possible choices."""


# Others.

class WrappedFailure(Exception):
    """Wraps one or several failures.

    When exception cannot be re-raised (for example, because
    the value and traceback is lost in serialization) or
    there are several exceptions, we wrap corresponding Failure
    objects into this exception class.
    """

    def __init__(self, causes):
        super(WrappedFailure, self).__init__()
        self._causes = []
        for cause in causes:
            if cause.check(type(self)) and cause.exception:
                # NOTE(imelnikov): flatten wrapped failures.
                self._causes.extend(cause.exception)
            else:
                self._causes.append(cause)

    def __iter__(self):
        """Iterate over failures that caused the exception."""
        return iter(self._causes)

    def __len__(self):
        """Return number of wrapped failures."""
        return len(self._causes)

    def check(self, *exc_classes):
        """Check if any of exc_classes caused (part of) the failure.

        Arguments of this method can be exception types or type names
        (strings). If any of wrapped failures were caused by exception
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
        causes = [exception_message(cause) for cause in self._causes]
        return 'WrappedFailure: %s' % causes


def exception_message(exc):
    """Return the string representation of exception."""
    # NOTE(imelnikov): Dealing with non-ascii data in python is difficult:
    # https://bugs.launchpad.net/taskflow/+bug/1275895
    # https://bugs.launchpad.net/taskflow/+bug/1276053
    try:
        return six.text_type(exc)
    except UnicodeError:
        return str(exc)
