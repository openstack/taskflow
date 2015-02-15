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

import os
import traceback

import six


class TaskFlowException(Exception):
    """Base class for *most* exceptions emitted from this library.

    NOTE(harlowja): in later versions of python we can likely remove the need
    to have a cause here as PY3+ have implemented PEP 3134 which handles
    chaining in a much more elegant manner.

    :param message: the exception message, typically some string that is
                    useful for consumers to view when debugging or analyzing
                    failures.
    :param cause: the cause of the exception being raised, when provided this
                  should itself be an exception instance, this is useful for
                  creating a chain of exceptions for versions of python where
                  this is not yet implemented/supported natively.
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
        return os.linesep.join(self._pformat(self, [], 0,
                                             indent=indent,
                                             indent_text=indent_text))

    @classmethod
    def _pformat(cls, excp, lines, current_indent, indent=2, indent_text=" "):
        line_prefix = indent_text * current_indent
        for line in traceback.format_exception_only(type(excp), excp):
            # We'll add our own newlines on at the end of formatting.
            #
            # NOTE(harlowja): the reason we don't search for os.linesep is
            # that the traceback module seems to only use '\n' (for some
            # reason).
            if line.endswith("\n"):
                line = line[0:-1]
            lines.append(line_prefix + line)
        try:
            cause = excp.cause
        except AttributeError:
            pass
        else:
            if cause is not None:
                cls._pformat(cause, lines, current_indent + indent,
                             indent=indent, indent_text=indent_text)
        return lines


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
    """Raised when a worker request was not finished within allotted time."""


class InvalidState(ExecutionFailure):
    """Raised when a invalid state transition is attempted while executing."""


# Other errors that do not fit the above categories (at the current time).


class DependencyFailure(TaskFlowException):
    """Raised when some type of dependency problem occurs."""


class AmbiguousDependency(DependencyFailure):
    """Raised when some type of ambiguous dependency problem occurs."""


class MissingDependencies(DependencyFailure):
    """Raised when a entity has dependencies that can not be satisfied.

    :param who: the entity that caused the missing dependency to be triggered.
    :param requirements: the dependency which were not satisfied.

    Further arguments are interpreted as for in
    :py:class:`~taskflow.exceptions.TaskFlowException`.
    """

    #: Exception message template used when creating an actual message.
    MESSAGE_TPL = ("'%(who)s' requires %(requirements)s but no other entity"
                   " produces said requirements")

    def __init__(self, who, requirements, cause=None):
        message = self.MESSAGE_TPL % {'who': who, 'requirements': requirements}
        super(MissingDependencies, self).__init__(message, cause=cause)
        self.missing_requirements = requirements


class CompilationFailure(TaskFlowException):
    """Raised when some type of compilation issue is found."""


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


class InvalidFormat(TaskFlowException):
    """Raised when some object/entity is not in the expected format."""


# Others.

class NotImplementedError(NotImplementedError):
    """Exception for when some functionality really isn't implemented.

    This is typically useful when the library itself needs to distinguish
    internal features not being made available from users features not being
    made available/implemented (and to avoid misinterpreting the two).
    """


class WrappedFailure(Exception):
    """Wraps one or several failure objects.

    When exception/s cannot be re-raised (for example, because the value and
    traceback are lost in serialization) or there are several exceptions active
    at the same time (due to more than one thread raising exceptions), we will
    wrap the corresponding failure objects into this exception class and
    *may* reraise this exception type to allow users to handle the contained
    failures/causes as they see fit...

    See the failure class documentation for a more comprehensive set of reasons
    why this object *may* be reraised instead of the original exception.

    :param causes: the :py:class:`~taskflow.types.failure.Failure` objects
                   that caused this this exception to be raised.
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
        """Check if any of exception classes caused the failure/s.

        :param exc_classes: exception types/exception type names to
                            search for.

        If any of the contained failures were caused by an exception of a
        given type, the corresponding argument that matched is returned. If
        not then none is returned.
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
    """Return the string representation of exception.

    :param exc: exception object to get a string representation of.
    """
    # NOTE(imelnikov): Dealing with non-ascii data in python is difficult:
    # https://bugs.launchpad.net/taskflow/+bug/1275895
    # https://bugs.launchpad.net/taskflow/+bug/1276053
    try:
        return six.text_type(exc)
    except UnicodeError:
        return str(exc)
