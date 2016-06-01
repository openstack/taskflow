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

from oslo_utils import excutils
from oslo_utils import reflection
import six
from taskflow.utils import mixins


def raise_with_cause(exc_cls, message, *args, **kwargs):
    """Helper to raise + chain exceptions (when able) and associate a *cause*.

    NOTE(harlowja): Since in py3.x exceptions can be chained (due to
    :pep:`3134`) we should try to raise the desired exception with the given
    *cause* (or extract a *cause* from the current stack if able) so that the
    exception formats nicely in old and new versions of python. Since py2.x
    does **not** support exception chaining (or formatting) our root exception
    class has a :py:meth:`~taskflow.exceptions.TaskFlowException.pformat`
    method that can be used to get *similar* information instead (and this
    function makes sure to retain the *cause* in that case as well so
    that the :py:meth:`~taskflow.exceptions.TaskFlowException.pformat` method
    shows them).

    :param exc_cls: the :py:class:`~taskflow.exceptions.TaskFlowException`
                    class to raise.
    :param message: the text/str message that will be passed to
                    the exceptions constructor as its first positional
                    argument.
    :param args: any additional positional arguments to pass to the
                 exceptions constructor.
    :param kwargs: any additional keyword arguments to pass to the
                   exceptions constructor.
    """
    if not issubclass(exc_cls, TaskFlowException):
        raise ValueError("Subclass of taskflow exception is required")
    excutils.raise_with_cause(exc_cls, message, *args, **kwargs)


class TaskFlowException(Exception):
    """Base class for *most* exceptions emitted from this library.

    NOTE(harlowja): in later versions of python we can likely remove the need
    to have a ``cause`` here as PY3+ have implemented :pep:`3134` which
    handles chaining in a much more elegant manner.

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

    def __str__(self):
        return self.pformat()

    def _get_message(self):
        # We must *not* call into the __str__ method as that will reactivate
        # the pformat method, which will end up badly (and doesn't look
        # pretty at all); so be careful...
        return self.args[0]

    def pformat(self, indent=2, indent_text=" ", show_root_class=False):
        """Pretty formats a taskflow exception + any connected causes."""
        if indent < 0:
            raise ValueError("Provided 'indent' must be greater than"
                             " or equal to zero instead of %s" % indent)
        buf = six.StringIO()
        if show_root_class:
            buf.write(reflection.get_class_name(self, fully_qualified=False))
            buf.write(": ")
        buf.write(self._get_message())
        active_indent = indent
        next_up = self.cause
        seen = []
        while next_up is not None and next_up not in seen:
            seen.append(next_up)
            buf.write(os.linesep)
            if isinstance(next_up, TaskFlowException):
                buf.write(indent_text * active_indent)
                buf.write(reflection.get_class_name(next_up,
                                                    fully_qualified=False))
                buf.write(": ")
                buf.write(next_up._get_message())
            else:
                lines = traceback.format_exception_only(type(next_up), next_up)
                for i, line in enumerate(lines):
                    buf.write(indent_text * active_indent)
                    if line.endswith("\n"):
                        # We'll add our own newlines on...
                        line = line[0:-1]
                    buf.write(line)
                    if i + 1 != len(lines):
                        buf.write(os.linesep)
            if not isinstance(next_up, TaskFlowException):
                # Don't go deeper into non-taskflow exceptions... as we
                # don't know if there exception 'cause' attributes are even
                # useable objects...
                break
            active_indent += indent
            next_up = getattr(next_up, 'cause', None)
        return buf.getvalue()


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

    METHOD_TPL = "'%(method)s' method on "

    def __init__(self, who, requirements, cause=None, method=None):
        message = self.MESSAGE_TPL % {'who': who, 'requirements': requirements}
        if method:
            message = (self.METHOD_TPL % {'method': method}) + message
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


class DisallowedAccess(TaskFlowException):
    """Raised when storage access is not possible due to state limitations."""

    def __init__(self, message, cause=None, state=None):
        super(DisallowedAccess, self).__init__(message, cause=cause)
        self.state = state


# Others.

class NotImplementedError(NotImplementedError):
    """Exception for when some functionality really isn't implemented.

    This is typically useful when the library itself needs to distinguish
    internal features not being made available from users features not being
    made available/implemented (and to avoid misinterpreting the two).
    """


class WrappedFailure(mixins.StrMixin, Exception):
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

    def __bytes__(self):
        buf = six.BytesIO()
        buf.write(b'WrappedFailure: [')
        causes_gen = (six.binary_type(cause) for cause in self._causes)
        buf.write(b", ".join(causes_gen))
        buf.write(b']')
        return buf.getvalue()

    def __unicode__(self):
        buf = six.StringIO()
        buf.write(u'WrappedFailure: [')
        causes_gen = (six.text_type(cause) for cause in self._causes)
        buf.write(u", ".join(causes_gen))
        buf.write(u']')
        return buf.getvalue()
