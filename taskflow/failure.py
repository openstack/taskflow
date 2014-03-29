# -*- coding: utf-8 -*-

#    Copyright (C) 2013-2014 Yahoo! Inc. All Rights Reserved.
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
import sys
import traceback

import six

from taskflow import exceptions as exc
from taskflow.utils import misc
from taskflow.utils import reflection


@contextlib.contextmanager
def wrap_all_failures():
    """Convert any exceptions to WrappedFailure.

    When you expect several failures, it may be convenient
    to wrap any exception with WrappedFailure in order to
    unify error handling.
    """
    try:
        yield
    except Exception:
        raise exc.WrappedFailure([Failure()])


@contextlib.contextmanager
def capture_failure():
    """Save current exception, and yield back the failure (or raises a
    runtime error if no active exception is being handled).

    In some cases the exception context can be cleared, resulting in None
    being attempted to be saved after an exception handler is run. This
    can happen when eventlet switches greenthreads or when running an
    exception handler, code raises and catches an exception. In both
    cases the exception context will be cleared.

    To work around this, we save the exception state, yield a failure and
    then run other code.

    For example::

      except Exception:
        with capture_failure() as fail:
            LOG.warn("Activating cleanup")
            cleanup()
            save_failure(fail)
    """
    exc_info = sys.exc_info()
    if not any(exc_info):
        raise RuntimeError("No active exception is being handled")
    else:
        yield Failure(exc_info=exc_info)


class Failure(object):
    """Object that represents failure.

    Failure objects encapsulate exception information so that
    it can be re-used later to re-raise or inspect.
    """
    DICT_VERSION = 1

    def __init__(self, exc_info=None, **kwargs):
        if not kwargs:
            if exc_info is None:
                exc_info = sys.exc_info()
            self._exc_info = exc_info
            self._exc_type_names = list(
                reflection.get_all_class_names(exc_info[0], up_to=Exception))
            if not self._exc_type_names:
                raise TypeError('Invalid exception type: %r' % exc_info[0])
            self._exception_str = exc.exception_message(self._exc_info[1])
            self._traceback_str = ''.join(
                traceback.format_tb(self._exc_info[2]))
        else:
            self._exc_info = exc_info  # may be None
            self._exception_str = kwargs.pop('exception_str')
            self._exc_type_names = kwargs.pop('exc_type_names', [])
            self._traceback_str = kwargs.pop('traceback_str', None)
            if kwargs:
                raise TypeError(
                    'Failure.__init__ got unexpected keyword argument(s): %s'
                    % ', '.join(six.iterkeys(kwargs)))

    @classmethod
    def from_exception(cls, exception):
        return cls((type(exception), exception, None))

    def _matches(self, other):
        if self is other:
            return True
        return (self._exc_type_names == other._exc_type_names
                and self.exception_str == other.exception_str
                and self.traceback_str == other.traceback_str)

    def matches(self, other):
        if not isinstance(other, Failure):
            return False
        if self.exc_info is None or other.exc_info is None:
            return self._matches(other)
        else:
            return self == other

    def __eq__(self, other):
        if not isinstance(other, Failure):
            return NotImplemented
        return (self._matches(other) and
                misc.are_equal_exc_info_tuples(self.exc_info, other.exc_info))

    def __ne__(self, other):
        return not (self == other)

    # NOTE(imelnikov): obj.__hash__() should return same values for equal
    # objects, so we should redefine __hash__. Failure equality semantics
    # is a bit complicated, so for now we just mark Failure objects as
    # unhashable. See python docs on object.__hash__  for more info:
    # http://docs.python.org/2/reference/datamodel.html#object.__hash__
    __hash__ = None

    @property
    def exception(self):
        """Exception value, or None if exception value is not present.

        Exception value may be lost during serialization.
        """
        if self._exc_info:
            return self._exc_info[1]
        else:
            return None

    @property
    def exception_str(self):
        """String representation of exception."""
        return self._exception_str

    @property
    def exc_info(self):
        """Exception info tuple or None."""
        return self._exc_info

    @property
    def traceback_str(self):
        """Exception traceback as string."""
        return self._traceback_str

    @staticmethod
    def reraise_if_any(failures):
        """Re-raise exceptions if argument is not empty.

        If argument is empty list, this method returns None. If
        argument is list with single Failure object in it,
        this failure is reraised. Else, WrappedFailure exception
        is raised with failures list as causes.
        """
        failures = list(failures)
        if len(failures) == 1:
            failures[0].reraise()
        elif len(failures) > 1:
            raise exc.WrappedFailure(failures)

    def reraise(self):
        """Re-raise captured exception."""
        if self._exc_info:
            six.reraise(*self._exc_info)
        else:
            raise exc.WrappedFailure([self])

    def check(self, *exc_classes):
        """Check if any of exc_classes caused the failure.

        Arguments of this method can be exception types or type
        names (stings). If captured exception is instance of
        exception of given type, the corresponding argument is
        returned. Else, None is returned.
        """
        for cls in exc_classes:
            if isinstance(cls, type):
                err = reflection.get_class_name(cls)
            else:
                err = cls
            if err in self._exc_type_names:
                return cls
        return None

    def __str__(self):
        return 'Failure: %s: %s' % (self._exc_type_names[0],
                                    self._exception_str)

    def __iter__(self):
        """Iterate over exception type names."""
        for et in self._exc_type_names:
            yield et

    @classmethod
    def from_dict(cls, data):
        data = dict(data)
        version = data.pop('version', None)
        if version != cls.DICT_VERSION:
            raise ValueError('Invalid dict version of failure object: %r'
                             % version)
        return cls(**data)

    def to_dict(self):
        return {
            'exception_str': self.exception_str,
            'traceback_str': self.traceback_str,
            'exc_type_names': list(self),
            'version': self.DICT_VERSION,
        }

    def copy(self):
        return Failure(exc_info=misc.copy_exc_info(self.exc_info),
                       exception_str=self.exception_str,
                       traceback_str=self.traceback_str,
                       exc_type_names=self._exc_type_names[:])
