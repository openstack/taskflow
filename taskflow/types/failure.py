# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

import collections
import copy
import os
import sys
import traceback

from oslo_utils import encodeutils
from oslo_utils import reflection
import six

from taskflow import exceptions as exc
from taskflow.utils import iter_utils
from taskflow.utils import mixins
from taskflow.utils import schema_utils as su


_exception_message = encodeutils.exception_to_unicode


def _copy_exc_info(exc_info):
    if exc_info is None:
        return None
    exc_type, exc_value, tb = exc_info
    # NOTE(imelnikov): there is no need to copy the exception type, and
    # a shallow copy of the value is fine and we can't copy the traceback since
    # it contains reference to the internal stack frames...
    return (exc_type, copy.copy(exc_value), tb)


def _are_equal_exc_info_tuples(ei1, ei2):
    if ei1 == ei2:
        return True
    if ei1 is None or ei2 is None:
        return False  # if both are None, we returned True above

    # NOTE(imelnikov): we can't compare exceptions with '=='
    # because we want exc_info be equal to it's copy made with
    # copy_exc_info above.
    if ei1[0] is not ei2[0]:
        return False
    # NOTE(dhellmann): The flake8/pep8 error E721 does not apply here
    # because we want the types to be exactly the same, not just have
    # one be inherited from the other.
    if not all((type(ei1[1]) == type(ei2[1]),  # noqa: E721
                _exception_message(ei1[1]) == _exception_message(ei2[1]),
                repr(ei1[1]) == repr(ei2[1]))):
        return False
    if ei1[2] == ei2[2]:
        return True
    tb1 = traceback.format_tb(ei1[2])
    tb2 = traceback.format_tb(ei2[2])
    return tb1 == tb2


class Failure(mixins.StrMixin):
    """An immutable object that represents failure.

    Failure objects encapsulate exception information so that they can be
    re-used later to re-raise, inspect, examine, log, print, serialize,
    deserialize...

    One example where they are depended upon is in the WBE engine. When a
    remote worker throws an exception, the WBE based engine will receive that
    exception and desire to reraise it to the user/caller of the WBE based
    engine for appropriate handling (this matches the behavior of non-remote
    engines). To accomplish this a failure object (or a
    :py:meth:`~.Failure.to_dict` form) would be sent over the WBE channel
    and the WBE based engine would deserialize it and use this objects
    :meth:`.reraise` method to cause an exception that contains
    similar/equivalent information as the original exception to be reraised,
    allowing the user (or the WBE engine itself) to then handle the worker
    failure/exception as they desire.

    For those who are curious, here are a few reasons why the original
    exception itself *may* not be reraised and instead a reraised wrapped
    failure exception object will be instead. These explanations are *only*
    applicable when a failure object is serialized and deserialized (when it is
    retained inside the python process that the exception was created in the
    the original exception can be reraised correctly without issue).

    * Traceback objects are not serializable/recreatable, since they contain
      references to stack frames at the location where the exception was
      raised. When a failure object is serialized and sent across a channel
      and recreated it is *not* possible to restore the original traceback and
      originating stack frames.
    * The original exception *type* can not be guaranteed to be found, workers
      can run code that is not accessible/available when the failure is being
      deserialized. Even if it was possible to use pickle safely it would not
      be possible to find the originating exception or associated code in this
      situation.
    * The original exception *type* can not be guaranteed to be constructed in
      a *correct* manner. At the time of failure object creation the exception
      has already been created and the failure object can not assume it has
      knowledge (or the ability) to recreate the original type of the captured
      exception (this is especially hard if the original exception was created
      via a complex process via some custom exception constructor).
    * The original exception *type* can not be guaranteed to be constructed in
      a *safe* manner. Importing *foreign* exception types dynamically can be
      problematic when not done correctly and in a safe manner; since failure
      objects can capture any exception it would be *unsafe* to try to import
      those exception types namespaces and modules on the receiver side
      dynamically (this would create similar issues as the ``pickle`` module in
      python has where foreign modules can be imported, causing those modules
      to have code ran when this happens, and this can cause issues and
      side-effects that the receiver would not have intended to have caused).

    TODO(harlowja): use parts of http://bugs.python.org/issue17911 and the
    backport at https://pypi.org/project/traceback2/ to (hopefully)
    simplify the methods and contents of this object...
    """
    DICT_VERSION = 1

    BASE_EXCEPTIONS = ('BaseException', 'Exception')
    """
    Root exceptions of all other python exceptions.

    See: https://docs.python.org/2/library/exceptions.html
    """

    #: Expected failure schema (in json schema format).
    SCHEMA = {
        "$ref": "#/definitions/cause",
        "definitions": {
            "cause": {
                "type": "object",
                'properties': {
                    'version': {
                        "type": "integer",
                        "minimum": 0,
                    },
                    'exc_args': {
                        "type": "array",
                        "minItems": 0,
                    },
                    'exception_str': {
                        "type": "string",
                    },
                    'traceback_str': {
                        "type": "string",
                    },
                    'exc_type_names': {
                        "type": "array",
                        "items": {
                            "type": "string",
                        },
                        "minItems": 1,
                    },
                    'causes': {
                        "type": "array",
                        "items": {
                            "$ref": "#/definitions/cause",
                        },
                    }
                },
                "required": [
                    "exception_str",
                    'traceback_str',
                    'exc_type_names',
                ],
                "additionalProperties": True,
            },
        },
    }

    def __init__(self, exc_info=None, **kwargs):
        if not kwargs:
            if exc_info is None:
                exc_info = sys.exc_info()
            else:
                # This should always be the (type, value, traceback) tuple,
                # either from a prior sys.exc_info() call or from some other
                # creation...
                if len(exc_info) != 3:
                    raise ValueError("Provided 'exc_info' must contain three"
                                     " elements")
            self._exc_info = exc_info
            self._exc_args = tuple(getattr(exc_info[1], 'args', []))
            self._exc_type_names = tuple(
                reflection.get_all_class_names(exc_info[0], up_to=Exception))
            if not self._exc_type_names:
                raise TypeError("Invalid exception type '%s' (%s)"
                                % (exc_info[0], type(exc_info[0])))
            self._exception_str = _exception_message(self._exc_info[1])
            self._traceback_str = ''.join(
                traceback.format_tb(self._exc_info[2]))
            self._causes = kwargs.pop('causes', None)
        else:
            self._causes = kwargs.pop('causes', None)
            self._exc_info = exc_info
            self._exc_args = tuple(kwargs.pop('exc_args', []))
            self._exception_str = kwargs.pop('exception_str')
            self._exc_type_names = tuple(kwargs.pop('exc_type_names', []))
            self._traceback_str = kwargs.pop('traceback_str', None)
            if kwargs:
                raise TypeError(
                    'Failure.__init__ got unexpected keyword argument(s): %s'
                    % ', '.join(six.iterkeys(kwargs)))

    @classmethod
    def from_exception(cls, exception):
        """Creates a failure object from a exception instance."""
        exc_info = (
            type(exception),
            exception,
            getattr(exception, '__traceback__', None)
        )
        return cls(exc_info=exc_info)

    @classmethod
    def validate(cls, data):
        """Validate input data matches expected failure ``dict`` format."""
        try:
            su.schema_validate(data, cls.SCHEMA)
        except su.ValidationError as e:
            raise exc.InvalidFormat("Failure data not of the"
                                    " expected format: %s" % (e.message), e)
        else:
            # Ensure that all 'exc_type_names' originate from one of
            # BASE_EXCEPTIONS, because those are the root exceptions that
            # python mandates/provides and anything else is invalid...
            causes = collections.deque([data])
            while causes:
                cause = causes.popleft()
                root_exc_type = cause['exc_type_names'][-1]
                if root_exc_type not in cls.BASE_EXCEPTIONS:
                    raise exc.InvalidFormat(
                        "Failure data 'exc_type_names' must"
                        " have an initial exception type that is one"
                        " of %s types: '%s' is not one of those"
                        " types" % (cls.BASE_EXCEPTIONS, root_exc_type))
                sub_causes = cause.get('causes')
                if sub_causes:
                    causes.extend(sub_causes)

    def _matches(self, other):
        if self is other:
            return True
        return (self._exc_type_names == other._exc_type_names
                and self.exception_args == other.exception_args
                and self.exception_str == other.exception_str
                and self.traceback_str == other.traceback_str
                and self.causes == other.causes)

    def matches(self, other):
        """Checks if another object is equivalent to this object.

        :returns: checks if another object is equivalent to this object
        :rtype: boolean
        """
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
                _are_equal_exc_info_tuples(self.exc_info, other.exc_info))

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
        """Exception value, or none if exception value is not present.

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
    def exception_args(self):
        """Tuple of arguments given to the exception constructor."""
        return self._exc_args

    @property
    def exc_info(self):
        """Exception info tuple or none.

        See: https://docs.python.org/2/library/sys.html#sys.exc_info for what
             the contents of this tuple are (if none, then no contents can
             be examined).
        """
        return self._exc_info

    @property
    def traceback_str(self):
        """Exception traceback as string."""
        return self._traceback_str

    @staticmethod
    def reraise_if_any(failures):
        """Re-raise exceptions if argument is not empty.

        If argument is empty list/tuple/iterator, this method returns
        None. If argument is converted into a list with a
        single ``Failure`` object in it, that failure is reraised. Else, a
        :class:`~taskflow.exceptions.WrappedFailure` exception
        is raised with the failure list as causes.
        """
        if not isinstance(failures, (list, tuple)):
            # Convert generators/other into a list...
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
        """Check if any of ``exc_classes`` caused the failure.

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

    @classmethod
    def _extract_causes_iter(cls, exc_val):
        seen = [exc_val]
        causes = [exc_val]
        while causes:
            exc_val = causes.pop()
            if exc_val is None:
                continue
            # See: https://www.python.org/dev/peps/pep-3134/ for why/what
            # these are...
            #
            # '__cause__' attribute for explicitly chained exceptions
            # '__context__' attribute for implicitly chained exceptions
            # '__traceback__' attribute for the traceback
            #
            # See: https://www.python.org/dev/peps/pep-0415/ for why/what
            # the '__suppress_context__' is/means/implies...
            suppress_context = getattr(exc_val,
                                       '__suppress_context__', False)
            if suppress_context:
                attr_lookups = ['__cause__']
            else:
                attr_lookups = ['__cause__', '__context__']
            nested_exc_val = None
            for attr_name in attr_lookups:
                attr_val = getattr(exc_val, attr_name, None)
                if attr_val is None:
                    continue
                if attr_val not in seen:
                    nested_exc_val = attr_val
                    break
            if nested_exc_val is not None:
                exc_info = (
                    type(nested_exc_val),
                    nested_exc_val,
                    getattr(nested_exc_val, '__traceback__', None),
                )
                seen.append(nested_exc_val)
                causes.append(nested_exc_val)
                yield cls(exc_info=exc_info)

    @property
    def causes(self):
        """Tuple of all *inner* failure *causes* of this failure.

        NOTE(harlowja): Does **not** include the current failure (only
        returns connected causes of this failure, if any). This property
        is really only useful on 3.x or newer versions of python as older
        versions do **not** have associated causes (the tuple will **always**
        be empty on 2.x versions of python).

        Refer to :pep:`3134` and :pep:`409` and :pep:`415` for what
        this is examining to find failure causes.
        """
        if self._causes is not None:
            return self._causes
        else:
            self._causes = tuple(self._extract_causes_iter(self.exception))
            return self._causes

    def __unicode__(self):
        return self.pformat()

    def pformat(self, traceback=False):
        """Pretty formats the failure object into a string."""
        buf = six.StringIO()
        if not self._exc_type_names:
            buf.write('Failure: %s' % (self._exception_str))
        else:
            buf.write('Failure: %s: %s' % (self._exc_type_names[0],
                                           self._exception_str))
        if traceback:
            if self._traceback_str is not None:
                traceback_str = self._traceback_str.rstrip()
            else:
                traceback_str = None
            if traceback_str:
                buf.write(os.linesep)
                buf.write('Traceback (most recent call last):')
                buf.write(os.linesep)
                buf.write(traceback_str)
            else:
                buf.write(os.linesep)
                buf.write('Traceback not available.')
        return buf.getvalue()

    def __iter__(self):
        """Iterate over exception type names."""
        for et in self._exc_type_names:
            yield et

    def __getstate__(self):
        dct = self.to_dict()
        if self._exc_info:
            # Avoids 'TypeError: can't pickle traceback objects'
            dct['exc_info'] = self._exc_info[0:2]
        return dct

    def __setstate__(self, dct):
        self._exception_str = dct['exception_str']
        if 'exc_args' in dct:
            self._exc_args = tuple(dct['exc_args'])
        else:
            # Guess we got an older version somehow, before this
            # was added, so at that point just set to an empty tuple...
            self._exc_args = ()
        self._traceback_str = dct['traceback_str']
        self._exc_type_names = dct['exc_type_names']
        if 'exc_info' in dct:
            # Tracebacks can't be serialized/deserialized, but since we
            # provide a traceback string (and more) this should be
            # acceptable...
            #
            # TODO(harlowja): in the future we could do something like
            # what the twisted people have done, see for example
            # twisted-13.0.0/twisted/python/failure.py#L89 for how they
            # created a fake traceback object...
            self._exc_info = tuple(iter_utils.fill(dct['exc_info'], 3))
        else:
            self._exc_info = None
        causes = dct.get('causes')
        if causes is not None:
            causes = tuple(self.from_dict(d) for d in causes)
        self._causes = causes

    @classmethod
    def from_dict(cls, data):
        """Converts this from a dictionary to a object."""
        data = dict(data)
        version = data.pop('version', None)
        if version != cls.DICT_VERSION:
            raise ValueError('Invalid dict version of failure object: %r'
                             % version)
        causes = data.get('causes')
        if causes is not None:
            data['causes'] = tuple(cls.from_dict(d) for d in causes)
        return cls(**data)

    def to_dict(self, include_args=True):
        """Converts this object to a dictionary.

        :param include_args: boolean indicating whether to include the
                             exception args in the output.
        """
        return {
            'exception_str': self.exception_str,
            'traceback_str': self.traceback_str,
            'exc_type_names': list(self),
            'version': self.DICT_VERSION,
            'exc_args': self.exception_args if include_args else tuple(),
            'causes': [f.to_dict() for f in self.causes],
        }

    def copy(self):
        """Copies this object."""
        return Failure(exc_info=_copy_exc_info(self.exc_info),
                       exception_str=self.exception_str,
                       traceback_str=self.traceback_str,
                       exc_args=self.exception_args,
                       exc_type_names=self._exc_type_names[:],
                       causes=self._causes)
