# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#    Copyright (C) 2013 Rackspace Hosting All Rights Reserved.
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
import errno
import functools
import logging
import os
import six
import sys
import traceback

from taskflow import exceptions
from taskflow.utils import reflection


LOG = logging.getLogger(__name__)


def wraps(fn):
    """This will not be needed in python 3.2 or greater which already has this
    built-in to its functools.wraps method.
    """

    def wrapper(f):
        f = functools.wraps(fn)(f)
        f.__wrapped__ = getattr(fn, '__wrapped__', fn)
        return f

    return wrapper


def get_version_string(obj):
    """Gets a object's version as a string.

    Returns string representation of object's version taken from
    its 'version' attribute, or None if object does not have such
    attribute or its version is None.
    """
    obj_version = getattr(obj, 'version', None)
    if isinstance(obj_version, (list, tuple)):
        obj_version = '.'.join(str(item) for item in obj_version)
    if obj_version is not None and not isinstance(obj_version,
                                                  six.string_types):
        obj_version = str(obj_version)
    return obj_version


def get_duplicate_keys(iterable, key=None):
    if key is not None:
        iterable = six.moves.map(key, iterable)
    keys = set()
    duplicates = set()
    for item in iterable:
        if item in keys:
            duplicates.add(item)
        keys.add(item)
    return duplicates


def is_valid_attribute_name(name, allow_self=False, allow_hidden=False):
    """Validates that a string name is a valid/invalid python attribute name"""
    if not isinstance(name, six.string_types) or len(name) == 0:
        return False
    # Make the name just be a simple string in latin-1 encoding in python3
    name = six.b(name)
    if not allow_self and name.lower().startswith('self'):
        return False
    if not allow_hidden and name.startswith("_"):
        return False
    # See: http://docs.python.org/release/2.5.2/ref/grammar.txt (or newer)
    #
    # Python identifiers should start with a letter.
    if not name[0].isalpha():
        return False
    for i in range(1, len(name)):
        # The rest of an attribute name follows: (letter | digit | "_")*
        if not (name[i].isalpha() or name[i].isdigit() or name[i] == "_"):
            return False
    return True


class AttrDict(dict):
    """Helper utility dict sub-class to create a class that can be accessed by
    attribute name from a dictionary that contains a set of keys and values.
    """
    NO_ATTRS = tuple(reflection.get_member_names(dict))

    @classmethod
    def _is_valid_attribute_name(cls, name):
        if not is_valid_attribute_name(name):
            return False
        # Make the name just be a simple string in latin-1 encoding in python3
        name = six.b(name)
        if name in cls.NO_ATTRS:
            return False
        return True

    def __init__(self, **kwargs):
        for (k, v) in kwargs.items():
            if not self._is_valid_attribute_name(k):
                raise AttributeError("Invalid attribute name: '%s'" % (k))
            self[k] = v

    def __getattr__(self, name):
        if not self._is_valid_attribute_name(name):
            raise AttributeError("Invalid attribute name: '%s'" % (name))
        try:
            return self[name]
        except KeyError:
            raise AttributeError("No attributed named: '%s'" % (name))

    def __setattr__(self, name, value):
        if not self._is_valid_attribute_name(name):
            raise AttributeError("Invalid attribute name: '%s'" % (name))
        self[name] = value


class ExponentialBackoff(object):
    """An iterable object that will yield back an exponential delay sequence
    provided an exponent and a number of items to yield. This object may be
    iterated over multiple times (yielding the same sequence each time).
    """
    def __init__(self, attempts, exponent=2):
        self.attempts = int(attempts)
        self.exponent = exponent

    def __iter__(self):
        if self.attempts <= 0:
            raise StopIteration()
        for i in xrange(0, self.attempts):
            yield self.exponent ** i

    def __str__(self):
        return "ExponentialBackoff: %s" % ([str(v) for v in self])


def as_bool(val):
    """Converts an arbitary value into a boolean."""
    if isinstance(val, bool):
        return val
    if isinstance(val, six.string_types):
        if val.lower() in ('f', 'false', '0', 'n', 'no'):
            return False
        if val.lower() in ('t', 'true', '1', 'y', 'yes'):
            return True
    return bool(val)


def as_int(obj, quiet=False):
    """Converts an arbitary value into a integer."""
    # Try "2" -> 2
    try:
        return int(obj)
    except (ValueError, TypeError):
        pass
    # Try "2.5" -> 2
    try:
        return int(float(obj))
    except (ValueError, TypeError):
        pass
    # Eck, not sure what this is then.
    if not quiet:
        raise TypeError("Can not translate %s to an integer." % (obj))
    return obj


# Taken from oslo-incubator file-utils but since that module pulls in a large
# amount of other files it does not seem so useful to include that full
# module just for this function.
def ensure_tree(path):
    """Create a directory (and any ancestor directories required)

    :param path: Directory to create
    """
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            if not os.path.isdir(path):
                raise
        else:
            raise


class TransitionNotifier(object):
    """A utility helper class that can be used to subscribe to
    notifications of events occuring as well as allow a entity to post said
    notifications to subscribers.
    """

    RESERVED_KEYS = ('details',)
    ANY = '*'

    def __init__(self):
        self._listeners = collections.defaultdict(list)

    def reset(self):
        self._listeners = collections.defaultdict(list)

    def notify(self, state, details):
        listeners = list(self._listeners.get(self.ANY, []))
        for i in self._listeners[state]:
            if i not in listeners:
                listeners.append(i)
        if not listeners:
            return
        for (callback, args, kwargs) in listeners:
            if args is None:
                args = []
            if kwargs is None:
                kwargs = {}
            kwargs['details'] = details
            try:
                callback(state, *args, **kwargs)
            except Exception:
                LOG.exception(("Failure calling callback %s to notify about"
                               " state transition %s"), callback, state)

    def register(self, state, callback, args=None, kwargs=None):
        assert isinstance(callback, collections.Callable)
        for i, (cb, args, kwargs) in enumerate(self._listeners.get(state, [])):
            if cb is callback:
                raise ValueError("Callback %s already registered" % (callback))
        if kwargs:
            for k in self.RESERVED_KEYS:
                if k in kwargs:
                    raise KeyError(("Reserved key '%s' not allowed in "
                                    "kwargs") % k)
            kwargs = copy.copy(kwargs)
        if args:
            args = copy.copy(args)
        self._listeners[state].append((callback, args, kwargs))

    def deregister(self, state, callback):
        if state not in self._listeners:
            return
        for i, (cb, args, kwargs) in enumerate(self._listeners[state]):
            if cb is callback:
                self._listeners[state].pop(i)
                break


def copy_exc_info(exc_info):
    """Make copy of exception info tuple, as deep as possible"""
    if exc_info is None:
        return None
    exc_type, exc_value, tb = exc_info
    # NOTE(imelnikov): there is no need to copy type, and
    # we can't copy traceback
    return (exc_type, copy.deepcopy(exc_value), tb)


def are_equal_exc_info_tuples(ei1, ei2):
    if ei1 == ei2:
        return True
    if ei1 is None or ei2 is None:
        return False  # if both are None, we returned True above

    # NOTE(imelnikov): we can't compare exceptions with '=='
    # because we want exc_info be equal to it's copy made with
    # copy_exc_info above
    if ei1[0] is not ei2[0]:
        return False
    if not all((type(ei1[1]) == type(ei2[1]),
                str(ei1[1]) == str(ei2[1]),
                repr(ei1[1]) == repr(ei2[1]))):
        return False
    if ei1[2] == ei2[2]:
        return True
    tb1 = traceback.format_tb(ei1[2])
    tb2 = traceback.format_tb(ei2[2])
    return tb1 == tb2


class Failure(object):
    """Object that represents failure.

    Failure objects encapsulate exception information so that
    it can be re-used later to re-raise or inspect.
    """

    def __init__(self, exc_info=None, **kwargs):
        if not kwargs:
            if exc_info is None:
                exc_info = sys.exc_info()
            self._exc_info = exc_info
            self._exc_type_names = list(
                reflection.get_all_class_names(exc_info[0], up_to=Exception))
            if not self._exc_type_names:
                raise TypeError('Invalid exception type: %r' % exc_info[0])
            self._exception_str = str(self._exc_info[1])
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
                are_equal_exc_info_tuples(self.exc_info, other.exc_info))

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
            raise exceptions.WrappedFailure(failures)

    def reraise(self):
        """Re-raise captured exception"""
        if self._exc_info:
            six.reraise(*self._exc_info)
        else:
            raise exceptions.WrappedFailure([self])

    def check(self, *exc_classes):
        """Check if any of exc_classes caused the failure

        Arguments of this method can be exception types or type
        names (stings). If captured excption is instance of
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
        """Iterate over exception type names"""
        for et in self._exc_type_names:
            yield et

    def copy(self):
        return Failure(exc_info=copy_exc_info(self.exc_info),
                       exception_str=self.exception_str,
                       traceback_str=self.traceback_str,
                       exc_type_names=self._exc_type_names[:])
