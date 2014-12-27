# -*- coding: utf-8 -*-

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

import contextlib
import datetime
import errno
import inspect
import os
import re
import sys
import threading
import time
import types

from oslo_serialization import jsonutils
from oslo_utils import importutils
from oslo_utils import netutils
from oslo_utils import reflection
import six
from six.moves import map as compat_map
from six.moves import range as compat_range

from taskflow.types import failure
from taskflow.types import notifier
from taskflow.utils import deprecation


NUMERIC_TYPES = six.integer_types + (float,)

# NOTE(imelnikov): regular expression to get scheme from URI,
# see RFC 3986 section 3.1
_SCHEME_REGEX = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*):")

_MONOTONIC_LOCATIONS = tuple([
    # The built-in/expected location in python3.3+
    'time.monotonic',
    # NOTE(harlowja): Try to use the pypi module that provides this
    # functionality for older versions of python less than 3.3 so that
    # they to can benefit from better timing...
    #
    # See: http://pypi.python.org/pypi/monotonic
    'monotonic.monotonic',
])


def find_monotonic(allow_time_time=False):
    """Tries to find a monotonic time providing function (and returns it)."""
    for import_str in _MONOTONIC_LOCATIONS:
        mod_str, _sep, attr_str = import_str.rpartition('.')
        mod = importutils.try_import(mod_str)
        if mod is None:
            continue
        func = getattr(mod, attr_str, None)
        if func is not None:
            return func
    # Finally give up and use time.time (which isn't monotonic)...
    if allow_time_time:
        return time.time
    else:
        return None


def merge_uri(uri, conf):
    """Merges a parsed uri into the given configuration dictionary.

    Merges the username, password, hostname, and query params of a uri into
    the given configuration (it does not overwrite the configuration keys if
    they already exist) and returns the adjusted configuration.

    NOTE(harlowja): does not merge the path, scheme or fragment.
    """
    for (k, v) in [('username', uri.username), ('password', uri.password)]:
        if not v:
            continue
        conf.setdefault(k, v)
    if uri.hostname:
        hostname = uri.hostname
        if uri.port is not None:
            hostname += ":%s" % (uri.port)
        conf.setdefault('hostname', hostname)
    for (k, v) in six.iteritems(uri.params()):
        conf.setdefault(k, v)
    return conf


def find_subclasses(locations, base_cls, exclude_hidden=True):
    """Finds subclass types in the given locations.

    This will examines the given locations for types which are subclasses of
    the base class type provided and returns the found subclasses (or fails
    with exceptions if this introspection can not be accomplished).

    If a string is provided as one of the locations it will be imported and
    examined if it is a subclass of the base class. If a module is given,
    all of its members will be examined for attributes which are subclasses of
    the base class. If a type itself is given it will be examined for being a
    subclass of the base class.
    """
    derived = set()
    for item in locations:
        module = None
        if isinstance(item, six.string_types):
            try:
                pkg, cls = item.split(':')
            except ValueError:
                module = importutils.import_module(item)
            else:
                obj = importutils.import_class('%s.%s' % (pkg, cls))
                if not reflection.is_subclass(obj, base_cls):
                    raise TypeError("Object '%s' (%s) is not a '%s' subclass"
                                    % (item, type(item), base_cls))
                derived.add(obj)
        elif isinstance(item, types.ModuleType):
            module = item
        elif reflection.is_subclass(item, base_cls):
            derived.add(item)
        else:
            raise TypeError("Object '%s' (%s) is an unexpected type" %
                            (item, type(item)))
        # If it's a module derive objects from it if we can.
        if module is not None:
            for (name, obj) in inspect.getmembers(module):
                if name.startswith("_") and exclude_hidden:
                    continue
                if reflection.is_subclass(obj, base_cls):
                    derived.add(obj)
    return derived


def pick_first_not_none(*values):
    """Returns first of values that is *not* None (or None if all are/were)."""
    for val in values:
        if val is not None:
            return val
    return None


def parse_uri(uri):
    """Parses a uri into its components."""
    # Do some basic validation before continuing...
    if not isinstance(uri, six.string_types):
        raise TypeError("Can only parse string types to uri data, "
                        "and not '%s' (%s)" % (uri, type(uri)))
    match = _SCHEME_REGEX.match(uri)
    if not match:
        raise ValueError("Uri '%s' does not start with a RFC 3986 compliant"
                         " scheme" % (uri))
    return netutils.urlsplit(uri)


def clamp(value, minimum, maximum, on_clamped=None):
    """Clamps a value to ensure its >= minimum and <= maximum."""
    if minimum > maximum:
        raise ValueError("Provided minimum '%s' must be less than or equal to"
                         " the provided maximum '%s'" % (minimum, maximum))
    if value > maximum:
        value = maximum
        if on_clamped is not None:
            on_clamped()
    if value < minimum:
        value = minimum
        if on_clamped is not None:
            on_clamped()
    return value


def fix_newlines(text, replacement=os.linesep):
    """Fixes text that *may* end with wrong nl by replacing with right nl."""
    return replacement.join(text.splitlines())


def binary_encode(text, encoding='utf-8'):
    """Converts a string of into a binary type using given encoding.

    Does nothing if text not unicode string.
    """
    if isinstance(text, six.binary_type):
        return text
    elif isinstance(text, six.text_type):
        return text.encode(encoding)
    else:
        raise TypeError("Expected binary or string type not '%s'" % type(text))


def binary_decode(data, encoding='utf-8'):
    """Converts a binary type into a text type using given encoding.

    Does nothing if data is already unicode string.
    """
    if isinstance(data, six.binary_type):
        return data.decode(encoding)
    elif isinstance(data, six.text_type):
        return data
    else:
        raise TypeError("Expected binary or string type not '%s'" % type(data))


def decode_json(raw_data, root_types=(dict,)):
    """Parse raw data to get JSON object.

    Decodes a JSON from a given raw data binary and checks that the root
    type of that decoded object is in the allowed set of types (by
    default a JSON object/dict should be the root type).
    """
    try:
        data = jsonutils.loads(binary_decode(raw_data))
    except UnicodeDecodeError as e:
        raise ValueError("Expected UTF-8 decodable data: %s" % e)
    except ValueError as e:
        raise ValueError("Expected JSON decodable data: %s" % e)
    if root_types:
        if not isinstance(root_types, tuple):
            root_types = tuple(root_types)
        if not isinstance(data, root_types):
            if len(root_types) == 1:
                root_type = root_types[0]
                raise ValueError("Expected '%s' root type not '%s'"
                                 % (root_type, type(data)))
            else:
                raise ValueError("Expected %s root types not '%s'"
                                 % (list(root_types), type(data)))
    return data


class cachedproperty(object):
    """A *thread-safe* descriptor property that is only evaluated once.

    This caching descriptor can be placed on instance methods to translate
    those methods into properties that will be cached in the instance (avoiding
    repeated attribute checking logic to do the equivalent).

    NOTE(harlowja): by default the property that will be saved will be under
    the decorated methods name prefixed with an underscore. For example if we
    were to attach this descriptor to an instance method 'get_thing(self)' the
    cached property would be stored under '_get_thing' in the self object
    after the first call to 'get_thing' occurs.
    """
    def __init__(self, fget):
        self._lock = threading.RLock()
        # If a name is provided (as an argument) then this will be the string
        # to place the cached attribute under if not then it will be the
        # function itself to be wrapped into a property.
        if inspect.isfunction(fget):
            self._fget = fget
            self._attr_name = "_%s" % (fget.__name__)
            self.__doc__ = getattr(fget, '__doc__', None)
        else:
            self._attr_name = fget
            self._fget = None
            self.__doc__ = None

    def __call__(self, fget):
        # If __init__ received a string then this will be the function to be
        # wrapped as a property (if __init__ got a function then this will not
        # be called).
        self._fget = fget
        self.__doc__ = getattr(fget, '__doc__', None)
        return self

    def __set__(self, instance, value):
        raise AttributeError("can't set attribute")

    def __delete__(self, instance):
        raise AttributeError("can't delete attribute")

    def __get__(self, instance, owner):
        if instance is None:
            return self
        # Quick check to see if this already has been made (before acquiring
        # the lock). This is safe to do since we don't allow deletion after
        # being created.
        if hasattr(instance, self._attr_name):
            return getattr(instance, self._attr_name)
        else:
            with self._lock:
                try:
                    return getattr(instance, self._attr_name)
                except AttributeError:
                    value = self._fget(instance)
                    setattr(instance, self._attr_name, value)
                    return value


def millis_to_datetime(milliseconds):
    """Converts number of milliseconds (from epoch) into a datetime object."""
    return datetime.datetime.fromtimestamp(float(milliseconds) / 1000)


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


def sequence_minus(seq1, seq2):
    """Calculate difference of two sequences.

    Result contains the elements from first sequence that are not
    present in second sequence, in original order. Works even
    if sequence elements are not hashable.
    """
    result = list(seq1)
    for item in seq2:
        try:
            result.remove(item)
        except ValueError:
            pass
    return result


def get_duplicate_keys(iterable, key=None):
    if key is not None:
        iterable = compat_map(key, iterable)
    keys = set()
    duplicates = set()
    for item in iterable:
        if item in keys:
            duplicates.add(item)
        keys.add(item)
    return duplicates


class ExponentialBackoff(object):
    """An iterable object that will yield back an exponential delay sequence.

    This objects provides for a configurable exponent, count of numbers
    to generate, and a maximum number that will be returned. This object may
    also be iterated over multiple times (yielding the same sequence each
    time).
    """
    def __init__(self, count, exponent=2, max_backoff=3600):
        self.count = max(0, int(count))
        self.exponent = exponent
        self.max_backoff = max(0, int(max_backoff))

    def __iter__(self):
        if self.count <= 0:
            raise StopIteration()
        for i in compat_range(0, self.count):
            yield min(self.exponent ** i, self.max_backoff)

    def __str__(self):
        return "ExponentialBackoff: %s" % ([str(v) for v in self])


def as_int(obj, quiet=False):
    """Converts an arbitrary value into a integer."""
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
        raise TypeError("Can not translate '%s' (%s) to an integer"
                        % (obj, type(obj)))
    return obj


# Taken from oslo-incubator file-utils but since that module pulls in a large
# amount of other files it does not seem so useful to include that full
# module just for this function.
def ensure_tree(path):
    """Create a directory (and any ancestor directories required).

    :param path: Directory to create
    """
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            if not os.path.isdir(path):
                raise
        else:
            raise


Failure = deprecation.moved_class(failure.Failure, 'Failure', __name__,
                                  version="0.6", removal_version="?")


Notifier = deprecation.moved_class(notifier.Notifier, 'Notifier', __name__,
                                   version="0.6", removal_version="?")


@contextlib.contextmanager
def capture_failure():
    """Captures the occurring exception and provides a failure object back.

    This will save the current exception information and yield back a
    failure object for the caller to use (it will raise a runtime error if
    no active exception is being handled).

    This is useful since in some cases the exception context can be cleared,
    resulting in None being attempted to be saved after an exception handler is
    run. This can happen when eventlet switches greenthreads or when running an
    exception handler, code raises and catches an exception. In both
    cases the exception context will be cleared.

    To work around this, we save the exception state, yield a failure and
    then run other code.

    For example::

        >>> from taskflow.utils import misc
        >>>
        >>> def cleanup():
        ...     pass
        ...
        >>>
        >>> def save_failure(f):
        ...     print("Saving %s" % f)
        ...
        >>>
        >>> try:
        ...     raise IOError("Broken")
        ... except Exception:
        ...     with misc.capture_failure() as fail:
        ...         print("Activating cleanup")
        ...         cleanup()
        ...         save_failure(fail)
        ...
        Activating cleanup
        Saving Failure: IOError: Broken

    """
    exc_info = sys.exc_info()
    if not any(exc_info):
        raise RuntimeError("No active exception is being handled")
    else:
        yield failure.Failure(exc_info=exc_info)
