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

import collections
import contextlib
import datetime
import inspect
import os
import re
import socket
import sys
import threading
import types

import enum
import networkx as nx
from oslo_serialization import jsonutils
from oslo_serialization import msgpackutils
from oslo_utils import encodeutils
from oslo_utils import importutils
from oslo_utils import netutils
from oslo_utils import reflection
import six

from taskflow.types import failure


UNKNOWN_HOSTNAME = "<unknown>"
NUMERIC_TYPES = six.integer_types + (float,)

# NOTE(imelnikov): regular expression to get scheme from URI,
# see RFC 3986 section 3.1
_SCHEME_REGEX = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*):")


class Activator(object):
    """Tiny helper that starts and stops things *smartly* in bulk.

    NOTE(harlowja): :py:meth:`.start` and :py:meth:`.stop` should **not** be
    called by different threads at the same time, as local object state will
    not be maintained correctly if that happens (due to the delayed pop
    that occurs **after** each things stop has succeeded).
    """

    #: Callable attributes that objects to start/stop **must** have.
    REQUIRED_ATTRS = frozenset(['start', 'stop'])

    def __init__(self, it):
        self._need_stop = []
        self._need_start = tuple(self._validate_and_return(thing)
                                 for thing in it)

    @property
    def need_to_be_stopped(self):
        return len(self._need_stop)

    @property
    def need_to_be_started(self):
        return len(self._need_start[len(self._need_stop):])

    @classmethod
    def _validate_and_return(cls, thing):
        """Checks if provided thing is able to be started/stopped."""
        for attr_name in cls.REQUIRED_ATTRS:
            cb = getattr(thing, attr_name)
            if not six.callable(cb):
                raise ValueError("Attribute '%s' on '%s' must be"
                                 " callable" % (attr_name, thing))
        return thing

    def start(self):
        """Starts any not started things (from first not started to last)."""
        need_start = self._need_start[len(self._need_stop):]
        for thing in need_start:
            thing.start()
            self._need_stop.append(thing)

    def stop(self):
        """Stops all started things (from last to first)."""
        while self._need_stop:
            thing = self._need_stop[-1]
            thing.stop()
            # Only clear it after it works (in-case it raises...); instead
            # of removing it before (and then having it fail); which would
            # leave this thing to stop in a messed up state...
            self._need_stop.pop()


class StrEnum(str, enum.Enum):
    """An enumeration that is also a string and can be compared to strings."""

    def __new__(cls, *args, **kwargs):
        for a in args:
            if not isinstance(a, str):
                raise TypeError("Enumeration '%s' (%s) is not"
                                " a string" % (a, type(a).__name__))
        return super(StrEnum, cls).__new__(cls, *args, **kwargs)


class StringIO(six.StringIO):
    """String buffer with some small additions."""

    def write_nl(self, value='', linesep=os.linesep):
        """Writes the given value and then a newline."""
        self.write(value)
        self.write(linesep)

    def getvalue(self, fix_newlines=False):
        if not six.PY2:
            blob = super(StringIO, self).getvalue()
        else:
            # This is an old-style class in python 2.x so we can't use the
            # other call mechanism to call into the parent...
            blob = six.StringIO.getvalue(self)
        if fix_newlines:
            return os.linesep.join(blob.splitlines())
        else:
            return blob


class BytesIO(six.BytesIO):
    """Byte buffer with some small additions."""

    def reset(self):
        self.seek(0)
        self.truncate()


def get_hostname(unknown_hostname=UNKNOWN_HOSTNAME):
    """Gets the machines hostname; if not able to returns an invalid one."""
    try:
        hostname = socket.getfqdn()
        if not hostname:
            return unknown_hostname
        else:
            return hostname
    except socket.error:
        return unknown_hostname


def match_type(obj, matchers):
    """Matches a given object using the given matchers list/iterable.

    NOTE(harlowja): each element of the provided list/iterable must be
    tuple of (valid types, result).

    Returns the result (the second element of the provided tuple) if a type
    match occurs, otherwise none if no matches are found.
    """
    for (match_types, match_result) in matchers:
        if isinstance(obj, match_types):
            return match_result
    else:
        return None


def countdown_iter(start_at, decr=1):
    """Generator that decrements after each generation until <= zero.

    NOTE(harlowja): we can likely remove this when we can use an
    ``itertools.count`` that takes a step (on py2.6 which we still support
    that step parameter does **not** exist and therefore can't be used).
    """
    if decr <= 0:
        raise ValueError("Decrement value must be greater"
                         " than zero and not %s" % decr)
    while start_at > 0:
        yield start_at
        start_at -= decr


def extract_driver_and_conf(conf, conf_key):
    """Common function to get a driver name and its configuration."""
    if isinstance(conf, six.string_types):
        conf = {conf_key: conf}
    maybe_uri = conf[conf_key]
    try:
        uri = parse_uri(maybe_uri)
    except (TypeError, ValueError):
        return (maybe_uri, conf)
    else:
        return (uri.scheme, merge_uri(uri, conf.copy()))


def reverse_enumerate(items):
    """Like reversed(enumerate(items)) but with less copying/cloning..."""
    for i in countdown_iter(len(items)):
        yield i - 1, items[i - 1]


def merge_uri(uri, conf):
    """Merges a parsed uri into the given configuration dictionary.

    Merges the username, password, hostname, port, and query parameters of
    a URI into the given configuration dictionary (it does **not** overwrite
    existing configuration keys if they already exist) and returns the merged
    configuration.

    NOTE(harlowja): does not merge the path, scheme or fragment.
    """
    uri_port = uri.port
    specials = [
        ('username', uri.username, lambda v: bool(v)),
        ('password', uri.password, lambda v: bool(v)),
        # NOTE(harlowja): A different check function is used since 0 is
        # false (when bool(v) is applied), and that is a valid port...
        ('port', uri_port, lambda v: v is not None),
    ]
    hostname = uri.hostname
    if hostname:
        if uri_port is not None:
            hostname += ":%s" % (uri_port)
        specials.append(('hostname', hostname, lambda v: bool(v)))
    for (k, v, is_not_empty_value_func) in specials:
        if is_not_empty_value_func(v):
            conf.setdefault(k, v)
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


def disallow_when_frozen(excp_cls):
    """Frozen checking/raising method decorator."""

    def decorator(f):

        @six.wraps(f)
        def wrapper(self, *args, **kwargs):
            if self.frozen:
                raise excp_cls()
            else:
                return f(self, *args, **kwargs)

        return wrapper

    return decorator


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


def binary_encode(text, encoding='utf-8', errors='strict'):
    """Encodes a text string into a binary string using given encoding.

    Does nothing if data is already a binary string (raises on unknown types).
    """
    if isinstance(text, six.binary_type):
        return text
    else:
        return encodeutils.safe_encode(text, encoding=encoding,
                                       errors=errors)


def binary_decode(data, encoding='utf-8', errors='strict'):
    """Decodes a binary string into a text string using given encoding.

    Does nothing if data is already a text string (raises on unknown types).
    """
    if isinstance(data, six.text_type):
        return data
    else:
        return encodeutils.safe_decode(data, incoming=encoding,
                                       errors=errors)


def _check_decoded_type(data, root_types=(dict,)):
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


def decode_msgpack(raw_data, root_types=(dict,)):
    """Parse raw data to get decoded object.

    Decodes a msgback encoded 'blob' from a given raw data binary string and
    checks that the root type of that decoded object is in the allowed set of
    types (by default a dict should be the root type).
    """
    try:
        data = msgpackutils.loads(raw_data)
    except Exception as e:
        # TODO(harlowja): fix this when msgpackutils exposes the msgpack
        # exceptions so that we can avoid catching just exception...
        raise ValueError("Expected msgpack decodable data: %s" % e)
    else:
        return _check_decoded_type(data, root_types=root_types)


def decode_json(raw_data, root_types=(dict,)):
    """Parse raw data to get decoded object.

    Decodes a JSON encoded 'blob' from a given raw data binary string and
    checks that the root type of that decoded object is in the allowed set of
    types (by default a dict should be the root type).
    """
    try:
        data = jsonutils.loads(binary_decode(raw_data))
    except UnicodeDecodeError as e:
        raise ValueError("Expected UTF-8 decodable data: %s" % e)
    except ValueError as e:
        raise ValueError("Expected JSON decodable data: %s" % e)
    else:
        return _check_decoded_type(data, root_types=root_types)


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
    def __init__(self, fget=None, require_lock=True):
        if require_lock:
            self._lock = threading.RLock()
        else:
            self._lock = None
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
        # If __init__ received a string or a lock boolean then this will be
        # the function to be wrapped as a property (if __init__ got a
        # function then this will not be called).
        self._fget = fget
        if not self._attr_name:
            self._attr_name = "_%s" % (fget.__name__)
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
            if self._lock is not None:
                self._lock.acquire()
            try:
                return getattr(instance, self._attr_name)
            except AttributeError:
                value = self._fget(instance)
                setattr(instance, self._attr_name, value)
                return value
            finally:
                if self._lock is not None:
                    self._lock.release()


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


def is_iterable(obj):
    """Tests an object to to determine whether it is iterable.

    This function will test the specified object to determine whether it is
    iterable. String types (both ``str`` and ``unicode``) are ignored and will
    return False.

    :param obj: object to be tested for iterable
    :return: True if object is iterable and is not a string
    """
    return (not isinstance(obj, six.string_types) and
            isinstance(obj, collections.Iterable))


def safe_copy_dict(obj):
    """Copy an existing dictionary or default to empty dict...

    This will return a empty dict if given object is falsey, otherwise it
    will create a dict of the given object (which if provided a dictionary
    object will make a shallow copy of that object).
    """
    if not obj:
        return {}
    # default to a shallow copy to avoid most ownership issues
    return dict(obj)


def nx_version():
    return nx.__version__.split('.')[0]
