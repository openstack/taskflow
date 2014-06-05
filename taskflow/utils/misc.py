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
import copy
import datetime
import errno
import functools
import inspect
import keyword
import logging
import os
import re
import string
import sys
import threading
import time
import traceback

import six
from six.moves.urllib import parse as urlparse

from taskflow import exceptions as exc
from taskflow.openstack.common import jsonutils
from taskflow.openstack.common import network_utils
from taskflow.utils import reflection


LOG = logging.getLogger(__name__)
NUMERIC_TYPES = six.integer_types + (float,)

# NOTE(imelnikov): regular expression to get scheme from URI,
# see RFC 3986 section 3.1
_SCHEME_REGEX = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*):")


def merge_uri(uri_pieces, conf):
    """Merges the username, password, hostname, and query params of a uri into
    the given configuration (does not overwrite the configuration keys if they
    already exist) and returns the adjusted configuration.

    NOTE(harlowja): does not merge the path, scheme or fragment.
    """
    for k in ('username', 'password'):
        if not uri_pieces[k]:
            continue
        conf.setdefault(k, uri_pieces[k])
    hostname = uri_pieces.get('hostname')
    if hostname:
        port = uri_pieces.get('port')
        if port is not None:
            hostname += ":%s" % (port)
        conf.setdefault('hostname', hostname)
    for (k, v) in six.iteritems(uri_pieces['params']):
        conf.setdefault(k, v)
    return conf


def parse_uri(uri, query_duplicates=False):
    """Parses a uri into its components and returns a dictionary containing
    those components.
    """
    # Do some basic validation before continuing...
    if not isinstance(uri, six.string_types):
        raise TypeError("Can only parse string types to uri data, "
                        "and not an object of type %s"
                        % reflection.get_class_name(uri))
    match = _SCHEME_REGEX.match(uri)
    if not match:
        raise ValueError("Uri %r does not start with a RFC 3986 compliant"
                         " scheme" % (uri))
    parsed = network_utils.urlsplit(uri)
    if parsed.query:
        query_params = urlparse.parse_qsl(parsed.query)
        if not query_duplicates:
            query_params = dict(query_params)
        else:
            # Retain duplicates in a list for keys which have duplicates, but
            # for items which are not duplicated, just associate the key with
            # the value.
            tmp_query_params = {}
            for (k, v) in query_params:
                if k in tmp_query_params:
                    p_v = tmp_query_params[k]
                    if isinstance(p_v, list):
                        p_v.append(v)
                    else:
                        p_v = [p_v, v]
                        tmp_query_params[k] = p_v
                else:
                    tmp_query_params[k] = v
            query_params = tmp_query_params
    else:
        query_params = {}
    uri_pieces = {
        'scheme': parsed.scheme,
        'username': parsed.username,
        'password': parsed.password,
        'fragment': parsed.fragment,
        'path': parsed.path,
        'params': query_params,
    }
    for k in ('hostname', 'port'):
        try:
            uri_pieces[k] = getattr(parsed, k)
        except (IndexError, ValueError):
            # The underlying network_utils throws when the host string is empty
            # which it may be in cases where it is not provided.
            #
            # NOTE(harlowja): when https://review.openstack.org/#/c/86921/ gets
            # merged we can just remove this since that error will no longer
            # occur.
            uri_pieces[k] = None
    return AttrDict(**uri_pieces)


def binary_encode(text, encoding='utf-8'):
    """Converts a string of into a binary type using given encoding.

    Does nothing if text not unicode string.
    """
    if isinstance(text, six.binary_type):
        return text
    elif isinstance(text, six.text_type):
        return text.encode(encoding)
    else:
        raise TypeError("Expected binary or string type")


def binary_decode(data, encoding='utf-8'):
    """Converts a binary type into a text type using given encoding.

    Does nothing if data is already unicode string.
    """
    if isinstance(data, six.binary_type):
        return data.decode(encoding)
    elif isinstance(data, six.text_type):
        return data
    else:
        raise TypeError("Expected binary or string type")


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
    if root_types and not isinstance(data, tuple(root_types)):
        ok_types = ", ".join(str(t) for t in root_types)
        raise ValueError("Expected (%s) root types not: %s"
                         % (ok_types, type(data)))
    return data


class cachedproperty(object):
    """Descriptor that can be placed on instance methods to translate
    those methods into properties that will be cached in the instance (avoiding
    repeated creation checking logic to do the equivalent).
    """
    def __init__(self, fget):
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
        try:
            return getattr(instance, self._attr_name)
        except AttributeError:
            value = self._fget(instance)
            setattr(instance, self._attr_name, value)
            return value


def wallclock():
    # NOTE(harlowja): made into a function so that this can be easily mocked
    # out if we want to alter time related functionality (for testing
    # purposes).
    return time.time()


def wraps(fn):
    """This will not be needed in python 3.2 or greater which already has this
    built-in to its functools.wraps method.
    """

    def wrapper(f):
        f = functools.wraps(fn)(f)
        f.__wrapped__ = getattr(fn, '__wrapped__', fn)
        return f

    return wrapper


def millis_to_datetime(milliseconds):
    """Converts a given number of milliseconds from the epoch into a datetime
    object.
    """
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


def item_from(container, index, name=None):
    """Attempts to fetch a index/key from a given container."""
    if index is None:
        return container
    try:
        return container[index]
    except (IndexError, KeyError, ValueError, TypeError):
        # NOTE(harlowja): Perhaps the container is a dictionary-like object
        # and that key does not exist (key error), or the container is a
        # tuple/list and a non-numeric key is being requested (index error),
        # or there was no container and an attempt to index into none/other
        # unsubscriptable type is being requested (type error).
        if name is None:
            name = index
        raise exc.NotFound("Unable to find %r in container %s"
                           % (name, container))


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


# NOTE(imelnikov): we should not use str.isalpha or str.isdigit
# as they are locale-dependant
_ASCII_WORD_SYMBOLS = frozenset(string.ascii_letters + string.digits + '_')


def is_valid_attribute_name(name, allow_self=False, allow_hidden=False):
    """Validates that a string name is a valid/invalid python attribute
    name.
    """
    return all((
        isinstance(name, six.string_types),
        len(name) > 0,
        (allow_self or not name.lower().startswith('self')),
        (allow_hidden or not name.lower().startswith('_')),

        # NOTE(imelnikov): keywords should be forbidden.
        not keyword.iskeyword(name),

        # See: http://docs.python.org/release/2.5.2/ref/grammar.txt
        not (name[0] in string.digits),
        all(symbol in _ASCII_WORD_SYMBOLS for symbol in name)
    ))


class AttrDict(dict):
    """Helper utility dict sub-class to create a class that can be accessed by
    attribute name from a dictionary that contains a set of keys and values.
    """
    NO_ATTRS = tuple(reflection.get_member_names(dict))

    @classmethod
    def _is_valid_attribute_name(cls, name):
        if not is_valid_attribute_name(name):
            return False
        # Make the name just be a simple string in latin-1 encoding in python3.
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


class Timeout(object):
    """An object which represents a timeout.

    This object has the ability to be interrupted before the actual timeout
    is reached.
    """
    def __init__(self, timeout):
        if timeout < 0:
            raise ValueError("Timeout must be >= 0 and not %s" % (timeout))
        self._timeout = timeout
        self._event = threading.Event()

    def interrupt(self):
        self._event.set()

    def is_stopped(self):
        return self._event.is_set()

    def wait(self):
        self._event.wait(self._timeout)

    def reset(self):
        self._event.clear()


class ExponentialBackoff(object):
    """An iterable object that will yield back an exponential delay sequence
    provided an exponent and a number of items to yield. This object may be
    iterated over multiple times (yielding the same sequence each time).
    """
    def __init__(self, count, exponent=2, max_backoff=3600):
        self.count = max(0, int(count))
        self.exponent = exponent
        self.max_backoff = max(0, int(max_backoff))

    def __iter__(self):
        if self.count <= 0:
            raise StopIteration()
        for i in six.moves.range(0, self.count):
            yield min(self.exponent ** i, self.max_backoff)

    def __str__(self):
        return "ExponentialBackoff: %s" % ([str(v) for v in self])


def as_bool(val):
    """Converts an arbitrary value into a boolean."""
    if isinstance(val, bool):
        return val
    if isinstance(val, six.string_types):
        if val.lower() in ('f', 'false', '0', 'n', 'no'):
            return False
        if val.lower() in ('t', 'true', '1', 'y', 'yes'):
            return True
    return bool(val)


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
        raise TypeError("Can not translate %s to an integer." % (obj))
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
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            if not os.path.isdir(path):
                raise
        else:
            raise


class StopWatch(object):
    """A simple timer/stopwatch helper class.

    Inspired by: apache-commons-lang java stopwatch.

    Not thread-safe.
    """
    _STARTED = 'STARTED'
    _STOPPED = 'STOPPED'

    def __init__(self, duration=None):
        self._duration = duration
        self._started_at = None
        self._stopped_at = None
        self._state = None

    def start(self):
        if self._state == self._STARTED:
            return self
        self._started_at = wallclock()
        self._stopped_at = None
        self._state = self._STARTED
        return self

    def elapsed(self):
        if self._state == self._STOPPED:
            return float(self._stopped_at - self._started_at)
        elif self._state == self._STARTED:
            return float(wallclock() - self._started_at)
        else:
            raise RuntimeError("Can not get the elapsed time of an invalid"
                               " stopwatch")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        try:
            self.stop()
        except RuntimeError:
            pass
        # NOTE(harlowja): don't silence the exception.
        return False

    def leftover(self):
        if self._duration is None:
            raise RuntimeError("Can not get the leftover time of a watch that"
                               " has no duration")
        if self._state != self._STARTED:
            raise RuntimeError("Can not get the leftover time of a stopwatch"
                               " that has not been started")
        end_time = self._started_at + self._duration
        return max(0.0, end_time - wallclock())

    def expired(self):
        if self._duration is None:
            return False
        if self.elapsed() > self._duration:
            return True
        return False

    def resume(self):
        if self._state == self._STOPPED:
            self._state = self._STARTED
            return self
        else:
            raise RuntimeError("Can not resume a stopwatch that has not been"
                               " stopped")

    def stop(self):
        if self._state == self._STOPPED:
            return self
        if self._state != self._STARTED:
            raise RuntimeError("Can not stop a stopwatch that has not been"
                               " started")
        self._stopped_at = wallclock()
        self._state = self._STOPPED
        return self


class Notifier(object):
    """A utility helper class that can be used to subscribe to
    notifications of events occurring as well as allow a entity to post said
    notifications to subscribers.
    """

    RESERVED_KEYS = ('details',)
    ANY = '*'

    def __init__(self):
        self._listeners = collections.defaultdict(list)

    def __len__(self):
        """Returns how many callbacks are registered."""
        count = 0
        for (_event_type, callbacks) in six.iteritems(self._listeners):
            count += len(callbacks)
        return count

    def is_registered(self, event_type, callback):
        """Check if a callback is registered."""
        listeners = list(self._listeners.get(event_type, []))
        for (cb, _args, _kwargs) in listeners:
            if reflection.is_same_callback(cb, callback):
                return True
        return False

    def reset(self):
        """Forget all previously registered callbacks."""
        self._listeners.clear()

    def notify(self, event_type, details):
        """Notify about event occurrence.

        All callbacks registered to receive notifications about given
        event type will be called.

        :param event_type: event type that occurred
        :param details: addition event details
        """
        listeners = list(self._listeners.get(self.ANY, []))
        for i in self._listeners[event_type]:
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
                callback(event_type, *args, **kwargs)
            except Exception:
                LOG.warn("Failure calling callback %s to notify about event"
                         " %s, details: %s", callback, event_type,
                         details, exc_info=True)

    def register(self, event_type, callback, args=None, kwargs=None):
        """Register a callback to be called when event of a given type occurs.

        Callback will be called with provided ``args`` and ``kwargs`` and
        when event type occurs (or on any event if ``event_type`` equals to
        ``Notifier.ANY``). It will also get additional keyword argument,
        ``details``, that will hold event details provided to
        :py:meth:`notify` method.
        """
        assert six.callable(callback), "Callback must be callable"
        if self.is_registered(event_type, callback):
            raise ValueError("Callback %s already registered" % (callback))
        if kwargs:
            for k in self.RESERVED_KEYS:
                if k in kwargs:
                    raise KeyError(("Reserved key '%s' not allowed in "
                                    "kwargs") % k)
            kwargs = copy.copy(kwargs)
        if args:
            args = copy.copy(args)
        self._listeners[event_type].append((callback, args, kwargs))

    def deregister(self, event_type, callback):
        """Remove a single callback from listening to event ``event_type``."""
        if event_type not in self._listeners:
            return
        for i, (cb, args, kwargs) in enumerate(self._listeners[event_type]):
            if reflection.is_same_callback(cb, callback):
                self._listeners[event_type].pop(i)
                break


def copy_exc_info(exc_info):
    """Make copy of exception info tuple, as deep as possible."""
    if exc_info is None:
        return None
    exc_type, exc_value, tb = exc_info
    # NOTE(imelnikov): there is no need to copy type, and
    # we can't copy traceback.
    return (exc_type, copy.deepcopy(exc_value), tb)


def are_equal_exc_info_tuples(ei1, ei2):
    if ei1 == ei2:
        return True
    if ei1 is None or ei2 is None:
        return False  # if both are None, we returned True above

    # NOTE(imelnikov): we can't compare exceptions with '=='
    # because we want exc_info be equal to it's copy made with
    # copy_exc_info above.
    if ei1[0] is not ei2[0]:
        return False
    if not all((type(ei1[1]) == type(ei2[1]),
                exc.exception_message(ei1[1]) == exc.exception_message(ei2[1]),
                repr(ei1[1]) == repr(ei2[1]))):
        return False
    if ei1[2] == ei2[2]:
        return True
    tb1 = traceback.format_tb(ei1[2])
    tb2 = traceback.format_tb(ei2[2])
    return tb1 == tb2


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
        return Failure(exc_info=copy_exc_info(self.exc_info),
                       exception_str=self.exception_str,
                       traceback_str=self.traceback_str,
                       exc_type_names=self._exc_type_names[:])
