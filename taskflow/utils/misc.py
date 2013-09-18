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

from distutils import version

import collections
import copy
import errno
import logging
import os
import sys

import six

LOG = logging.getLogger(__name__)


def get_task_version(task):
    """Gets a tasks *string* version, whether it is a task object/function."""
    task_version = getattr(task, 'version')
    if isinstance(task_version, (list, tuple)):
        task_version = '.'.join(str(item) for item in task_version)
    if task_version is not None and not isinstance(task_version,
                                                   six.string_types):
        task_version = str(task_version)
    return task_version


class ExponentialBackoff(object):
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
    if isinstance(val, bool):
        return val
    if isinstance(val, six.string_types):
        if val.lower() in ('f', 'false', '0', 'n', 'no'):
            return False
        if val.lower() in ('t', 'true', '1', 'y', 'yes'):
            return True
    return bool(val)


def as_int(obj, quiet=False):
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


def is_version_compatible(version_1, version_2):
    """Checks for major version compatibility of two *string" versions."""
    try:
        version_1_tmp = version.StrictVersion(version_1)
        version_2_tmp = version.StrictVersion(version_2)
    except ValueError:
        version_1_tmp = version.LooseVersion(version_1)
        version_2_tmp = version.LooseVersion(version_2)
    version_1 = version_1_tmp
    version_2 = version_2_tmp
    if version_1 == version_2 or version_1.version[0] == version_2.version[0]:
        return True
    return False


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


class LastFedIter(object):
    """An iterator which yields back the first item and then yields back
    results from the provided iterator.
    """

    def __init__(self, first, rest_itr):
        self.first = first
        self.rest_itr = rest_itr

    def __iter__(self):
        yield self.first
        for i in self.rest_itr:
            yield i


class Failure(object):
    """Indicates failure"""
    # NOTE(imelnikov): flow_utils.FlowFailure uses runner, but
    #   engine code does not, so we need separate class

    def __init__(self, exc_info=None):
        if exc_info is not None:
            self._exc_info = exc_info
        else:
            self._exc_info = sys.exc_info()

    @property
    def exc_info(self):
        return self._exc_info

    @property
    def exc(self):
        return self._exc_info[1]

    def reraise(self):
        raise self.exc_info[0], self.exc_info[1], self.exc_info[2]

    def __str__(self):
        try:
            exc_name = self.exc_info[0].__name__
        except AttributeError:
            exc_name = str(self.exc_info)
        return 'Failure: %s: %s' % (exc_name, self.exc_info[1])
