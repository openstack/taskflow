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
import logging

import six

from taskflow.utils import reflection

LOG = logging.getLogger(__name__)


class _Listener(object):
    """Internal helper that represents a notification listener/target."""

    def __init__(self, callback, args=None, kwargs=None, details_filter=None):
        self._callback = callback
        self._details_filter = details_filter
        if not args:
            self._args = ()
        else:
            self._args = args[:]
        if not kwargs:
            self._kwargs = {}
        else:
            self._kwargs = kwargs.copy()

    def __call__(self, event_type, details):
        if self._details_filter is not None:
            if not self._details_filter(details):
                return
        kwargs = self._kwargs.copy()
        kwargs['details'] = details
        self._callback(event_type, *self._args, **kwargs)

    def __repr__(self):
        repr_msg = "%s object at 0x%x calling into '%r'" % (
            reflection.get_class_name(self), id(self), self._callback)
        if self._details_filter is not None:
            repr_msg += " using details filter '%r'" % self._details_filter
        return "<%s>" % repr_msg

    def is_equivalent(self, callback, details_filter=None):
        if not reflection.is_same_callback(self._callback, callback):
            return False
        if details_filter is not None:
            if self._details_filter is None:
                return False
            else:
                return reflection.is_same_callback(self._details_filter,
                                                   details_filter)
        else:
            return self._details_filter is None

    def __eq__(self, other):
        if isinstance(other, _Listener):
            return self.is_equivalent(other._callback,
                                      details_filter=other._details_filter)
        else:
            return NotImplemented


class Notifier(object):
    """A notification helper class.

    It is intended to be used to subscribe to notifications of events
    occurring as well as allow a entity to post said notifications to any
    associated subscribers without having either entity care about how this
    notification occurs.
    """

    #: Keys that can *not* be used in callbacks arguments
    RESERVED_KEYS = ('details',)

    #: Kleene star constant that is used to recieve all notifications
    ANY = '*'

    def __init__(self):
        self._listeners = collections.defaultdict(list)

    def __len__(self):
        """Returns how many callbacks are registered."""
        count = 0
        for (_event_type, listeners) in six.iteritems(self._listeners):
            count += len(listeners)
        return count

    def is_registered(self, event_type, callback, details_filter=None):
        """Check if a callback is registered."""
        for listener in self._listeners.get(event_type, []):
            if listener.is_equivalent(callback, details_filter=details_filter):
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
        for listener in self._listeners[event_type]:
            if listener not in listeners:
                listeners.append(listener)
        if not listeners:
            return
        for listener in listeners:
            try:
                listener(event_type, details)
            except Exception:
                LOG.warn("Failure calling listener %s to notify about event"
                         " %s, details: %s", listener, event_type,
                         details, exc_info=True)

    def register(self, event_type, callback,
                 args=None, kwargs=None, details_filter=None):
        """Register a callback to be called when event of a given type occurs.

        Callback will be called with provided ``args`` and ``kwargs`` and
        when event type occurs (or on any event if ``event_type`` equals to
        :attr:`.ANY`). It will also get additional keyword argument,
        ``details``, that will hold event details provided to the
        :meth:`.notify` method (if a details filter callback is provided then
        the target callback will *only* be triggered if the details filter
        callback returns a truthy value).
        """
        if not six.callable(callback):
            raise ValueError("Event callback must be callable")
        if details_filter is not None:
            if not six.callable(details_filter):
                raise ValueError("Details filter must be callable")
        if self.is_registered(event_type, callback,
                              details_filter=details_filter):
            raise ValueError("Event callback already registered with"
                             " equivalent details filter")
        if kwargs:
            for k in self.RESERVED_KEYS:
                if k in kwargs:
                    raise KeyError("Reserved key '%s' not allowed in "
                                   "kwargs" % k)
        self._listeners[event_type].append(
            _Listener(callback,
                      args=args, kwargs=kwargs,
                      details_filter=details_filter))

    def deregister(self, event_type, callback, details_filter=None):
        """Remove a single callback from listening to event ``event_type``."""
        if event_type not in self._listeners:
            return
        for i, listener in enumerate(self._listeners[event_type]):
            if listener.is_equivalent(callback, details_filter=details_filter):
                self._listeners[event_type].pop(i)
                break
