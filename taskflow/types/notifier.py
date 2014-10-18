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
import logging

import six

from taskflow.utils import reflection

LOG = logging.getLogger(__name__)


class Notifier(object):
    """A notification helper class.

    It is intended to be used to subscribe to notifications of events
    occurring as well as allow a entity to post said notifications to any
    associated subscribers without having either entity care about how this
    notification occurs.
    """

    #: Keys that can not be used in callbacks arguments
    RESERVED_KEYS = ('details',)

    #: Kleene star constant that is used to recieve all notifications
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
        :attr:`.ANY`). It will also get additional keyword argument,
        ``details``, that will hold event details provided to the
        :meth:`.notify` method.
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
