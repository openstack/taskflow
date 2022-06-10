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
import contextlib
import copy
import logging

from oslo_utils import reflection

LOG = logging.getLogger(__name__)


class Listener(object):
    """Immutable helper that represents a notification listener/target."""

    def __init__(self, callback, args=None, kwargs=None, details_filter=None):
        """Initialize members

        :param callback: callback function
        :param details_filter: a callback that will be called before the
                               actual callback that can be used to discard
                               the event (thus avoiding the invocation of
                               the actual callback)
        :param args: non-keyworded arguments
        :type args: list/iterable/tuple
        :param kwargs: key-value pair arguments
        :type kwargs: dictionary
        """
        self._callback = callback
        self._details_filter = details_filter
        if not args:
            self._args = ()
        else:
            if not isinstance(args, tuple):
                self._args = tuple(args)
            else:
                self._args = args
        if not kwargs:
            self._kwargs = {}
        else:
            self._kwargs = kwargs.copy()

    @property
    def callback(self):
        """Callback (can not be none) to call with event + details."""
        return self._callback

    @property
    def details_filter(self):
        """Callback (may be none) to call to discard events + details."""
        return self._details_filter

    @property
    def kwargs(self):
        """Dictionary of keyword arguments to use in future calls."""
        return self._kwargs.copy()

    @property
    def args(self):
        """Tuple of positional arguments to use in future calls."""
        return self._args

    def __call__(self, event_type, details):
        """Activate the target callback with the given event + details.

        NOTE(harlowja): if a details filter callback exists and it returns
        a falsey value when called with the provided ``details``, then the
        target callback will **not** be called.
        """
        if self._details_filter is not None:
            if not self._details_filter(details):
                return
        kwargs = self._kwargs.copy()
        kwargs['details'] = details
        self._callback(event_type, *self._args, **kwargs)

    def __repr__(self):
        repr_msg = "%s object at 0x%x calling into '%r'" % (
            reflection.get_class_name(self, fully_qualified=False),
            id(self), self._callback)
        if self._details_filter is not None:
            repr_msg += " using details filter '%r'" % self._details_filter
        return "<%s>" % repr_msg

    def is_equivalent(self, callback, details_filter=None):
        """Check if the callback is same

        :param callback: callback used for comparison
        :param details_filter: callback used for comparison
        :returns: false if not the same callback, otherwise true
        :rtype: boolean
        """
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
        if isinstance(other, Listener):
            return self.is_equivalent(other._callback,
                                      details_filter=other._details_filter)
        else:
            return NotImplemented

    def __ne__(self, other):
        return not self.__eq__(other)


class Notifier(object):
    """A notification (`pub/sub`_ *like*) helper class.

    It is intended to be used to subscribe to notifications of events
    occurring as well as allow a entity to post said notifications to any
    associated subscribers without having either entity care about how this
    notification occurs.

    **Not** thread-safe when a single notifier is mutated at the same
    time by multiple threads. For example having multiple threads call
    into :py:meth:`.register` or :py:meth:`.reset` at the same time could
    potentially end badly. It is thread-safe when
    only :py:meth:`.notify` calls or other read-only actions (like calling
    into :py:meth:`.is_registered`) are occurring at the same time.

    .. _pub/sub: http://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern
    """

    #: Keys that can *not* be used in callbacks arguments
    RESERVED_KEYS = ('details',)

    #: Kleene star constant that is used to receive all notifications
    ANY = '*'

    #: Events which can *not* be used to trigger notifications
    _DISALLOWED_NOTIFICATION_EVENTS = set([ANY])

    def __init__(self):
        self._topics = collections.defaultdict(list)

    def __len__(self):
        """Returns how many callbacks are registered.

        :returns: count of how many callbacks are registered
        :rtype: number
        """
        count = 0
        for (_event_type, listeners) in self._topics.items():
            count += len(listeners)
        return count

    def is_registered(self, event_type, callback, details_filter=None):
        """Check if a callback is registered.

        :returns: checks if the callback is registered
        :rtype: boolean
        """
        for listener in self._topics.get(event_type, []):
            if listener.is_equivalent(callback, details_filter=details_filter):
                return True
        return False

    def reset(self):
        """Forget all previously registered callbacks."""
        self._topics.clear()

    def notify(self, event_type, details):
        """Notify about event occurrence.

        All callbacks registered to receive notifications about given
        event type will be called. If the provided event type can not be
        used to emit notifications (this is checked via
        the :meth:`.can_be_registered` method) then it will silently be
        dropped (notification failures are not allowed to cause or
        raise exceptions).

        :param event_type: event type that occurred
        :param details: additional event details *dictionary* passed to
                        callback keyword argument with the same name
        :type details: dictionary
        """
        if not self.can_trigger_notification(event_type):
            LOG.debug("Event type '%s' is not allowed to trigger"
                      " notifications", event_type)
            return
        listeners = list(self._topics.get(self.ANY, []))
        listeners.extend(self._topics.get(event_type, []))
        if not listeners:
            return
        if not details:
            details = {}
        for listener in listeners:
            try:
                listener(event_type, details.copy())
            except Exception:
                LOG.warning("Failure calling listener %s to notify about event"
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

        :param event_type: event type input
        :param callback: function callback to be registered.
        :param args: non-keyworded arguments
        :type args: list
        :param kwargs: key-value pair arguments
        :type kwargs: dictionary
        """
        if not callable(callback):
            raise ValueError("Event callback must be callable")
        if details_filter is not None:
            if not callable(details_filter):
                raise ValueError("Details filter must be callable")
        if not self.can_be_registered(event_type):
            raise ValueError("Disallowed event type '%s' can not have a"
                             " callback registered" % event_type)
        if self.is_registered(event_type, callback,
                              details_filter=details_filter):
            raise ValueError("Event callback already registered with"
                             " equivalent details filter")
        if kwargs:
            for k in self.RESERVED_KEYS:
                if k in kwargs:
                    raise KeyError("Reserved key '%s' not allowed in "
                                   "kwargs" % k)
        self._topics[event_type].append(
            Listener(callback,
                     args=args, kwargs=kwargs,
                     details_filter=details_filter))

    def deregister(self, event_type, callback, details_filter=None):
        """Remove a single listener bound to event ``event_type``.

        :param event_type: deregister listener bound to event_type
        """
        if event_type not in self._topics:
            return False
        for i, listener in enumerate(self._topics.get(event_type, [])):
            if listener.is_equivalent(callback, details_filter=details_filter):
                self._topics[event_type].pop(i)
                return True
        return False

    def deregister_event(self, event_type):
        """Remove a group of listeners bound to event ``event_type``.

        :param event_type: deregister listeners bound to event_type
        """
        return len(self._topics.pop(event_type, []))

    def copy(self):
        c = copy.copy(self)
        c._topics = collections.defaultdict(list)
        for (event_type, listeners) in self._topics.items():
            c._topics[event_type] = listeners[:]
        return c

    def listeners_iter(self):
        """Return an iterator over the mapping of event => listeners bound.

        NOTE(harlowja): Each listener in the yielded (event, listeners)
        tuple is an instance of the :py:class:`~.Listener`  type, which
        itself wraps a provided callback (and its details filter
        callback, if any).
        """
        for event_type, listeners in self._topics.items():
            if listeners:
                yield (event_type, listeners)

    def can_be_registered(self, event_type):
        """Checks if the event can be registered/subscribed to."""
        return True

    def can_trigger_notification(self, event_type):
        """Checks if the event can trigger a notification.

        :param event_type: event that needs to be verified
        :returns: whether the event can trigger a notification
        :rtype: boolean
        """
        if event_type in self._DISALLOWED_NOTIFICATION_EVENTS:
            return False
        else:
            return True


class RestrictedNotifier(Notifier):
    """A notification class that restricts events registered/triggered.

    NOTE(harlowja): This class unlike :class:`.Notifier` restricts and
    disallows registering callbacks for event types that are not declared
    when constructing the notifier.
    """

    def __init__(self, watchable_events, allow_any=True):
        super(RestrictedNotifier, self).__init__()
        self._watchable_events = frozenset(watchable_events)
        self._allow_any = allow_any

    def events_iter(self):
        """Returns iterator of events that can be registered/subscribed to.

        NOTE(harlowja): does not include back the ``ANY`` event type as that
        meta-type is not a specific event but is a capture-all that does not
        imply the same meaning as specific event types.
        """
        for event_type in self._watchable_events:
            yield event_type

    def can_be_registered(self, event_type):
        """Checks if the event can be registered/subscribed to.

        :param event_type: event that needs to be verified
        :returns: whether the event can be registered/subscribed to
        :rtype: boolean
        """
        return (event_type in self._watchable_events or
                (event_type == self.ANY and self._allow_any))


@contextlib.contextmanager
def register_deregister(notifier, event_type, callback=None,
                        args=None, kwargs=None, details_filter=None):
    """Context manager that registers a callback, then deregisters on exit.

    NOTE(harlowja): if the callback is none, then this registers nothing, which
                    is different from the behavior of the ``register`` method
                    which will *not* accept none as it is not callable...
    """
    if callback is None:
        yield
    else:
        notifier.register(event_type, callback,
                          args=args, kwargs=kwargs,
                          details_filter=details_filter)
        try:
            yield
        finally:
            notifier.deregister(event_type, callback,
                                details_filter=details_filter)
