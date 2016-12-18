# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

from __future__ import absolute_import

import abc

from oslo_utils import excutils
import six

from taskflow import logging
from taskflow import states
from taskflow.types import failure
from taskflow.types import notifier

LOG = logging.getLogger(__name__)

#: These states will results be usable, other states do not produce results.
FINISH_STATES = (states.FAILURE, states.SUCCESS,
                 states.REVERTED, states.REVERT_FAILURE)

#: What is listened for by default...
DEFAULT_LISTEN_FOR = (notifier.Notifier.ANY,)


def _task_matcher(details):
    """Matches task details emitted."""
    if not details:
        return False
    if 'task_name' in details and 'task_uuid' in details:
        return True
    return False


def _retry_matcher(details):
    """Matches retry details emitted."""
    if not details:
        return False
    if 'retry_name' in details and 'retry_uuid' in details:
        return True
    return False


def _bulk_deregister(notifier, registered, details_filter=None):
    """Bulk deregisters callbacks associated with many states."""
    while registered:
        state, cb = registered.pop()
        notifier.deregister(state, cb,
                            details_filter=details_filter)


def _bulk_register(watch_states, notifier, cb, details_filter=None):
    """Bulk registers a callback associated with many states."""
    registered = []
    try:
        for state in watch_states:
            if not notifier.is_registered(state, cb,
                                          details_filter=details_filter):
                notifier.register(state, cb,
                                  details_filter=details_filter)
                registered.append((state, cb))
    except ValueError:
        with excutils.save_and_reraise_exception():
            _bulk_deregister(notifier, registered,
                             details_filter=details_filter)
    else:
        return registered


class Listener(object):
    """Base class for listeners.

    A listener can be attached to an engine to do various actions on flow and
    atom state transitions. It implements the context manager protocol to be
    able to register and unregister with a given engine automatically when a
    context is entered and when it is exited.

    To implement a listener, derive from this class and override
    ``_flow_receiver`` and/or ``_task_receiver`` and/or ``_retry_receiver``
    methods (in this class, they do nothing).
    """

    def __init__(self, engine,
                 task_listen_for=DEFAULT_LISTEN_FOR,
                 flow_listen_for=DEFAULT_LISTEN_FOR,
                 retry_listen_for=DEFAULT_LISTEN_FOR):
        if not task_listen_for:
            task_listen_for = []
        if not retry_listen_for:
            retry_listen_for = []
        if not flow_listen_for:
            flow_listen_for = []
        self._listen_for = {
            'task': list(task_listen_for),
            'retry': list(retry_listen_for),
            'flow': list(flow_listen_for),
        }
        self._engine = engine
        self._registered = {}

    def _flow_receiver(self, state, details):
        pass

    def _task_receiver(self, state, details):
        pass

    def _retry_receiver(self, state, details):
        pass

    def deregister(self):
        if 'task' in self._registered:
            _bulk_deregister(self._engine.atom_notifier,
                             self._registered['task'],
                             details_filter=_task_matcher)
            del self._registered['task']
        if 'retry' in self._registered:
            _bulk_deregister(self._engine.atom_notifier,
                             self._registered['retry'],
                             details_filter=_retry_matcher)
            del self._registered['retry']
        if 'flow' in self._registered:
            _bulk_deregister(self._engine.notifier,
                             self._registered['flow'])
            del self._registered['flow']

    def register(self):
        if 'task' not in self._registered:
            self._registered['task'] = _bulk_register(
                self._listen_for['task'], self._engine.atom_notifier,
                self._task_receiver, details_filter=_task_matcher)
        if 'retry' not in self._registered:
            self._registered['retry'] = _bulk_register(
                self._listen_for['retry'], self._engine.atom_notifier,
                self._retry_receiver, details_filter=_retry_matcher)
        if 'flow' not in self._registered:
            self._registered['flow'] = _bulk_register(
                self._listen_for['flow'], self._engine.notifier,
                self._flow_receiver)

    def __enter__(self):
        self.register()
        return self

    def __exit__(self, type, value, tb):
        try:
            self.deregister()
        except Exception:
            # Don't let deregistering throw exceptions
            LOG.warning("Failed deregistering listeners from engine %s",
                        self._engine, exc_info=True)


@six.add_metaclass(abc.ABCMeta)
class DumpingListener(Listener):
    """Abstract base class for dumping listeners.

    This provides a simple listener that can be attached to an engine which can
    be derived from to dump task and/or flow state transitions to some target
    backend.

    To implement your own dumping listener derive from this class and
    override the ``_dump`` method.
    """

    @abc.abstractmethod
    def _dump(self, message, *args, **kwargs):
        """Dumps the provided *templated* message to some output."""

    def _flow_receiver(self, state, details):
        self._dump("%s has moved flow '%s' (%s) into state '%s'"
                   " from state '%s'", self._engine, details['flow_name'],
                   details['flow_uuid'], state, details['old_state'])

    def _task_receiver(self, state, details):
        if state in FINISH_STATES:
            result = details.get('result')
            exc_info = None
            was_failure = False
            if isinstance(result, failure.Failure):
                if result.exc_info:
                    exc_info = tuple(result.exc_info)
                was_failure = True
            self._dump("%s has moved task '%s' (%s) into state '%s'"
                       " from state '%s' with result '%s' (failure=%s)",
                       self._engine, details['task_name'],
                       details['task_uuid'], state, details['old_state'],
                       result, was_failure, exc_info=exc_info)
        else:
            self._dump("%s has moved task '%s' (%s) into state '%s'"
                       " from state '%s'", self._engine, details['task_name'],
                       details['task_uuid'], state, details['old_state'])
