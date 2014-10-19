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
import logging

from oslo.utils import excutils
import six

from taskflow import states
from taskflow.types import failure
from taskflow.types import notifier

LOG = logging.getLogger(__name__)

# NOTE(harlowja): on these states will results be usable, all other states
# do not produce results.
FINISH_STATES = (states.FAILURE, states.SUCCESS)


class ListenerBase(object):
    """Base class for listeners.

    A listener can be attached to an engine to do various actions on flow and
    task state transitions.  It implements context manager protocol to be able
    to register and unregister with a given engine automatically when a context
    is entered and when it is exited.

    To implement a listener, derive from this class and override
    ``_flow_receiver`` and/or ``_task_receiver`` methods (in this class,
    they do nothing).
    """

    def __init__(self, engine,
                 task_listen_for=(notifier.Notifier.ANY,),
                 flow_listen_for=(notifier.Notifier.ANY,)):
        if not task_listen_for:
            task_listen_for = []
        if not flow_listen_for:
            flow_listen_for = []
        self._listen_for = {
            'task': list(task_listen_for),
            'flow': list(flow_listen_for),
        }
        self._engine = engine
        self._registered = False

    def _flow_receiver(self, state, details):
        pass

    def _task_receiver(self, state, details):
        pass

    def deregister(self):
        if not self._registered:
            return

        def _deregister(watch_states, notifier, cb):
            for s in watch_states:
                notifier.deregister(s, cb)

        _deregister(self._listen_for['task'], self._engine.task_notifier,
                    self._task_receiver)
        _deregister(self._listen_for['flow'], self._engine.notifier,
                    self._flow_receiver)

        self._registered = False

    def register(self):
        if self._registered:
            return

        def _register(watch_states, notifier, cb):
            registered = []
            try:
                for s in watch_states:
                    if not notifier.is_registered(s, cb):
                        notifier.register(s, cb)
                        registered.append((s, cb))
            except ValueError:
                with excutils.save_and_reraise_exception():
                    for (s, cb) in registered:
                        notifier.deregister(s, cb)

        _register(self._listen_for['task'], self._engine.task_notifier,
                  self._task_receiver)
        _register(self._listen_for['flow'], self._engine.notifier,
                  self._flow_receiver)

        self._registered = True

    def __enter__(self):
        self.register()

    def __exit__(self, type, value, tb):
        try:
            self.deregister()
        except Exception:
            # Don't let deregistering throw exceptions
            LOG.warn("Failed deregistering listeners from engine %s",
                     self._engine, exc_info=True)


@six.add_metaclass(abc.ABCMeta)
class LoggingBase(ListenerBase):
    """Abstract base class for logging listeners.

    This provides a simple listener that can be attached to an engine which can
    be derived from to log task and/or flow state transitions to some logging
    backend.

    To implement your own logging listener derive form this class and
    override the ``_log`` method.
    """

    @abc.abstractmethod
    def _log(self, message, *args, **kwargs):
        """Logs the provided *templated* message to some output."""

    def _flow_receiver(self, state, details):
        self._log("%s has moved flow '%s' (%s) into state '%s'",
                  self._engine, details['flow_name'],
                  details['flow_uuid'], state)

    def _task_receiver(self, state, details):
        if state in FINISH_STATES:
            result = details.get('result')
            exc_info = None
            was_failure = False
            if isinstance(result, failure.Failure):
                if result.exc_info:
                    exc_info = tuple(result.exc_info)
                was_failure = True
            self._log("%s has moved task '%s' (%s) into state '%s'"
                      " with result '%s' (failure=%s)",
                      self._engine, details['task_name'],
                      details['task_uuid'], state, result, was_failure,
                      exc_info=exc_info)
        else:
            self._log("%s has moved task '%s' (%s) into state '%s'",
                      self._engine, details['task_name'],
                      details['task_uuid'], state)
