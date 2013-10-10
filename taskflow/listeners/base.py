# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import six

from taskflow import states
from taskflow.utils import misc

LOG = logging.getLogger(__name__)

# NOTE(harlowja): on these states will results be useable, all other states
# do not produce results.
FINISH_STATES = (states.FAILURE, states.SUCCESS)


class LoggingBase(six.with_metaclass(abc.ABCMeta)):
    """This provides a simple listener that can be attached to an engine which
    can be derived from to log the received actions to some logging backend. It
    provides a useful context manager access to be able to register and
    unregister with a given engine automatically when a context is entered and
    when it is exited.
    """
    def __init__(self, engine,
                 listen_for=misc.TransitionNotifier.ANY):
        self._listen_for = listen_for
        self._engine = engine
        self._registered = False

    @abc.abstractmethod
    def _log(self, message, *args, **kwargs):
        raise NotImplementedError()

    def _flow_receiver(self, state, details):
        self._log("%s has moved flow '%s' (%s) into state '%s'",
                  details['engine'], details['flow_name'],
                  details['flow_uuid'], state)

    def _task_receiver(self, state, details):
        if state in FINISH_STATES:
            result = details.get('result')
            exc_info = None
            was_failure = False
            if isinstance(result, misc.Failure):
                if result.exc_info:
                    exc_info = tuple(result.exc_info)
                was_failure = True
            self._log("%s has moved task '%s' (%s) into state '%s'"
                      " with result '%s' (failure=%s)",
                      details['engine'], details['task_name'],
                      details['task_uuid'], state, result, was_failure,
                      exc_info=exc_info)
        else:
            self._log("%s has moved task '%s' (%s) into state '%s'",
                      details['engine'], details['task_name'],
                      details['task_uuid'], state)

    def __enter__(self):
        if not self._registered:
            self._engine.notifier.register(self._listen_for,
                                           self._flow_receiver)
            self._engine.task_notifier.register(self._listen_for,
                                                self._task_receiver)
            self._registered = True

    def __exit__(self, type, value, tb):
        try:
            self._engine.notifier.deregister(self._listen_for,
                                             self._flow_receiver)
            self._engine.task_notifier.deregister(self._listen_for,
                                                  self._task_receiver)
            self._registered = False
        except Exception:
            # Don't let deregistering throw exceptions
            LOG.exception("Failed deregistering listeners from engine %s",
                          self._engine)
