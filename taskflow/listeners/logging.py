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

import logging
import sys

from taskflow.listeners import base
from taskflow import states
from taskflow.types import failure
from taskflow.types import notifier

LOG = logging.getLogger(__name__)

if sys.version_info[0:2] == (2, 6):
    _PY26 = True
else:
    _PY26 = False


# Fixes this for python 2.6 which was missing the is enabled for method
# when a logger adapter is being used/provided, this will no longer be needed
# when we can just support python 2.7+ (which fixed the lack of this method
# on adapters).
def _isEnabledFor(logger, level):
    if _PY26 and isinstance(logger, logging.LoggerAdapter):
        return logger.logger.isEnabledFor(level)
    return logger.isEnabledFor(level)


class LoggingListener(base.LoggingBase):
    """Listener that logs notifications it receives.

    It listens for task and flow notifications and writes those notifications
    to a provided logger, or logger of its module
    (``taskflow.listeners.logging``) if none is provided. The log level
    can also be configured, ``logging.DEBUG`` is used by default when none
    is provided.
    """
    def __init__(self, engine,
                 task_listen_for=(notifier.Notifier.ANY,),
                 flow_listen_for=(notifier.Notifier.ANY,),
                 log=None,
                 level=logging.DEBUG):
        super(LoggingListener, self).__init__(engine,
                                              task_listen_for=task_listen_for,
                                              flow_listen_for=flow_listen_for)
        if not log:
            self._logger = LOG
        else:
            self._logger = log
        self._level = level

    def _log(self, message, *args, **kwargs):
        self._logger.log(self._level, message, *args, **kwargs)


class DynamicLoggingListener(base.ListenerBase):
    """Listener that logs notifications it receives.

    It listens for task and flow notifications and writes those notifications
    to a provided logger, or logger of its module
    (``taskflow.listeners.logging``) if none is provided. The log level
    can *slightly* be configured and ``logging.DEBUG`` or ``logging.WARNING``
    (unless overriden via a constructor parameter) will be selected
    automatically based on the execution state and results produced.

    The following flow states cause ``logging.WARNING`` (or provided
    level) to be used:

    * ``states.FAILURE``
    * ``states.REVERTED``

    The following task states cause ``logging.WARNING`` (or provided level)
    to be used:

    * ``states.FAILURE``
    * ``states.RETRYING``
    * ``states.REVERTING``

    When a task produces a :py:class:`~taskflow.types.failure.Failure` object
    as its result (typically this happens when a task raises an exception) this
    will **always** switch the logger to use ``logging.WARNING`` (if the
    failure object contains a ``exc_info`` tuple this will also be logged to
    provide a meaningful traceback).
    """

    def __init__(self, engine,
                 task_listen_for=(notifier.Notifier.ANY,),
                 flow_listen_for=(notifier.Notifier.ANY,),
                 log=None, failure_level=logging.WARNING,
                 level=logging.DEBUG):
        super(DynamicLoggingListener, self).__init__(
            engine,
            task_listen_for=task_listen_for,
            flow_listen_for=flow_listen_for)
        self._failure_level = failure_level
        self._level = level
        if not log:
            self._logger = LOG
        else:
            self._logger = log

    def _flow_receiver(self, state, details):
        # Gets called on flow state changes.
        level = self._level
        if state in (states.FAILURE, states.REVERTED):
            level = self._failure_level
        self._logger.log(level, "Flow '%s' (%s) transitioned into state '%s'"
                         " from state '%s'", details['flow_name'],
                         details['flow_uuid'], state, details.get('old_state'))

    def _task_receiver(self, state, details):
        # Gets called on task state changes.
        if 'result' in details and state in base.FINISH_STATES:
            # If the task failed, it's useful to show the exception traceback
            # and any other available exception information.
            result = details.get('result')
            if isinstance(result, failure.Failure):
                if result.exc_info:
                    exc_info = result.exc_info
                    manual_tb = ''
                else:
                    # When a remote failure occurs (or somehow the failure
                    # object lost its traceback), we will not have a valid
                    # exc_info that can be used but we *should* have a string
                    # version that we can use instead...
                    exc_info = None
                    manual_tb = "\n%s" % result.pformat(traceback=True)
                self._logger.log(self._failure_level,
                                 "Task '%s' (%s) transitioned into state"
                                 " '%s'%s", details['task_name'],
                                 details['task_uuid'], state, manual_tb,
                                 exc_info=exc_info)
            else:
                # Otherwise, depending on the enabled logging level/state we
                # will show or hide results that the task may have produced
                # during execution.
                level = self._level
                if state == states.FAILURE:
                    level = self._failure_level
                if (_isEnabledFor(self._logger, self._level)
                        or state == states.FAILURE):
                    self._logger.log(level, "Task '%s' (%s) transitioned into"
                                     " state '%s' with result '%s'",
                                     details['task_name'],
                                     details['task_uuid'], state,
                                     result)
                else:
                    self._logger.log(level, "Task '%s' (%s) transitioned into"
                                     " state '%s'", details['task_name'],
                                     details['task_uuid'], state)
        else:
            level = self._level
            if state in (states.REVERTING, states.RETRYING):
                level = self._failure_level
            self._logger.log(level, "Task '%s' (%s) transitioned into state"
                             " '%s'", details['task_name'],
                             details['task_uuid'], state)
