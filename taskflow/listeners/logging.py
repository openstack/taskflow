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

import logging as logging_base
import os
import sys

from taskflow.listeners import base
from taskflow import logging
from taskflow import states
from taskflow.types import failure
from taskflow.utils import misc

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
    if _PY26 and isinstance(logger, logging_base.LoggerAdapter):
        return logger.logger.isEnabledFor(level)
    return logger.isEnabledFor(level)


class LoggingListener(base.DumpingListener):
    """Listener that logs notifications it receives.

    It listens for task and flow notifications and writes those notifications
    to a provided logger, or logger of its module
    (``taskflow.listeners.logging``) if none is provided (and no class
    attribute is overriden). The log level can also be
    configured, ``logging.DEBUG`` is used by default when none is provided.
    """

    #: Default logger to use if one is not provided on construction.
    _LOGGER = None

    def __init__(self, engine,
                 task_listen_for=base.DEFAULT_LISTEN_FOR,
                 flow_listen_for=base.DEFAULT_LISTEN_FOR,
                 retry_listen_for=base.DEFAULT_LISTEN_FOR,
                 log=None,
                 level=logging.DEBUG):
        super(LoggingListener, self).__init__(
            engine, task_listen_for=task_listen_for,
            flow_listen_for=flow_listen_for, retry_listen_for=retry_listen_for)
        self._logger = misc.pick_first_not_none(log, self._LOGGER, LOG)
        self._level = level

    def _dump(self, message, *args, **kwargs):
        self._logger.log(self._level, message, *args, **kwargs)


class DynamicLoggingListener(base.Listener):
    """Listener that logs notifications it receives.

    It listens for task and flow notifications and writes those notifications
    to a provided logger, or logger of its module
    (``taskflow.listeners.logging``) if none is provided (and no class
    attribute is overriden). The log level can *slightly* be configured
    and ``logging.DEBUG`` or ``logging.WARNING`` (unless overriden via a
    constructor parameter) will be selected automatically based on the
    execution state and results produced.

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

    #: Default logger to use if one is not provided on construction.
    _LOGGER = None

    def __init__(self, engine,
                 task_listen_for=base.DEFAULT_LISTEN_FOR,
                 flow_listen_for=base.DEFAULT_LISTEN_FOR,
                 retry_listen_for=base.DEFAULT_LISTEN_FOR,
                 log=None, failure_level=logging.WARNING,
                 level=logging.DEBUG):
        super(DynamicLoggingListener, self).__init__(
            engine, task_listen_for=task_listen_for,
            flow_listen_for=flow_listen_for, retry_listen_for=retry_listen_for)
        self._failure_level = failure_level
        self._level = level
        self._task_log_levels = {
            states.FAILURE: self._failure_level,
            states.REVERTED: self._failure_level,
            states.RETRYING: self._failure_level,
        }
        self._flow_log_levels = {
            states.FAILURE: self._failure_level,
            states.REVERTED: self._failure_level,
        }
        self._logger = misc.pick_first_not_none(log, self._LOGGER, LOG)

    @staticmethod
    def _format_failure(fail):
        """Returns a (exc_info, exc_details) tuple about the failure.

        The ``exc_info`` tuple should be a standard three element
        (exctype, value, traceback) tuple that will be used for further
        logging. If a non-empty string is returned for ``exc_details`` it
        should contain any string info about the failure (with any specific
        details the ``exc_info`` may not have/contain). If the ``exc_info``
        tuple is returned as ``None`` then it will cause the logging
        system to avoid outputting any traceback information (read
        the python documentation on the logger interaction with ``exc_info``
        to learn more).
        """
        if fail.exc_info:
            exc_info = fail.exc_info
            exc_details = ''
        else:
            # When a remote failure occurs (or somehow the failure
            # object lost its traceback), we will not have a valid
            # exc_info that can be used but we *should* have a string
            # version that we can use instead...
            exc_info = None
            exc_details = "%s%s" % (os.linesep, fail.pformat(traceback=True))
        return (exc_info, exc_details)

    def _flow_receiver(self, state, details):
        """Gets called on flow state changes."""
        level = self._flow_log_levels.get(state, self._level)
        self._logger.log(level, "Flow '%s' (%s) transitioned into state '%s'"
                         " from state '%s'", details['flow_name'],
                         details['flow_uuid'], state, details.get('old_state'))

    def _task_receiver(self, state, details):
        """Gets called on task state changes."""
        if 'result' in details and state in base.FINISH_STATES:
            # If the task failed, it's useful to show the exception traceback
            # and any other available exception information.
            result = details.get('result')
            if isinstance(result, failure.Failure):
                exc_info, exc_details = self._format_failure(result)
                self._logger.log(self._failure_level,
                                 "Task '%s' (%s) transitioned into state"
                                 " '%s' from state '%s'%s",
                                 details['task_name'], details['task_uuid'],
                                 state, details['old_state'], exc_details,
                                 exc_info=exc_info)
            else:
                # Otherwise, depending on the enabled logging level/state we
                # will show or hide results that the task may have produced
                # during execution.
                level = self._task_log_levels.get(state, self._level)
                if (_isEnabledFor(self._logger, self._level)
                        or state == states.FAILURE):
                    self._logger.log(level, "Task '%s' (%s) transitioned into"
                                     " state '%s' from state '%s' with"
                                     " result '%s'", details['task_name'],
                                     details['task_uuid'], state,
                                     details['old_state'], result)
                else:
                    self._logger.log(level, "Task '%s' (%s) transitioned into"
                                     " state '%s' from state '%s'",
                                     details['task_name'],
                                     details['task_uuid'], state,
                                     details['old_state'])
        else:
            # Just a intermediary state, carry on!
            level = self._task_log_levels.get(state, self._level)
            self._logger.log(level, "Task '%s' (%s) transitioned into state"
                             " '%s' from state '%s'", details['task_name'],
                             details['task_uuid'], state, details['old_state'])
