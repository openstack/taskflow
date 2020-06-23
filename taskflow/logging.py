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

import logging

_BASE = __name__.split(".", 1)[0]

# Add a BLATHER/TRACE level, this matches the multiprocessing
# utils.py module (and oslo.log, kazoo and others) that declares a similar
# level, this level is for information that is even lower level than regular
# DEBUG and gives out so much runtime information that it is only
# useful by low-level/certain users...
BLATHER = 5
TRACE = BLATHER


# Copy over *select* attributes to make it easy to use this module.
CRITICAL = logging.CRITICAL
DEBUG = logging.DEBUG
ERROR = logging.ERROR
FATAL = logging.FATAL
INFO = logging.INFO
NOTSET = logging.NOTSET
WARN = logging.WARN
WARNING = logging.WARNING


class _TraceLoggerAdapter(logging.LoggerAdapter):

    def trace(self, msg, *args, **kwargs):
        """Delegate a trace call to the underlying logger."""
        self.log(TRACE, msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        """Delegate a warning call to the underlying logger."""
        self.warning(msg, *args, **kwargs)


def getLogger(name=_BASE, extra=None):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return _TraceLoggerAdapter(logger, extra=extra)
