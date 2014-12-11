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

from __future__ import absolute_import

import logging
import sys

_BASE = __name__.split(".", 1)[0]

# Add a BLATHER level, this matches the multiprocessing utils.py module (and
# kazoo and others) that declares a similar level, this level is for
# information that is even lower level than regular DEBUG and gives out so
# much runtime information that it is only useful by low-level/certain users...
BLATHER = 5

# Copy over *select* attributes to make it easy to use this module.
CRITICAL = logging.CRITICAL
DEBUG = logging.DEBUG
ERROR = logging.ERROR
FATAL = logging.FATAL
NOTSET = logging.NOTSET
WARN = logging.WARN
WARNING = logging.WARNING


class _BlatherLoggerAdapter(logging.LoggerAdapter):

    def blather(self, msg, *args, **kwargs):
        """Delegate a blather call to the underlying logger."""
        self.log(BLATHER, msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        """Delegate a warning call to the underlying logger."""
        self.warning(msg, *args, **kwargs)


# TODO(harlowja): we should remove when we no longer have to support 2.6...
if sys.version_info[0:2] == (2, 6):

    class _FixedBlatherLoggerAdapter(_BlatherLoggerAdapter):
        """Ensures isEnabledFor() exists on adapters that are created."""

        def isEnabledFor(self, level):
            return self.logger.isEnabledFor(level)

    _BlatherLoggerAdapter = _FixedBlatherLoggerAdapter

    # Taken from python2.7 (same in python3.4)...
    class _NullHandler(logging.Handler):
        """This handler does nothing.

        It's intended to be used to avoid the
        "No handlers could be found for logger XXX" one-off warning. This is
        important for library code, which may contain code to log events. If a
        user of the library does not configure logging, the one-off warning
        might be produced; to avoid this, the library developer simply needs
        to instantiate a _NullHandler and add it to the top-level logger of the
        library module or package.
        """

        def handle(self, record):
            """Stub."""

        def emit(self, record):
            """Stub."""

        def createLock(self):
            self.lock = None

else:
    _NullHandler = logging.NullHandler


def getLogger(name=_BASE, extra=None):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.addHandler(_NullHandler())
    return _BlatherLoggerAdapter(logger, extra=extra)
