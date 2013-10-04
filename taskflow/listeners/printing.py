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

from __future__ import print_function

import sys
import traceback

from taskflow.listeners import base
from taskflow.utils import misc


class PrintingListener(base.LoggingBase):
    """Writes the task and flow notifications messages to stdout or stderr"""
    def __init__(self, engine,
                 listen_for=misc.TransitionNotifier.ANY, stderr=False):
        super(PrintingListener, self).__init__(engine, listen_for)
        if stderr:
            self._file = sys.stderr
        else:
            self._file = sys.stdout

    def _log(self, message, *args, **kwargs):
        print(message % args, file=self._file)
        exc_info = kwargs.get('exc_info')
        if exc_info is not None:
            traceback.print_exception(exc_info[0], exc_info[1], exc_info[2],
                                      file=self._file)
