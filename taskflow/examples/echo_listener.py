# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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
import os
import sys

logging.basicConfig(level=logging.DEBUG)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from taskflow import engines
from taskflow.listeners import logging as logging_listener
from taskflow.patterns import linear_flow as lf
from taskflow import task

# INTRO: This example walks through a miniature workflow which will do a
# simple echo operation; during this execution a listener is associated with
# the engine to receive all notifications about what the flow has performed,
# this example dumps that output to the stdout for viewing (at debug level
# to show all the information which is possible).


class Echo(task.Task):
    def execute(self):
        print(self.name)


# Generate the work to be done (but don't do it yet).
wf = lf.Flow('abc')
wf.add(Echo('a'))
wf.add(Echo('b'))
wf.add(Echo('c'))

# This will associate the listener with the engine (the listener
# will automatically register for notifications with the engine and deregister
# when the context is exited).
e = engines.load(wf)
with logging_listener.DynamicLoggingListener(e):
    e.run()
