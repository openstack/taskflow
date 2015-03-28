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
import os
import sys

logging.basicConfig(level=logging.ERROR)

self_dir = os.path.abspath(os.path.dirname(__file__))
top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)
sys.path.insert(0, self_dir)

from taskflow import engines
from taskflow.patterns import linear_flow
from taskflow import task

# INTRO: This example shows how a task (in a linear/serial workflow) can
# produce an output that can be then consumed/used by a downstream task.


class TaskA(task.Task):
    default_provides = 'a'

    def execute(self):
        print("Executing '%s'" % (self.name))
        return 'a'


class TaskB(task.Task):
    def execute(self, a):
        print("Executing '%s'" % (self.name))
        print("Got input '%s'" % (a))


print("Constructing...")
wf = linear_flow.Flow("pass-from-to")
wf.add(TaskA('a'), TaskB('b'))

print("Loading...")
e = engines.load(wf)

print("Compiling...")
e.compile()

print("Preparing...")
e.prepare()

print("Running...")
e.run()

print("Done...")
