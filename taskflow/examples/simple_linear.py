# -*- coding: utf-8 -*-

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

import taskflow.engines
from taskflow.patterns import linear_flow as lf
from taskflow import task

# INTRO: In this example we create two tasks, each of which ~calls~ a given
# ~phone~ number (provided as a function input) in a linear fashion (one after
# the other). For a workflow which is serial this shows a extremely simple way
# of structuring your tasks (the code that does the work) into a linear
# sequence (the flow) and then passing the work off to an engine, with some
# initial data to be ran in a reliable manner.
#
# NOTE(harlowja): This example shows a basic usage of the taskflow structures
# without involving the complexity of persistence. Using the structures that
# taskflow provides via tasks and flows makes it possible for you to easily at
# a later time hook in a persistence layer (and then gain the functionality
# that offers) when you decide the complexity of adding that layer in
# is 'worth it' for your application's usage pattern (which certain
# applications may not need).


class CallJim(task.Task):
    def execute(self, jim_number, *args, **kwargs):
        print("Calling jim %s." % jim_number)


class CallJoe(task.Task):
    def execute(self, joe_number, *args, **kwargs):
        print("Calling joe %s." % joe_number)


# Create your flow and associated tasks (the work to be done).
flow = lf.Flow('simple-linear').add(
    CallJim(),
    CallJoe()
)

# Now run that flow using the provided initial data (store below).
taskflow.engines.run(flow, store=dict(joe_number=444,
                                      jim_number=555))
