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
from taskflow.patterns import unordered_flow as uf
from taskflow import task

# INTRO: These examples show how a linear flow and an unordered flow can be
# used together to execute calculations in parallel and then use the
# result for the next task/s. The adder task is used for all calculations
# and argument bindings are used to set correct parameters for each task.


# This task provides some values from as a result of execution, this can be
# useful when you want to provide values from a static set to other tasks that
# depend on those values existing before those tasks can run.
#
# NOTE(harlowja): this usage is *depreciated* in favor of a simpler mechanism
# that provides those values on engine running by prepopulating the storage
# backend before your tasks are ran (which accomplishes a similar goal in a
# more uniform manner).
class Provider(task.Task):
    def __init__(self, name, *args, **kwargs):
        super(Provider, self).__init__(name=name, **kwargs)
        self._provide = args

    def execute(self):
        return self._provide


# This task adds two input variables and returns the result of that addition.
#
# Note that since this task does not have a revert() function (since addition
# is a stateless operation) there are no side-effects that this function needs
# to undo if some later operation fails.
class Adder(task.Task):
    def execute(self, x, y):
        return x + y


flow = lf.Flow('root').add(
    # Provide the initial values for other tasks to depend on.
    #
    # x1 = 2, y1 = 3, x2 = 5, x3 = 8
    Provider("provide-adder", 2, 3, 5, 8,
             provides=('x1', 'y1', 'x2', 'y2')),
    # Note here that we define the flow that contains the 2 adders to be an
    # unordered flow since the order in which these execute does not matter,
    # another way to solve this would be to use a graph_flow pattern, which
    # also can run in parallel (since they have no ordering dependencies).
    uf.Flow('adders').add(
        # Calculate 'z1 = x1+y1 = 5'
        #
        # Rebind here means that the execute() function x argument will be
        # satisfied from a previous output named 'x1', and the y argument
        # of execute() will be populated from the previous output named 'y1'
        #
        # The output (result of adding) will be mapped into a variable named
        # 'z1' which can then be refereed to and depended on by other tasks.
        Adder(name="add", provides='z1', rebind=['x1', 'y1']),
        # z2 = x2+y2 = 13
        Adder(name="add-2", provides='z2', rebind=['x2', 'y2']),
    ),
    # r = z1+z2 = 18
    Adder(name="sum-1", provides='r', rebind=['z1', 'z2']))


# The result here will be all results (from all tasks) which is stored in an
# in-memory storage location that backs this engine since it is not configured
# with persistence storage.
result = taskflow.engines.run(flow, engine='parallel')
print(result)
