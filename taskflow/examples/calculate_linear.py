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


# INTRO: In this example a linear flow is used to group four tasks to calculate
# a value. A single added task is used twice, showing how this can be done
# and the twice added task takes in different bound values. In the first case
# it uses default parameters ('x' and 'y') and in the second case arguments
# are bound with ('z', 'd') keys from the engines internal storage mechanism.
#
# A multiplier task uses a binding that another task also provides, but this
# example explicitly shows that 'z' parameter is bound with 'a' key
# This shows that if a task depends on a key named the same as a key provided
# from another task the name can be remapped to take the desired key from a
# different origin.


# This task provides some values from as a result of execution, this can be
# useful when you want to provide values from a static set to other tasks that
# depend on those values existing before those tasks can run.
#
# NOTE(harlowja): this usage is *depreciated* in favor of a simpler mechanism
# that just provides those values on engine running by prepopulating the
# storage backend before your tasks are ran (which accomplishes a similar goal
# in a more uniform manner).
class Provider(task.Task):

    def __init__(self, name, *args, **kwargs):
        super(Provider, self).__init__(name=name, **kwargs)
        self._provide = args

    def execute(self):
        return self._provide


# This task adds two input variables and returns the result.
#
# Note that since this task does not have a revert() function (since addition
# is a stateless operation) there are no side-effects that this function needs
# to undo if some later operation fails.
class Adder(task.Task):
    def execute(self, x, y):
        return x + y


# This task multiplies an input variable by a multiplier and returns the
# result.
#
# Note that since this task does not have a revert() function (since
# multiplication is a stateless operation) and there are no side-effects that
# this function needs to undo if some later operation fails.
class Multiplier(task.Task):
    def __init__(self, name, multiplier, provides=None, rebind=None):
        super(Multiplier, self).__init__(name=name, provides=provides,
                                         rebind=rebind)
        self._multiplier = multiplier

    def execute(self, z):
        return z * self._multiplier


# Note here that the ordering is established so that the correct sequences
# of operations occurs where the adding and multiplying is done according
# to the expected and typical mathematical model. A graph flow could also be
# used here to automatically infer & ensure the correct ordering.
flow = lf.Flow('root').add(
    # Provide the initial values for other tasks to depend on.
    #
    # x = 2, y = 3, d = 5
    Provider("provide-adder", 2, 3, 5, provides=('x', 'y', 'd')),
    # z = x+y = 5
    Adder("add-1", provides='z'),
    # a = z+d = 10
    Adder("add-2", provides='a', rebind=['z', 'd']),
    # Calculate 'r = a*3 = 30'
    #
    # Note here that the 'z' argument of the execute() function will not be
    # bound to the 'z' variable provided from the above 'provider' object but
    # instead the 'z' argument will be taken from the 'a' variable provided
    # by the second add-2 listed above.
    Multiplier("multi", 3, provides='r', rebind={'z': 'a'})
)

# The result here will be all results (from all tasks) which is stored in an
# in-memory storage location that backs this engine since it is not configured
# with persistence storage.
results = taskflow.engines.run(flow)
print(results)
