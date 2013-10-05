# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

# This examples shows how LinearFlow and ParallelFlow can be used
# together to execute calculations in parallel and then use the
# result for the next task. Adder task is used for all calculations
# and arguments' bindings are used to set correct parameters to the task.


class Provider(task.Task):
    def __init__(self, name, *args, **kwargs):
        super(Provider, self).__init__(name=name, **kwargs)
        self._provide = args

    def execute(self):
        return self._provide


class Adder(task.Task):
    def execute(self, x, y):
        return x + y


flow = lf.Flow('root').add(
    # x1 = 2, y1 = 3, x2 = 5, x3 = 8
    Provider("provide-adder", 2, 3, 5, 8,
             provides=('x1', 'y1', 'x2', 'y2')),
    uf.Flow('adders').add(
        # z1 = x1+y1 = 5
        Adder(name="add", provides='z1', rebind=['x1', 'y1']),
        # z2 = x2+y2 = 13
        Adder(name="add-2", provides='z2', rebind=['x2', 'y2']),
    ),
    # r = z1+z2 = 18
    Adder(name="sum-1", provides='r', rebind=['z1', 'z2']))


result = taskflow.engines.run(flow, engine_conf='parallel')
print result
