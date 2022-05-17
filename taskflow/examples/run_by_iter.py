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
from taskflow.patterns import linear_flow as lf
from taskflow import task


# INTRO: This example shows how to run a set of engines at the same time, each
# running in different engines using a single thread of control to iterate over
# each engine (which causes that engine to advanced to its next state during
# each iteration).


class EchoTask(task.Task):
    def execute(self, value):
        print(value)
        return chr(ord(value) + 1)


def make_alphabet_flow(i):
    f = lf.Flow("alphabet_%s" % (i))
    start_value = 'A'
    end_value = 'Z'
    curr_value = start_value
    while ord(curr_value) <= ord(end_value):
        next_value = chr(ord(curr_value) + 1)
        if curr_value != end_value:
            f.add(EchoTask(name="echoer_%s" % curr_value,
                           rebind={'value': curr_value},
                           provides=next_value))
        else:
            f.add(EchoTask(name="echoer_%s" % curr_value,
                           rebind={'value': curr_value}))
        curr_value = next_value
    return f


# Adjust this number to change how many engines/flows run at once.
flow_count = 1
flows = []
for i in range(0, flow_count):
    f = make_alphabet_flow(i + 1)
    flows.append(make_alphabet_flow(i + 1))
engine_iters = []
for f in flows:
    e = engines.load(f)
    e.compile()
    e.storage.inject({'A': 'A'})
    e.prepare()
    engine_iters.append(e.run_iter())
while engine_iters:
    for it in list(engine_iters):
        try:
            print(next(it))
        except StopIteration:
            engine_iters.remove(it)
