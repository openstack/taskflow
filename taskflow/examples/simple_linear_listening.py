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
from taskflow import task


def call_jim(context):
    print("Calling jim.")
    print("Context = %s" % (context))


def call_joe(context):
    print("Calling joe.")
    print("Context = %s" % (context))


def flow_watch(state, details):
    print('Flow => %s' % state)


def task_watch(state, details):
    print('Task %s => %s' % (details.get('task_name'), state))


flow = lf.Flow("Call-them")
flow.add(task.FunctorTask(execute=call_jim))
flow.add(task.FunctorTask(execute=call_joe))

engine = taskflow.engines.load(flow, store={
    'context': {
        "joe_number": 444,
        "jim_number": 555,
    }
})

engine.notifier.register('*', flow_watch)
engine.task_notifier.register('*', task_watch)
engine.run()
