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
from taskflow.types import notifier

ANY = notifier.Notifier.ANY

# INTRO: In this example we create two tasks (this time as functions instead
# of task subclasses as in the simple_linear.py example), each of which ~calls~
# a given ~phone~ number (provided as a function input) in a linear fashion
# (one after the other).
#
# For a workflow which is serial this shows an extremely simple way
# of structuring your tasks (the code that does the work) into a linear
# sequence (the flow) and then passing the work off to an engine, with some
# initial data to be ran in a reliable manner.
#
# This example shows a basic usage of the taskflow structures without involving
# the complexity of persistence. Using the structures that taskflow provides
# via tasks and flows makes it possible for you to easily at a later time
# hook in a persistence layer (and then gain the functionality that offers)
# when you decide the complexity of adding that layer in is 'worth it' for your
# applications usage pattern (which some applications may not need).
#
# It **also** adds on to the simple_linear.py example by adding a set of
# callback functions which the engine will call when a flow state transition
# or task state transition occurs. These types of functions are useful for
# updating task or flow progress, or for debugging, sending notifications to
# external systems, or for other yet unknown future usage that you may create!


def call_jim(context):
    print("Calling jim.")
    print("Context = %s" % (sorted(context.items(), key=lambda x: x[0])))


def call_joe(context):
    print("Calling joe.")
    print("Context = %s" % (sorted(context.items(), key=lambda x: x[0])))


def flow_watch(state, details):
    print('Flow => %s' % state)


def task_watch(state, details):
    print('Task %s => %s' % (details.get('task_name'), state))


# Wrap your functions into a task type that knows how to treat your functions
# as tasks. There was previous work done to just allow a function to be
# directly passed, but in python 3.0 there is no easy way to capture an
# instance method, so this wrapping approach was decided upon instead which
# can attach to instance methods (if that's desired).
flow = lf.Flow("Call-them")
flow.add(task.FunctorTask(execute=call_jim))
flow.add(task.FunctorTask(execute=call_joe))

# Now load (but do not run) the flow using the provided initial data.
engine = taskflow.engines.load(flow, store={
    'context': {
        "joe_number": 444,
        "jim_number": 555,
    }
})

# This is where we attach our callback functions to the 2 different
# notification objects that an engine exposes. The usage of a ANY (kleene star)
# here means that we want to be notified on all state changes, if you want to
# restrict to a specific state change, just register that instead.
engine.notifier.register(ANY, flow_watch)
engine.atom_notifier.register(ANY, task_watch)

# And now run!
engine.run()
