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
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow import task


# INTRO: This examples shows how a graph_flow and linear_flow can be used
# together to execute non-dependent tasks by going through the steps required
# to build a simplistic car (an assembly line if you will). It also shows
# how raw functions can be wrapped into a task object instead of being forced
# to use the more heavy task base class. This is useful in scenarios where
# pre-existing code has functions that you easily want to plug-in to taskflow,
# without requiring a large amount of code changes.


def build_frame():
    return 'steel'


def build_engine():
    return 'honda'


def build_doors():
    return '2'


def build_wheels():
    return '4'


def install_engine(frame, engine):
    return True


def install_doors(frame, windows_installed, doors):
    return True


def install_windows(frame, doors):
    return True


def install_wheels(frame, engine, engine_installed, wheels):
    return True


def trash(**kwargs):
    print_wrapped("Throwing away pieces of car!")


def print_wrapped(text):
    print("-" * (len(text)))
    print(text)
    print("-" * (len(text)))


def startup(**kwargs):
    # If you want to see the rollback function being activated try uncommenting
    # the following line.
    #
    # raise ValueError("Car not verified")
    return True


def verify(spec, **kwargs):
    # If the car is not what we ordered throw away the car (trigger reversion).
    for key, value in kwargs.items():
        if spec[key] != value:
            raise Exception("Car doesn't match spec!")
    return True


# These two functions connect into the state transition notification emission
# points that the engine outputs, they can be used to log state transitions
# that are occuring, or they can be used to suspend the engine (or perform
# other useful activities).
def flow_watch(state, details):
    print('Flow => %s' % state)


def task_watch(state, details):
    print('Task %s => %s' % (details.get('task_name'), state))


flow = lf.Flow("make-auto").add(
    task.FunctorTask(startup, revert=trash, provides='ran'),
    gf.Flow("install-parts").add(
        task.FunctorTask(build_frame, provides='frame'),
        task.FunctorTask(build_engine, provides='engine'),
        task.FunctorTask(build_doors, provides='doors'),
        task.FunctorTask(build_wheels, provides='wheels'),
        # These *_installed outputs allow for other tasks to depend on certain
        # actions being performed (aka the components were installed), another
        # way to do this is to link() the tasks manually instead of creating
        # an 'artifical' data dependency that accomplishes the same goal the
        # manual linking would result in.
        task.FunctorTask(install_engine, provides='engine_installed'),
        task.FunctorTask(install_doors, provides='doors_installed'),
        task.FunctorTask(install_windows, provides='windows_installed'),
        task.FunctorTask(install_wheels, provides='wheels_installed')),
    task.FunctorTask(verify, requires=['frame',
                                       'engine',
                                       'doors',
                                       'wheels',
                                       'engine_installed',
                                       'doors_installed',
                                       'windows_installed',
                                       'wheels_installed']))

# This dictionary will be provided to the tasks as a specification for what
# the tasks should produce, in this example this specification will influence
# what those tasks do and what output they create. Different tasks depend on
# different information from this specification, all of which will be provided
# automatically by the engine.
spec = {
    "frame": 'steel',
    "engine": 'honda',
    "doors": '2',
    "wheels": '4',
    # These are used to compare the result product, a car without the pieces
    # installed is not a car after all.
    "engine_installed": True,
    "doors_installed": True,
    "windows_installed": True,
    "wheels_installed": True,
}


engine = taskflow.engines.load(flow, store={'spec': spec.copy()})

# This registers all (*) state transitions to trigger a call to the flow_watch
# function for flow state transitions, and registers the same all (*) state
# transitions for task state transitions.
engine.notifier.register('*', flow_watch)
engine.task_notifier.register('*', task_watch)

print_wrapped("Building a car")
engine.run()

# Alter the specification and ensure that the reverting logic gets triggered
# since the resultant car that will be built by the build_wheels function will
# build a car with 4 doors only (not 5), this will cause the verification
# task to mark the car that is produced as not matching the desired spec.
spec['doors'] = 5

engine = taskflow.engines.load(flow, store={'spec': spec.copy()})
engine.notifier.register('*', flow_watch)
engine.task_notifier.register('*', task_watch)

print_wrapped("Building a wrong car that doesn't match specification")
try:
    engine.run()
except Exception as e:
    print_wrapped("Flow failed: %s" % e)
