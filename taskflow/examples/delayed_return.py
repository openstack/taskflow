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

from concurrent import futures

logging.basicConfig(level=logging.ERROR)

self_dir = os.path.abspath(os.path.dirname(__file__))
top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)
sys.path.insert(0, self_dir)

# INTRO: in this example linear_flow we will attach a listener to an engine
# and delay the return from a function until after the result of a task has
# occurred in that engine. The engine will continue running (in the background)
# while the function will have returned.

import taskflow.engines
from taskflow.listeners import base
from taskflow.patterns import linear_flow as lf
from taskflow import states
from taskflow import task
from taskflow.types import notifier


class PokeFutureListener(base.Listener):
    def __init__(self, engine, future, task_name):
        super(PokeFutureListener, self).__init__(
            engine,
            task_listen_for=(notifier.Notifier.ANY,),
            flow_listen_for=[])
        self._future = future
        self._task_name = task_name

    def _task_receiver(self, state, details):
        if state in (states.SUCCESS, states.FAILURE):
            if details.get('task_name') == self._task_name:
                if state == states.SUCCESS:
                    self._future.set_result(details['result'])
                else:
                    failure = details['result']
                    self._future.set_exception(failure.exception)


class Hi(task.Task):
    def execute(self):
        # raise IOError("I broken")
        return 'hi'


class Bye(task.Task):
    def execute(self):
        return 'bye'


def return_from_flow(pool):
    wf = lf.Flow("root").add(Hi("hi"), Bye("bye"))
    eng = taskflow.engines.load(wf, engine='serial')
    f = futures.Future()
    watcher = PokeFutureListener(eng, f, 'hi')
    watcher.register()
    pool.submit(eng.run)
    return (eng, f.result())


with futures.ThreadPoolExecutor(1) as pool:
    engine, hi_result = return_from_flow(pool)
    print(hi_result)

print(engine.storage.get_flow_state())
