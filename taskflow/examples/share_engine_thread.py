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
import random
import sys
import time

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

import futurist

from taskflow import engines
from taskflow.patterns import unordered_flow as uf
from taskflow import task
from taskflow.utils import threading_utils as tu

# INTRO: in this example we create 2 dummy flow(s) with a 2 dummy task(s), and
# run it using a shared thread pool executor to show how a single executor can
# be used with more than one engine (sharing the execution thread pool between
# them); this allows for saving resources and reusing threads in situations
# where this is benefical.


class DelayedTask(task.Task):
    def __init__(self, name):
        super(DelayedTask, self).__init__(name=name)
        self._wait_for = random.random()

    def execute(self):
        print("Running '%s' in thread '%s'" % (self.name, tu.get_ident()))
        time.sleep(self._wait_for)


f1 = uf.Flow("f1")
f1.add(DelayedTask("f1-1"))
f1.add(DelayedTask("f1-2"))

f2 = uf.Flow("f2")
f2.add(DelayedTask("f2-1"))
f2.add(DelayedTask("f2-2"))

# Run them all using the same futures (thread-pool based) executor...
with futurist.ThreadPoolExecutor() as ex:
    e1 = engines.load(f1, engine='parallel', executor=ex)
    e2 = engines.load(f2, engine='parallel', executor=ex)
    iters = [e1.run_iter(), e2.run_iter()]
    # Iterate over a copy (so we can remove from the source list).
    cloned_iters = list(iters)
    while iters:
        # Run a single 'step' of each iterator, forcing each engine to perform
        # some work, then yield, and repeat until each iterator is consumed
        # and there is no more engine work to be done.
        for it in cloned_iters:
            try:
                next(it)
            except StopIteration:
                try:
                    iters.remove(it)
                except ValueError:
                    pass
