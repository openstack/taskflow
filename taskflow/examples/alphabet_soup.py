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

import fractions
import functools
import logging
import os
import string
import sys
import time

logging.basicConfig(level=logging.ERROR)

self_dir = os.path.abspath(os.path.dirname(__file__))
top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)
sys.path.insert(0, self_dir)

from taskflow import engines
from taskflow import exceptions
from taskflow.patterns import linear_flow
from taskflow import task


# In this example we show how a simple linear set of tasks can be executed
# using local processes (and not threads or remote workers) with minimal (if
# any) modification to those tasks to make them safe to run in this mode.
#
# This is useful since it allows further scaling up your workflows when thread
# execution starts to become a bottleneck (which it can start to be due to the
# GIL in python). It also offers a intermediary scalable runner that can be
# used when the scale and/or setup of remote workers is not desirable.


def progress_printer(task, event_type, details):
    # This callback, attached to each task will be called in the local
    # process (not the child processes)...
    progress = details.pop('progress')
    progress = int(progress * 100.0)
    print("Task '%s' reached %d%% completion" % (task.name, progress))


class AlphabetTask(task.Task):
    # Second delay between each progress part.
    _DELAY = 0.1

    # This task will run in X main stages (each with a different progress
    # report that will be delivered back to the running process...). The
    # initial 0% and 100% are triggered automatically by the engine when
    # a task is started and finished (so that's why those are not emitted
    # here).
    _PROGRESS_PARTS = [fractions.Fraction("%s/5" % x) for x in range(1, 5)]

    def execute(self):
        for p in self._PROGRESS_PARTS:
            self.update_progress(p)
            time.sleep(self._DELAY)


print("Constructing...")
soup = linear_flow.Flow("alphabet-soup")
for letter in string.ascii_lowercase:
    abc = AlphabetTask(letter)
    abc.notifier.register(task.EVENT_UPDATE_PROGRESS,
                          functools.partial(progress_printer, abc))
    soup.add(abc)
try:
    print("Loading...")
    e = engines.load(soup, engine='parallel', executor='processes')
    print("Compiling...")
    e.compile()
    print("Preparing...")
    e.prepare()
    print("Running...")
    e.run()
    print("Done: %s" % e.statistics)
except exceptions.NotImplementedError as e:
    print(e)
