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

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from taskflow import engines
from taskflow.patterns import graph_flow as gf
from taskflow import task


class DummyTask(task.Task):
    def execute(self):
        print("Running %s" % self.name)


def allow(history):
    print(history)
    return False


# Declare our work to be done...
r = gf.Flow("root")
r_a = DummyTask('r-a')
r_b = DummyTask('r-b')
r.add(r_a, r_b)
r.link(r_a, r_b, decider=allow)

# Setup and run the engine layer.
e = engines.load(r)
e.compile()
e.prepare()
e.run()


print("---------")
print("After run")
print("---------")
backend = e.storage.backend
entries = [os.path.join(backend.memory.root_path, child)
           for child in backend.memory.ls(backend.memory.root_path)]
while entries:
    path = entries.pop()
    value = backend.memory[path]
    if value:
        print("%s -> %s" % (path, value))
    else:
        print("%s" % (path))
    entries.extend(os.path.join(path, child)
                   for child in backend.memory.ls(path))
