# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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
from taskflow.persistence import backends
from taskflow import task
from taskflow.utils import persistence_utils as pu

# INTRO: in this example we create a dummy flow with a dummy task, and run
# it using a in-memory backend and pre/post run we dump out the contents
# of the in-memory backends tree structure (which can be quite useful to
# look at for debugging or other analysis).


class PrintTask(task.Task):
    def execute(self):
        print("Running '%s'" % self.name)


backend = backends.fetch({
    'connection': 'memory://',
})
book, flow_detail = pu.temporary_flow_detail(backend=backend)

# Make a little flow and run it...
f = lf.Flow('root')
for alpha in ['a', 'b', 'c']:
    f.add(PrintTask(alpha))

e = engines.load(f, flow_detail=flow_detail,
                 book=book, backend=backend)
e.compile()
e.prepare()

print("----------")
print("Before run")
print("----------")
print(backend.memory.pformat())
print("----------")

e.run()

print("---------")
print("After run")
print("---------")
for path in backend.memory.ls_r(backend.memory.root_path, absolute=True):
    value = backend.memory[path]
    if value:
        print("%s -> %s" % (path, value))
    else:
        print("%s" % (path))
