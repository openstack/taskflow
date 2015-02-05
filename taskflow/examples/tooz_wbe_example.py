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

from __future__ import print_function

import logging
import os
import sys

self_dir = os.path.abspath(os.path.dirname(__file__))
top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)
sys.path.insert(0, self_dir)

logging.basicConfig(level=logging.ERROR)

from oslo_utils import uuidutils

from taskflow import engines
from taskflow.engines.worker_based import executor
from taskflow.engines.worker_based import types as wt
from taskflow.engines.worker_based import worker
from taskflow.patterns import linear_flow as lf
from taskflow import task
from taskflow.utils import persistence_utils as pu
from taskflow.utils import threading_utils as tu


# Some simple utility functions.

def banner_writer(banner):
    for line in banner.splitlines():
        print(" %s" % line)


# Some dummy tasks that would typically do something more complex...

class TwoTask(task.Task):
    default_provides = 'two'

    def execute(self):
        return 2


class OneTask(task.Task):
    default_provides = 'one'

    def execute(self):
        return 1


# Settings that the tooz worker + finder need to learn of each other...
#
# See: http://docs.openstack.org/developer/tooz/
coordinator_groups = ['x', 'y', 'z']
coordinator_groups = frozenset(x.encode('ascii') for x in coordinator_groups)
coordinator_url = "zake://localhost"

# Settings used when interacting with kombu...
#
# See: http://kombu.readthedocs.org/
exchange = 'default'
transport = 'memory'

# Create & start our worker that will actually run tasks..
#
# It will join 'coordinator_groups' groups and advertise its capabilities in
# those groups (so that engines may find it).
worker_id = uuidutils.generate_uuid()
advertiser_factory = wt.ToozWorkerAdvertiser.generate_factory({
    'coordinator': {
        'url': coordinator_url,
        'groups': coordinator_groups,
    },
})
w = worker.Worker(exchange, uuidutils.generate_uuid(), [OneTask, TwoTask],
                  transport=transport, advertiser_factory=advertiser_factory)
w_runner = tu.daemon_thread(target=w.run, banner_writer=banner_writer)

print("Booting up the worker...")
w_runner.start()
w.wait()

# Now make the engine that will use the previous workers to do some work...
finder_factory = wt.ToozWorkerFinder.generate_factory({
    'coordinator': {
        'url': coordinator_url,
        'groups': coordinator_groups,
    },
})

# Create a custom executor for the engine to use.
#
# TODO(harlowja): this will be fixed in a future version (so that instead
# of doing this an entrypoint can be requested instead).
executor_id = uuidutils.generate_uuid()
ex = executor.WorkerTaskExecutor(executor_id, exchange, finder_factory,
                                 transport=transport,
                                 # How long (in seconds) we will wait to find
                                 # a worker before timing out the request(s).
                                 transition_timeout=60)

# Now create some work and run it in a worker engine!
f = lf.Flow('dummy')
f.add(OneTask('1'), TwoTask('2'))

print("Running...")
eng = engines.load(f, pu.create_flow_detail(f), executor=ex, engine='workers')
for st in eng.run_iter():
    print(" -> %s" % st)
print('Done!!')
print("Results: %s" % eng.storage.fetch_all())
print("Shutting down the worker...")
w.stop()
w_runner.join()

print('Goodbye!!')
