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

import json
import logging
import os
import sys
import tempfile

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from taskflow import engines
from taskflow.engines.worker_based import worker
from taskflow.patterns import linear_flow as lf
from taskflow.tests import utils
from taskflow.utils import threading_utils

import example_utils  # noqa

# INTRO: This example walks through a miniature workflow which shows how to
# start up a number of workers (these workers will process task execution and
# reversion requests using any provided input data) and then use an engine
# that creates a set of *capable* tasks and flows (the engine can not create
# tasks that the workers are not able to run, this will end in failure) that
# those workers will run and then executes that workflow seamlessly using the
# workers to perform the actual execution.
#
# NOTE(harlowja): this example simulates the expected larger number of workers
# by using a set of threads (which in this example simulate the remote workers
# that would typically be running on other external machines).

# A filesystem can also be used as the queue transport (useful as simple
# transport type that does not involve setting up a larger mq system). If this
# is false then the memory transport is used instead, both work in standalone
# setups.
USE_FILESYSTEM = False
BASE_SHARED_CONF = {
    'exchange': 'taskflow',
}

# Until https://github.com/celery/kombu/issues/398 is resolved it is not
# recommended to run many worker threads in this example due to the types
# of errors mentioned in that issue.
MEMORY_WORKERS = 2
FILE_WORKERS = 1
WORKER_CONF = {
    # These are the tasks the worker can execute, they *must* be importable,
    # typically this list is used to restrict what workers may execute to
    # a smaller set of *allowed* tasks that are known to be safe (one would
    # not want to allow all python code to be executed).
    'tasks': [
        'taskflow.tests.utils:TaskOneArgOneReturn',
        'taskflow.tests.utils:TaskMultiArgOneReturn'
    ],
}


def run(engine_options):
    flow = lf.Flow('simple-linear').add(
        utils.TaskOneArgOneReturn(provides='result1'),
        utils.TaskMultiArgOneReturn(provides='result2')
    )
    eng = engines.load(flow,
                       store=dict(x=111, y=222, z=333),
                       engine='worker-based', **engine_options)
    eng.run()
    return eng.storage.fetch_all()


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)

    # Setup our transport configuration and merge it into the worker and
    # engine configuration so that both of those use it correctly.
    shared_conf = dict(BASE_SHARED_CONF)

    tmp_path = None
    if USE_FILESYSTEM:
        worker_count = FILE_WORKERS
        tmp_path = tempfile.mkdtemp(prefix='wbe-example-')
        shared_conf.update({
            'transport': 'filesystem',
            'transport_options': {
                'data_folder_in': tmp_path,
                'data_folder_out': tmp_path,
                'polling_interval': 0.1,
            },
        })
    else:
        worker_count = MEMORY_WORKERS
        shared_conf.update({
            'transport': 'memory',
            'transport_options': {
                'polling_interval': 0.1,
            },
        })
    worker_conf = dict(WORKER_CONF)
    worker_conf.update(shared_conf)
    engine_options = dict(shared_conf)
    workers = []
    worker_topics = []

    try:
        # Create a set of workers to simulate actual remote workers.
        print('Running %s workers.' % (worker_count))
        for i in range(0, worker_count):
            worker_conf['topic'] = 'worker-%s' % (i + 1)
            worker_topics.append(worker_conf['topic'])
            w = worker.Worker(**worker_conf)
            runner = threading_utils.daemon_thread(w.run)
            runner.start()
            w.wait()
            workers.append((runner, w.stop))

        # Now use those workers to do something.
        print('Executing some work.')
        engine_options['topics'] = worker_topics
        result = run(engine_options)
        print('Execution finished.')
        # This is done so that the test examples can work correctly
        # even when the keys change order (which will happen in various
        # python versions).
        print("Result = %s" % json.dumps(result, sort_keys=True))
    finally:
        # And cleanup.
        print('Stopping workers.')
        while workers:
            r, stopper = workers.pop()
            stopper()
            r.join()
        if tmp_path:
            example_utils.rm_path(tmp_path)
