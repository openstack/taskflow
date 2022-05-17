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
import string
import sys
import time

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from taskflow import engines
from taskflow.engines.worker_based import worker
from taskflow.patterns import linear_flow as lf
from taskflow import task
from taskflow.types import notifier
from taskflow.utils import threading_utils

ANY = notifier.Notifier.ANY

# INTRO: These examples show how to use a remote worker's event notification
# attribute to proxy back task event notifications to the controlling process.
#
# In this case a simple set of events is triggered by a worker running a
# task (simulated to be remote by using a kombu memory transport and threads).
# Those events that the 'remote worker' produces will then be proxied back to
# the task that the engine is running 'remotely', and then they will be emitted
# back to the original callbacks that exist in the originating engine
# process/thread. This creates a one-way *notification* channel that can
# transparently be used in-process, outside-of-process using remote workers and
# so-on that allows tasks to signal to its controlling process some sort of
# action that has occurred that the task may need to tell others about (for
# example to trigger some type of response when the task reaches 50% done...).


def event_receiver(event_type, details):
    """This is the callback that (in this example) doesn't do much..."""
    print("Recieved event '%s'" % event_type)
    print("Details = %s" % details)


class EventReporter(task.Task):
    """This is the task that will be running 'remotely' (not really remote)."""

    EVENTS = tuple(string.ascii_uppercase)
    EVENT_DELAY = 0.1

    def execute(self):
        for i, e in enumerate(self.EVENTS):
            details = {
                'leftover': self.EVENTS[i:],
            }
            self.notifier.notify(e, details)
            time.sleep(self.EVENT_DELAY)


BASE_SHARED_CONF = {
    'exchange': 'taskflow',
    'transport': 'memory',
    'transport_options': {
        'polling_interval': 0.1,
    },
}

# Until https://github.com/celery/kombu/issues/398 is resolved it is not
# recommended to run many worker threads in this example due to the types
# of errors mentioned in that issue.
MEMORY_WORKERS = 1
WORKER_CONF = {
    'tasks': [
        # Used to locate which tasks we can run (we don't want to allow
        # arbitrary code/tasks to be ran by any worker since that would
        # open up a variety of vulnerabilities).
        '%s:EventReporter' % (__name__),
    ],
}


def run(engine_options):
    reporter = EventReporter()
    reporter.notifier.register(ANY, event_receiver)
    flow = lf.Flow('event-reporter').add(reporter)
    eng = engines.load(flow, engine='worker-based', **engine_options)
    eng.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)

    # Setup our transport configuration and merge it into the worker and
    # engine configuration so that both of those objects use it correctly.
    worker_conf = dict(WORKER_CONF)
    worker_conf.update(BASE_SHARED_CONF)
    engine_options = dict(BASE_SHARED_CONF)
    workers = []

    # These topics will be used to request worker information on; those
    # workers will respond with their capabilities which the executing engine
    # will use to match pending tasks to a matched worker, this will cause
    # the task to be sent for execution, and the engine will wait until it
    # is finished (a response is received) and then the engine will either
    # continue with other tasks, do some retry/failure resolution logic or
    # stop (and potentially re-raise the remote workers failure)...
    worker_topics = []

    try:
        # Create a set of worker threads to simulate actual remote workers...
        print('Running %s workers.' % (MEMORY_WORKERS))
        for i in range(0, MEMORY_WORKERS):
            # Give each one its own unique topic name so that they can
            # correctly communicate with the engine (they will all share the
            # same exchange).
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
    finally:
        # And cleanup.
        print('Stopping workers.')
        while workers:
            r, stopper = workers.pop()
            stopper()
            r.join()
