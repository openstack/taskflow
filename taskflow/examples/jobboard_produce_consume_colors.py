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

import collections
import contextlib
import logging
import os
import random
import sys
import threading
import time

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

import six
from six.moves import range as compat_range
from zake import fake_client

from taskflow import exceptions as excp
from taskflow.jobs import backends
from taskflow.utils import threading_utils

# In this example we show how a jobboard can be used to post work for other
# entities to work on. This example creates a set of jobs using one producer
# thread (typically this would be split across many machines) and then having
# other worker threads with their own jobboards select work using a given
# filters [red/blue] and then perform that work (and consuming or abandoning
# the job after it has been completed or failed).

# Things to note:
# - No persistence layer is used (or logbook), just the job details are used
#   to determine if a job should be selected by a worker or not.
# - This example runs in a single process (this is expected to be atypical
#   but this example shows that it can be done if needed, for testing...)
# - The iterjobs(), claim(), consume()/abandon() worker workflow.
# - The post() producer workflow.

SHARED_CONF = {
    'path': "/taskflow/jobs",
    'board': 'zookeeper',
}

# How many workers and producers of work will be created (as threads).
PRODUCERS = 3
WORKERS = 5

# How many units of work each producer will create.
PRODUCER_UNITS = 10

# How many units of work are expected to be produced (used so workers can
# know when to stop running and shutdown, typically this would not be a
# a value but we have to limit this example's execution time to be less than
# infinity).
EXPECTED_UNITS = PRODUCER_UNITS * PRODUCERS

# Delay between producing/consuming more work.
WORKER_DELAY, PRODUCER_DELAY = (0.5, 0.5)

# To ensure threads don't trample other threads output.
STDOUT_LOCK = threading.Lock()


def dispatch_work(job):
    # This is where the jobs contained work *would* be done
    time.sleep(1.0)


def safe_print(name, message, prefix=""):
    with STDOUT_LOCK:
        if prefix:
            print("%s %s: %s" % (prefix, name, message))
        else:
            print("%s: %s" % (name, message))


def worker(ident, client, consumed):
    # Create a personal board (using the same client so that it works in
    # the same process) and start looking for jobs on the board that we want
    # to perform.
    name = "W-%s" % (ident)
    safe_print(name, "started")
    claimed_jobs = 0
    consumed_jobs = 0
    abandoned_jobs = 0
    with backends.backend(name, SHARED_CONF.copy(), client=client) as board:
        while len(consumed) != EXPECTED_UNITS:
            favorite_color = random.choice(['blue', 'red'])
            for job in board.iterjobs(ensure_fresh=True, only_unclaimed=True):
                # See if we should even bother with it...
                if job.details.get('color') != favorite_color:
                    continue
                safe_print(name, "'%s' [attempting claim]" % (job))
                try:
                    board.claim(job, name)
                    claimed_jobs += 1
                    safe_print(name, "'%s' [claimed]" % (job))
                except (excp.NotFound, excp.UnclaimableJob):
                    safe_print(name, "'%s' [claim unsuccessful]" % (job))
                else:
                    try:
                        dispatch_work(job)
                        board.consume(job, name)
                        safe_print(name, "'%s' [consumed]" % (job))
                        consumed_jobs += 1
                        consumed.append(job)
                    except Exception:
                        board.abandon(job, name)
                        abandoned_jobs += 1
                        safe_print(name, "'%s' [abandoned]" % (job))
            time.sleep(WORKER_DELAY)
    safe_print(name,
               "finished (claimed %s jobs, consumed %s jobs,"
               " abandoned %s jobs)" % (claimed_jobs, consumed_jobs,
                                        abandoned_jobs), prefix=">>>")


def producer(ident, client):
    # Create a personal board (using the same client so that it works in
    # the same process) and start posting jobs on the board that we want
    # some entity to perform.
    name = "P-%s" % (ident)
    safe_print(name, "started")
    with backends.backend(name, SHARED_CONF.copy(), client=client) as board:
        for i in compat_range(0, PRODUCER_UNITS):
            job_name = "%s-%s" % (name, i)
            details = {
                'color': random.choice(['red', 'blue']),
            }
            job = board.post(job_name, book=None, details=details)
            safe_print(name, "'%s' [posted]" % (job))
            time.sleep(PRODUCER_DELAY)
    safe_print(name, "finished", prefix=">>>")


def main():
    if six.PY3:
        # TODO(harlowja): Hack to make eventlet work right, remove when the
        # following is fixed: https://github.com/eventlet/eventlet/issues/230
        from taskflow.utils import eventlet_utils as _eu  # noqa
        try:
            import eventlet as _eventlet  # noqa
        except ImportError:
            pass
    with contextlib.closing(fake_client.FakeClient()) as c:
        created = []
        for i in compat_range(0, PRODUCERS):
            p = threading_utils.daemon_thread(producer, i + 1, c)
            created.append(p)
            p.start()
        consumed = collections.deque()
        for i in compat_range(0, WORKERS):
            w = threading_utils.daemon_thread(worker, i + 1, c, consumed)
            created.append(w)
            w.start()
        while created:
            t = created.pop()
            t.join()
        # At the end there should be nothing leftover, let's verify that.
        board = backends.fetch('verifier', SHARED_CONF.copy(), client=c)
        board.connect()
        with contextlib.closing(board):
            if board.job_count != 0 or len(consumed) != EXPECTED_UNITS:
                return 1
            return 0


if __name__ == "__main__":
    sys.exit(main())
