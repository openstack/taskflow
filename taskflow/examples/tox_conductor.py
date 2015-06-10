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

import contextlib
import itertools
import logging
import os
import shutil
import socket
import sys
import tempfile
import threading
import time

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from oslo_utils import timeutils
from oslo_utils import uuidutils
import six
from zake import fake_client

from taskflow.conductors import backends as conductors
from taskflow import engines
from taskflow.jobs import backends as boards
from taskflow.patterns import linear_flow
from taskflow.persistence import backends as persistence
from taskflow.persistence import models
from taskflow import task
from taskflow.utils import threading_utils

# INTRO: This examples shows how a worker/producer can post desired work (jobs)
# to a jobboard and a conductor can consume that work (jobs) from that jobboard
# and execute those jobs in a reliable & async manner (for example, if the
# conductor were to crash then the job will be released back onto the jobboard
# and another conductor can attempt to finish it, from wherever that job last
# left off).
#
# In this example a in-memory jobboard (and in-memory storage) is created and
# used that simulates how this would be done at a larger scale (it is an
# example after all).

# Restrict how long this example runs for...
RUN_TIME = 5
REVIEW_CREATION_DELAY = 0.5
SCAN_DELAY = 0.1
NAME = "%s_%s" % (socket.getfqdn(), os.getpid())

# This won't really use zookeeper but will use a local version of it using
# the zake library that mimics an actual zookeeper cluster using threads and
# an in-memory data structure.
JOBBOARD_CONF = {
    'board': 'zookeeper://localhost?path=/taskflow/tox/jobs',
}


class RunReview(task.Task):
    # A dummy task that clones the review and runs tox...

    def _clone_review(self, review, temp_dir):
        print("Cloning review '%s' into %s" % (review['id'], temp_dir))

    def _run_tox(self, temp_dir):
        print("Running tox in %s" % temp_dir)

    def execute(self, review, temp_dir):
        self._clone_review(review, temp_dir)
        self._run_tox(temp_dir)


class MakeTempDir(task.Task):
    # A task that creates and destroys a temporary dir (on failure).
    #
    # It provides the location of the temporary dir for other tasks to use
    # as they see fit.

    default_provides = 'temp_dir'

    def execute(self):
        return tempfile.mkdtemp()

    def revert(self, *args, **kwargs):
        temp_dir = kwargs.get(task.REVERT_RESULT)
        if temp_dir:
            shutil.rmtree(temp_dir)


class CleanResources(task.Task):
    # A task that cleans up any workflow resources.

    def execute(self, temp_dir):
        print("Removing %s" % temp_dir)
        shutil.rmtree(temp_dir)


def review_iter():
    """Makes reviews (never-ending iterator/generator)."""
    review_id_gen = itertools.count(0)
    while True:
        review_id = six.next(review_id_gen)
        review = {
            'id': review_id,
        }
        yield review


# The reason this is at the module namespace level is important, since it must
# be accessible from a conductor dispatching an engine, if it was a lambda
# function for example, it would not be reimportable and the conductor would
# be unable to reference it when creating the workflow to run.
def create_review_workflow():
    """Factory method used to create a review workflow to run."""
    f = linear_flow.Flow("tester")
    f.add(
        MakeTempDir(name="maker"),
        RunReview(name="runner"),
        CleanResources(name="cleaner")
    )
    return f


def generate_reviewer(client, saver, name=NAME):
    """Creates a review producer thread with the given name prefix."""
    real_name = "%s_reviewer" % name
    no_more = threading.Event()
    jb = boards.fetch(real_name, JOBBOARD_CONF,
                      client=client, persistence=saver)

    def make_save_book(saver, review_id):
        # Record what we want to happen (sometime in the future).
        book = models.LogBook("book_%s" % review_id)
        detail = models.FlowDetail("flow_%s" % review_id,
                                   uuidutils.generate_uuid())
        book.add(detail)
        # Associate the factory method we want to be called (in the future)
        # with the book, so that the conductor will be able to call into
        # that factory to retrieve the workflow objects that represent the
        # work.
        #
        # These args and kwargs *can* be used to save any specific parameters
        # into the factory when it is being called to create the workflow
        # objects (typically used to tell a factory how to create a unique
        # workflow that represents this review).
        factory_args = ()
        factory_kwargs = {}
        engines.save_factory_details(detail, create_review_workflow,
                                     factory_args, factory_kwargs)
        with contextlib.closing(saver.get_connection()) as conn:
            conn.save_logbook(book)
            return book

    def run():
        """Periodically publishes 'fake' reviews to analyze."""
        jb.connect()
        review_generator = review_iter()
        with contextlib.closing(jb):
            while not no_more.is_set():
                review = six.next(review_generator)
                details = {
                    'store': {
                        'review': review,
                    },
                }
                job_name = "%s_%s" % (real_name, review['id'])
                print("Posting review '%s'" % review['id'])
                jb.post(job_name,
                        book=make_save_book(saver, review['id']),
                        details=details)
                time.sleep(REVIEW_CREATION_DELAY)

    # Return the unstarted thread, and a callback that can be used
    # shutdown that thread (to avoid running forever).
    return (threading_utils.daemon_thread(target=run), no_more.set)


def generate_conductor(client, saver, name=NAME):
    """Creates a conductor thread with the given name prefix."""
    real_name = "%s_conductor" % name
    jb = boards.fetch(name, JOBBOARD_CONF,
                      client=client, persistence=saver)
    conductor = conductors.fetch("blocking", real_name, jb,
                                 engine='parallel', wait_timeout=SCAN_DELAY)

    def run():
        jb.connect()
        with contextlib.closing(jb):
            conductor.run()

    # Return the unstarted thread, and a callback that can be used
    # shutdown that thread (to avoid running forever).
    return (threading_utils.daemon_thread(target=run), conductor.stop)


def main():
    # Need to share the same backend, so that data can be shared...
    persistence_conf = {
        'connection': 'memory',
    }
    saver = persistence.fetch(persistence_conf)
    with contextlib.closing(saver.get_connection()) as conn:
        # This ensures that the needed backend setup/data directories/schema
        # upgrades and so on... exist before they are attempted to be used...
        conn.upgrade()
    fc1 = fake_client.FakeClient()
    # Done like this to share the same client storage location so the correct
    # zookeeper features work across clients...
    fc2 = fake_client.FakeClient(storage=fc1.storage)
    entities = [
        generate_reviewer(fc1, saver),
        generate_conductor(fc2, saver),
    ]
    for t, stopper in entities:
        t.start()
    try:
        watch = timeutils.StopWatch(duration=RUN_TIME)
        watch.start()
        while not watch.expired():
            time.sleep(0.1)
    finally:
        for t, stopper in reversed(entities):
            stopper()
            t.join()


if __name__ == '__main__':
    main()
