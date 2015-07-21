# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

from oslo_utils import uuidutils

import taskflow.engines
from taskflow.patterns import linear_flow as lf
from taskflow.persistence import models
from taskflow import task

import example_utils as eu  # noqa

# INTRO: In this example linear_flow is used to group three tasks, one which
# will suspend the future work the engine may do. This suspend engine is then
# discarded and the workflow is reloaded from the persisted data and then the
# workflow is resumed from where it was suspended. This allows you to see how
# to start an engine, have a task stop the engine from doing future work (if
# a multi-threaded engine is being used, then the currently active work is not
# preempted) and then resume the work later.
#
# Usage:
#
#   With a filesystem directory as backend
#
#     python taskflow/examples/resume_from_backend.py
#
#   With ZooKeeper as backend
#
#     python taskflow/examples/resume_from_backend.py \
#       zookeeper://127.0.0.1:2181/taskflow/resume_from_backend/


# UTILITY FUNCTIONS #########################################


def print_task_states(flowdetail, msg):
    eu.print_wrapped(msg)
    print("Flow '%s' state: %s" % (flowdetail.name, flowdetail.state))
    # Sort by these so that our test validation doesn't get confused by the
    # order in which the items in the flow detail can be in.
    items = sorted((td.name, td.version, td.state, td.results)
                   for td in flowdetail)
    for item in items:
        print(" %s==%s: %s, result=%s" % item)


def find_flow_detail(backend, lb_id, fd_id):
    conn = backend.get_connection()
    lb = conn.get_logbook(lb_id)
    return lb.find(fd_id)


# CREATE FLOW ###############################################


class InterruptTask(task.Task):
    def execute(self):
        # DO NOT TRY THIS AT HOME
        engine.suspend()


class TestTask(task.Task):
    def execute(self):
        print('executing %s' % self)
        return 'ok'


def flow_factory():
    return lf.Flow('resume from backend example').add(
        TestTask(name='first'),
        InterruptTask(name='boom'),
        TestTask(name='second'))


# INITIALIZE PERSISTENCE ####################################

with eu.get_backend() as backend:

    # Create a place where the persistence information will be stored.
    book = models.LogBook("example")
    flow_detail = models.FlowDetail("resume from backend example",
                                    uuid=uuidutils.generate_uuid())
    book.add(flow_detail)
    with contextlib.closing(backend.get_connection()) as conn:
        conn.save_logbook(book)

    # CREATE AND RUN THE FLOW: FIRST ATTEMPT ####################

    flow = flow_factory()
    engine = taskflow.engines.load(flow, flow_detail=flow_detail,
                                   book=book, backend=backend)

    print_task_states(flow_detail, "At the beginning, there is no state")
    eu.print_wrapped("Running")
    engine.run()
    print_task_states(flow_detail, "After running")

    # RE-CREATE, RESUME, RUN ####################################

    eu.print_wrapped("Resuming and running again")

    # NOTE(harlowja): reload the flow detail from backend, this will allow us
    # to resume the flow from its suspended state, but first we need to search
    # for the right flow details in the correct logbook where things are
    # stored.
    #
    # We could avoid re-loading the engine and just do engine.run() again, but
    # this example shows how another process may unsuspend a given flow and
    # start it again for situations where this is useful to-do (say the process
    # running the above flow crashes).
    flow2 = flow_factory()
    flow_detail_2 = find_flow_detail(backend, book.uuid, flow_detail.uuid)
    engine2 = taskflow.engines.load(flow2,
                                    flow_detail=flow_detail_2,
                                    backend=backend, book=book)
    engine2.run()
    print_task_states(flow_detail_2, "At the end")
