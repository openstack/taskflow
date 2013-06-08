# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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

from datetime import datetime

import functools
import threading
import unittest2

from taskflow import decorators
from taskflow import exceptions as exc
from taskflow import job
from taskflow import states

from taskflow.backends import memory
from taskflow.patterns import linear_flow as lw
from taskflow.tests import utils


class MemoryBackendTest(unittest2.TestCase):
    def _create_memory_impl(self, cons=1):
        worker_group = []
        poisons = []
        for _i in range(0, cons):
            poisons.append(threading.Event())

        def killer():
            for p in poisons:
                p.set()
            for t in worker_group:
                t.join()

        job_claimer = memory.MemoryClaimer()
        book_catalog = memory.MemoryCatalog()
        job_board = memory.MemoryJobBoard()

        def runner(my_name, poison):
            while not poison.isSet():
                my_jobs = []
                job_board.await(0.05)
                job_search_from = None
                for j in job_board.posted_after(job_search_from):
                    if j.owner is not None:
                        continue
                    try:
                        j.claim(my_name)
                        my_jobs.append(j)
                    except exc.UnclaimableJobException:
                        pass
                if not my_jobs:
                    # No jobs were claimed, lets not search the past again
                    # then, since *likely* those jobs will remain claimed...
                    job_search_from = datetime.utcnow()
                if my_jobs and poison.isSet():
                    # Oh crap, we need to unclaim and repost the jobs.
                    for j in my_jobs:
                        j.unclaim()
                        job_board.repost(j)
                else:
                    # Set all jobs to pending before starting them
                    for j in my_jobs:
                        j.state = states.PENDING
                    for j in my_jobs:
                        # Create some dummy flow for the job
                        wf = lw.Flow('dummy')
                        for _i in range(0, 5):
                            wf.add(utils.null_functor)
                        j.associate(wf)
                        j.state = states.RUNNING
                        wf.run(j.context)
                        j.state = states.SUCCESS
                        j.erase()

        for i in range(0, cons):
            t_name = "Thread-%s" % (i + 1)
            t_runner = functools.partial(runner, t_name, poisons[i])
            c = threading.Thread(name=t_name, target=t_runner)
            c.daemon = True
            worker_group.append(c)
            c.start()

        return (job_board, job_claimer, book_catalog, killer)

    def test_job_working(self):
        killer = None
        job_board = None
        book_catalog = None
        try:
            (job_board, job_claimer,
             book_catalog, killer) = self._create_memory_impl()
            j = job.Job("blah", {}, book_catalog, job_claimer)
            job_board.post(j)
            j.await()
            self.assertEquals(0, len(job_board.posted_after()))
        finally:
            if killer:
                killer()
            utils.close_all(book_catalog, job_board)

    def test_working_job_interrupted(self):
        job_claimer = memory.MemoryClaimer()
        book_catalog = memory.MemoryCatalog()

        j = job.Job("the-int-job", {}, book_catalog, job_claimer)
        self.assertEquals(states.UNCLAIMED, j.state)
        j.claim("me")
        self.assertEquals(states.CLAIMED, j.state)
        self.assertEquals('me', j.owner)

        wf = lw.Flow("the-int-action")
        j.associate(wf)
        self.assertEquals(states.PENDING, wf.state)

        call_log = []

        @decorators.task
        def do_1(context, *args, **kwargs):
            call_log.append(1)

        @decorators.task
        def do_2(context, *args, **kwargs):
            call_log.append(2)

        @decorators.task
        def do_interrupt(context, *args, **kwargs):
            wf.interrupt()

        task_1 = do_1
        task_1_5 = do_interrupt
        task_2 = do_2

        wf.add(task_1)
        wf.add(task_1_5)  # Interrupt it after task_1 finishes
        wf.add(task_2)

        wf.run(j.context)

        self.assertEquals(1, len(j.logbook))
        self.assertEquals(2, len(j.logbook["the-int-action"]))
        self.assertEquals(1, len(call_log))

        wf.reset()
        j.associate(wf)
        self.assertEquals(states.PENDING, wf.state)
        wf.run(j.context)

        self.assertEquals(1, len(j.logbook))
        self.assertEquals(3, len(j.logbook["the-int-action"]))
        self.assertEquals(2, len(call_log))
        self.assertEquals(states.SUCCESS, wf.state)

    def test_working_job(self):
        job_claimer = memory.MemoryClaimer()
        book_catalog = memory.MemoryCatalog()

        j = job.Job("the-line-job", {}, book_catalog, job_claimer)
        self.assertEquals(states.UNCLAIMED, j.state)
        j.claim("me")
        self.assertEquals(states.CLAIMED, j.state)
        self.assertEquals('me', j.owner)

        wf = lw.Flow('the-line-action')
        self.assertEquals(states.PENDING, wf.state)
        j.associate(wf)

        call_log = []

        @decorators.task
        def do_1(context, *args, **kwargs):
            call_log.append(1)

        @decorators.task
        def do_2(context, *args, **kwargs):
            call_log.append(2)

        wf.add(do_1)
        wf.add(do_2)
        wf.run(j.context)

        self.assertEquals(1, len(j.logbook))
        self.assertEquals(2, len(j.logbook["the-line-action"]))
        self.assertEquals(2, len(call_log))
        self.assertEquals(states.SUCCESS, wf.state)

    def test_post_receive_job(self):
        job_claimer = memory.MemoryClaimer()
        book_catalog = memory.MemoryCatalog()
        j = job.Job("test", {}, book_catalog, job_claimer)

        # Hook up some simulated workers to said job-board.
        job_board = memory.MemoryJobBoard()
        receiver_awake = threading.Event()
        work_items = []

        def post_job():
            job_board.post(j)

        def work_on_job(j):
            owner = 'me'
            j.claim(owner)

        def receive_job():
            start = datetime.utcnow()
            receiver_awake.set()
            new_jobs = []
            while not new_jobs:
                job_board.await(0.5)
                new_jobs = job_board.posted_after(start)
            work_items.extend(new_jobs)
            for j in work_items:
                work_on_job(j)

        poster = threading.Thread(target=post_job)
        receiver = threading.Thread(target=receive_job)
        receiver.start()
        while not receiver_awake.isSet():
            receiver_awake.wait()
        poster.start()

        for t in [poster, receiver]:
            t.join()

        self.assertEquals(1, len(work_items))
        self.assertEquals(j.owner, 'me')
