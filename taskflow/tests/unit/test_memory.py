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
import inspect
import threading
import time
import unittest

from taskflow import exceptions as exc
from taskflow import job
from taskflow import logbook
from taskflow import states
from taskflow import task

from taskflow.backends import memory
from taskflow.patterns import linear_workflow as lw


def null_functor(*args, **kwargs):
    return None


def gen_task_name(task, state):
    return "%s:%s" % (task.name, state)


class FunctorTask(task.Task):
    def __init__(self, apply_functor, revert_functor):
        super(FunctorTask, self).__init__("%s-%s" % (apply_functor.__name__,
                                                     revert_functor.__name__))
        self._apply_functor = apply_functor
        self._revert_functor = revert_functor

    def apply(self, context, *args, **kwargs):
        return self._apply_functor(context, *args, **kwargs)

    def revert(self, context, result, cause):
        return self._revert_functor(context, result, cause)


class MemoryBackendTest(unittest.TestCase):
    def testWorkJobLinearInterrupted(self):
        job_claimer = memory.MemoryClaimer()
        book_catalog = memory.MemoryCatalog()

        j = job.Job("the-big-action-job", {}, book_catalog, job_claimer)
        self.assertEquals(states.UNCLAIMED, j.state)
        j.claim("me")
        self.assertEquals(states.CLAIMED, j.state)
        self.assertEquals('me', j.owner)

        def wf_state_change_listener(context, wf, old_state):
            if wf.name in j.logbook:
                return
            j.logbook.add_workflow(wf.name)

        stop_after = []

        def task_state_change_listener(context, state, wf, task, result=None):
            metadata = None
            wf_details = j.logbook.fetch_workflow(wf.name)
            if state in (states.SUCCESS,):
                metadata = {
                    'result': result,
                }
                if task.name in stop_after:
                    # Oops, stopping...
                    wf.interrupt()
                    stop_after.remove(task.name)
            td_name = gen_task_name(task, state)
            if td_name not in wf_details:
                wf_details.add_task(logbook.TaskDetail(td_name, metadata))

        def task_result_fetcher(context, wf, task):
            wf_details = j.logbook.fetch_workflow(wf.name)
            td_name = gen_task_name(task, states.SUCCESS)
            if td_name in wf_details:
                task_details = wf_details.fetch_tasks(td_name)[0]
                return (True, task_details.metadata['result'])
            return (False, None)

        wf = lw.Workflow("the-big-action")
        self.assertEquals(states.PENDING, wf.state)

        call_log = []

        def do_1(context, *args, **kwargs):
            call_log.append(1)

        def do_2(context, *args, **kwargs):
            call_log.append(2)

        task_1 = FunctorTask(do_1, null_functor)
        task_2 = FunctorTask(do_2, null_functor)
        wf.add(task_1)
        wf.add(task_2)
        wf.task_listeners.append(task_state_change_listener)
        wf.listeners.append(wf_state_change_listener)
        wf.result_fetcher = task_result_fetcher

        # Interrupt it after task_1 finishes
        stop_after.append(task_1.name)
        wf.run({})

        self.assertEquals(1, len(j.logbook))
        self.assertEquals(2, len(j.logbook.fetch_workflow("the-big-action")))
        self.assertEquals(1, len(call_log))

        wf.reset()
        self.assertEquals(states.PENDING, wf.state)
        wf.run({})

        self.assertEquals(1, len(j.logbook))
        self.assertEquals(4, len(j.logbook.fetch_workflow("the-big-action")))
        self.assertEquals(2, len(call_log))
        self.assertEquals(states.SUCCESS, wf.state)

    def testWorkJobLinearClean(self):
        job_claimer = memory.MemoryClaimer()
        book_catalog = memory.MemoryCatalog()

        j = job.Job("the-big-action-job", {}, book_catalog, job_claimer)
        self.assertEquals(states.UNCLAIMED, j.state)
        j.claim("me")
        self.assertEquals(states.CLAIMED, j.state)
        self.assertEquals('me', j.owner)

        def wf_state_change_listener(context, wf, old_state):
            if wf.name in j.logbook:
                return
            j.logbook.add_workflow(wf.name)

        def task_state_change_listener(context, state, wf, task, result=None):
            metadata = None
            wf_details = j.logbook.fetch_workflow(wf.name)
            if state in (states.SUCCESS,):
                metadata = {
                    'result': result,
                }
            wf_details.add_task(logbook.TaskDetail(gen_task_name(task, state),
                                                   metadata))

        def task_result_fetcher(context, wf, task):
            wf_details = j.logbook.fetch_workflow(wf.name)
            td_name = gen_task_name(task, states.SUCCESS)
            if td_name in wf_details:
                task_details = wf_details.fetch_tasks(td_name)[0]
                return (True, task_details.metadata['result'])
            return (False, None)

        wf = lw.Workflow("the-big-action")
        self.assertEquals(states.PENDING, wf.state)

        call_log = []

        def do_1(context, *args, **kwargs):
            call_log.append(1)

        def do_2(context, *args, **kwargs):
            call_log.append(2)

        wf.add(FunctorTask(do_1, null_functor))
        wf.add(FunctorTask(do_2, null_functor))
        wf.task_listeners.append(task_state_change_listener)
        wf.listeners.append(wf_state_change_listener)
        wf.result_fetcher = task_result_fetcher
        wf.run({})

        self.assertEquals(1, len(j.logbook))
        self.assertEquals(4, len(j.logbook.fetch_workflow("the-big-action")))
        self.assertEquals(2, len(call_log))
        self.assertEquals(states.SUCCESS, wf.state)

    def testPostRecvJob(self):
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
