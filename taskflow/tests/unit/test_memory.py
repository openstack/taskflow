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

import time
import threading
import unittest

from taskflow import job
from taskflow.backends import memory


class MemoryBackendTest(unittest.TestCase):
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
