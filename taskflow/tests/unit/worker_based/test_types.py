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

import datetime
import threading
import time

from oslo.utils import reflection
from oslo.utils import timeutils

from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import types as worker_types
from taskflow import test
from taskflow.tests import utils
from taskflow.types import latch
from taskflow.types import timing


class TestWorkerTypes(test.TestCase):

    def setUp(self):
        super(TestWorkerTypes, self).setUp()
        self.task = utils.DummyTask()
        self.task_uuid = 'task-uuid'
        self.task_action = 'execute'
        self.task_args = {'a': 'a'}
        self.timeout = 60

    def request(self, **kwargs):
        request_kwargs = dict(task=self.task,
                              uuid=self.task_uuid,
                              action=self.task_action,
                              arguments=self.task_args,
                              progress_callback=None,
                              timeout=self.timeout)
        request_kwargs.update(kwargs)
        return pr.Request(**request_kwargs)

    def test_requests_cache_expiry(self):
        # Mock out the calls the underlying objects will soon use to return
        # times that we can control more easily...
        now = timeutils.utcnow()
        overrides = [
            now,
            now,
            now + datetime.timedelta(seconds=1),
            now + datetime.timedelta(seconds=self.timeout + 1),
        ]
        timeutils.set_time_override(overrides)
        self.addCleanup(timeutils.clear_time_override)

        cache = worker_types.RequestsCache()
        cache[self.task_uuid] = self.request()
        cache.cleanup()
        self.assertEqual(1, len(cache))
        cache.cleanup()
        self.assertEqual(0, len(cache))

    def test_requests_cache_match(self):
        cache = worker_types.RequestsCache()
        cache[self.task_uuid] = self.request()
        cache['task-uuid-2'] = self.request(task=utils.NastyTask(),
                                            uuid='task-uuid-2')
        worker = worker_types.TopicWorker("dummy-topic", [utils.DummyTask],
                                          identity="dummy")
        matches = cache.get_waiting_requests(worker)
        self.assertEqual(1, len(matches))
        self.assertEqual(2, len(cache))

    def test_topic_worker(self):
        worker = worker_types.TopicWorker("dummy-topic",
                                          [utils.DummyTask], identity="dummy")
        self.assertTrue(worker.performs(utils.DummyTask))
        self.assertFalse(worker.performs(utils.NastyTask))
        self.assertEqual('dummy', worker.identity)
        self.assertEqual('dummy-topic', worker.topic)

    def test_single_topic_workers(self):
        workers = worker_types.TopicWorkers()
        w = workers.add('dummy-topic', [utils.DummyTask])
        self.assertIsNotNone(w)
        self.assertEqual(1, len(workers))
        w2 = workers.get_worker_for_task(utils.DummyTask)
        self.assertEqual(w.identity, w2.identity)

    def test_multi_same_topic_workers(self):
        workers = worker_types.TopicWorkers()
        w = workers.add('dummy-topic', [utils.DummyTask])
        self.assertIsNotNone(w)
        w2 = workers.add('dummy-topic-2', [utils.DummyTask])
        self.assertIsNotNone(w2)
        w3 = workers.get_worker_for_task(
            reflection.get_class_name(utils.DummyTask))
        self.assertIn(w3.identity, [w.identity, w2.identity])

    def test_multi_different_topic_workers(self):
        workers = worker_types.TopicWorkers()
        added = []
        added.append(workers.add('dummy-topic', [utils.DummyTask]))
        added.append(workers.add('dummy-topic-2', [utils.DummyTask]))
        added.append(workers.add('dummy-topic-3', [utils.NastyTask]))
        self.assertEqual(3, len(workers))
        w = workers.get_worker_for_task(utils.NastyTask)
        self.assertEqual(added[-1].identity, w.identity)
        w = workers.get_worker_for_task(utils.DummyTask)
        self.assertIn(w.identity, [w_a.identity for w_a in added[0:2]])

    def test_periodic_worker(self):
        barrier = latch.Latch(5)
        to = timing.Timeout(0.01)
        called_at = []

        def callee():
            barrier.countdown()
            if barrier.needed == 0:
                to.interrupt()
            called_at.append(time.time())

        w = worker_types.PeriodicWorker(to, [callee])
        t = threading.Thread(target=w.start)
        t.start()
        t.join()

        self.assertEqual(0, barrier.needed)
        self.assertEqual(5, len(called_at))
        self.assertTrue(to.is_stopped())
