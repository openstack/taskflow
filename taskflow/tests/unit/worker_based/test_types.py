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

from oslo.utils import reflection

from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import types as worker_types
from taskflow import test
from taskflow.test import mock
from taskflow.tests import utils
from taskflow.types import timing


class TestRequestCache(test.TestCase):

    def setUp(self):
        super(TestRequestCache, self).setUp()
        self.addCleanup(timing.StopWatch.clear_overrides)
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
        overrides = [
            0,
            1,
            self.timeout + 1,
        ]
        timing.StopWatch.set_now_override(overrides)

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


class TestTopicWorker(test.TestCase):
    def test_topic_worker(self):
        worker = worker_types.TopicWorker("dummy-topic",
                                          [utils.DummyTask], identity="dummy")
        self.assertTrue(worker.performs(utils.DummyTask))
        self.assertFalse(worker.performs(utils.NastyTask))
        self.assertEqual('dummy', worker.identity)
        self.assertEqual('dummy-topic', worker.topic)


class TestProxyFinder(test.TestCase):
    def test_single_topic_worker(self):
        finder = worker_types.ProxyWorkerFinder('me', mock.MagicMock(), [])
        w, emit = finder._add('dummy-topic', [utils.DummyTask])
        self.assertIsNotNone(w)
        self.assertTrue(emit)
        self.assertEqual(1, finder._total_workers())
        w2 = finder.get_worker_for_task(utils.DummyTask)
        self.assertEqual(w.identity, w2.identity)

    def test_multi_same_topic_workers(self):
        finder = worker_types.ProxyWorkerFinder('me', mock.MagicMock(), [])
        w, emit = finder._add('dummy-topic', [utils.DummyTask])
        self.assertIsNotNone(w)
        self.assertTrue(emit)
        w2, emit = finder._add('dummy-topic-2', [utils.DummyTask])
        self.assertIsNotNone(w2)
        self.assertTrue(emit)
        w3 = finder.get_worker_for_task(
            reflection.get_class_name(utils.DummyTask))
        self.assertIn(w3.identity, [w.identity, w2.identity])

    def test_multi_different_topic_workers(self):
        finder = worker_types.ProxyWorkerFinder('me', mock.MagicMock(), [])
        added = []
        added.append(finder._add('dummy-topic', [utils.DummyTask]))
        added.append(finder._add('dummy-topic-2', [utils.DummyTask]))
        added.append(finder._add('dummy-topic-3', [utils.NastyTask]))
        self.assertEqual(3, finder._total_workers())
        w = finder.get_worker_for_task(utils.NastyTask)
        self.assertEqual(added[-1][0].identity, w.identity)
        w = finder.get_worker_for_task(utils.DummyTask)
        self.assertIn(w.identity, [w_a[0].identity for w_a in added[0:2]])
