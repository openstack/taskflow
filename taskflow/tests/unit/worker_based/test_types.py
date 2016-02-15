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

from oslo_utils import reflection

from taskflow.engines.worker_based import types as worker_types
from taskflow import test
from taskflow.test import mock
from taskflow.tests import utils


class TestTopicWorker(test.TestCase):
    def test_topic_worker(self):
        worker = worker_types.TopicWorker("dummy-topic",
                                          [utils.DummyTask], identity="dummy")
        self.assertTrue(worker.performs(utils.DummyTask))
        self.assertFalse(worker.performs(utils.NastyTask))
        self.assertEqual('dummy', worker.identity)
        self.assertEqual('dummy-topic', worker.topic)


class TestProxyFinder(test.TestCase):

    @mock.patch("oslo_utils.timeutils.now")
    def test_expiry(self, mock_now):
        finder = worker_types.ProxyWorkerFinder('me', mock.MagicMock(), [],
                                                worker_expiry=60)
        w, emit = finder._add('dummy-topic', [utils.DummyTask])
        w.last_seen = 0
        mock_now.side_effect = [120]
        gone = finder.clean()
        self.assertEqual(0, finder.total_workers)
        self.assertEqual(1, gone)

    def test_single_topic_worker(self):
        finder = worker_types.ProxyWorkerFinder('me', mock.MagicMock(), [])
        w, emit = finder._add('dummy-topic', [utils.DummyTask])
        self.assertIsNotNone(w)
        self.assertTrue(emit)
        self.assertEqual(1, finder.total_workers)
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
        self.assertEqual(3, finder.total_workers)
        w = finder.get_worker_for_task(utils.NastyTask)
        self.assertEqual(added[-1][0].identity, w.identity)
        w = finder.get_worker_for_task(utils.DummyTask)
        self.assertIn(w.identity, [w_a[0].identity for w_a in added[0:2]])
