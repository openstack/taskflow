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

import functools

from oslo_serialization import jsonutils
from oslo_utils import reflection
from tooz import coordination

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
        self.assertEqual(0, finder.available_workers)
        self.assertEqual(1, gone)

    def test_single_topic_worker(self):
        finder = worker_types.ProxyWorkerFinder('me', mock.MagicMock(), [])
        w, emit = finder._add('dummy-topic', [utils.DummyTask])
        self.assertIsNotNone(w)
        self.assertTrue(emit)
        self.assertEqual(1, finder.available_workers)
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
        self.assertEqual(3, finder.available_workers)
        w = finder.get_worker_for_task(utils.NastyTask)
        self.assertEqual(added[-1][0].identity, w.identity)
        w = finder.get_worker_for_task(utils.DummyTask)
        self.assertIn(w.identity, [w_a[0].identity for w_a in added[0:2]])


class ToozFakeResult(object):
    def __init__(self, result):
        self.result = result

    def get(self, timeout=None):
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class TestToozFinder(test.TestCase):
    def setUp(self):
        super(TestToozFinder, self).setUp()
        self.coordinator = mock.create_autospec(
            coordination.CoordinationDriver, instance=True)

    @mock.patch('tooz.coordination.get_coordinator')
    def test_empty_groups(self, get_coordinator_mock):
        get_coordinator_mock.return_value = self.coordinator
        finder = worker_types.ToozWorkerFinder("blah://", 'me', ['a', 'b'])
        self.coordinator.get_groups.return_value = ToozFakeResult(['a', 'b'])
        finder.start()
        finder.discover()
        self.assertTrue(self.coordinator.start.called)
        self.assertTrue(self.coordinator.get_groups.called)
        self.assertTrue(self.coordinator.get_members.called)
        self.assertTrue(self.coordinator.watch_join_group.called)
        self.assertTrue(self.coordinator.watch_leave_group.called)
        self.coordinator.get_members.return_value = ToozFakeResult([])
        self.coordinator.get_members.assert_any_call('a')
        self.coordinator.get_members.assert_any_call('b')
        self.assertEqual(0, finder.available_workers)

    @mock.patch('tooz.coordination.get_coordinator')
    def test_non_empty_group_single_worker(self, get_coordinator_mock):
        get_coordinator_mock.return_value = self.coordinator
        finder = worker_types.ToozWorkerFinder("blah://", 'me', ['a'])
        self.coordinator.get_groups.return_value = ToozFakeResult(['a'])
        self.coordinator.get_members.return_value = ToozFakeResult(['e'])
        worker_capabilities = {
            'topic': '1-202-555-0198',
            'tasks': ['y', 'z'],
        }
        worker_result = ToozFakeResult(jsonutils.dumps(worker_capabilities))
        self.coordinator.get_member_capabilities.return_value = worker_result
        finder.start()
        self.assertEqual(1, finder.available_workers)
        w = finder.get_worker_for_task('y')
        self.assertIsNotNone(w)
        self.assertEqual(w.identity, 'e')
        self.assertTrue(w.performs('z'))
        self.coordinator.get_members.assert_any_call('a')
        finder.clear()
        self.assertEqual(0, finder.available_workers)

    @mock.patch('tooz.coordination.get_coordinator')
    def test_empty_group_watch_join_triggered(self, get_coordinator_mock):
        join_watchers = []
        leave_watchers = []
        capture_join_watchers = lambda g, cb: join_watchers.append((g, cb))
        capture_leave_watchers = lambda g, cb: leave_watchers.append((g, cb))

        def run_watchers(watchers, event):
            for g, cb in watchers:
                cb(event)

        get_coordinator_mock.return_value = self.coordinator
        finder = worker_types.ToozWorkerFinder("blah://", 'me', ['a'])
        self.coordinator.get_groups.return_value = ToozFakeResult(['a'])
        self.coordinator.get_members.return_value = ToozFakeResult([])
        self.coordinator.watch_join_group.side_effect = capture_join_watchers
        self.coordinator.watch_leave_group.side_effect = capture_leave_watchers
        finder.start()
        self.assertEqual(0, finder.available_workers)
        self.assertEqual(1, len(join_watchers))
        self.assertEqual(1, len(leave_watchers))

        event = coordination.MemberJoinedGroup('a', 'y')
        run_join_watchers = functools.partial(run_watchers, join_watchers,
                                              event)
        self.coordinator.run_watchers.side_effect = run_join_watchers
        worker_capabilities = {
            'topic': '1-202-555-0133',
            'tasks': ['y', 'z'],
        }
        worker_result = ToozFakeResult(jsonutils.dumps(worker_capabilities))
        self.coordinator.get_member_capabilities.return_value = worker_result
        finder.notice()
        self.assertTrue(self.coordinator.get_member_capabilities.called)
        self.coordinator.get_member_capabilities.assert_any_call('a', 'y')

        self.assertEqual(1, finder.available_workers)
        w = finder.get_worker_for_task('y')
        self.assertIsNotNone(w)

    @mock.patch('tooz.coordination.get_coordinator')
    def test_group_appear_disappear(self, get_coordinator_mock):
        get_coordinator_mock.return_value = self.coordinator
        finder = worker_types.ToozWorkerFinder("blah://", 'me', ['a'])
        self.coordinator.get_groups.return_value = ToozFakeResult(['a'])
        self.coordinator.get_members.return_value = ToozFakeResult(['x', 'y'])
        workers = {
            'x': {
                'topic': '1-202-555-0122',
                'tasks': [],
            },
            'y': {
                'topic': '1-202-555-0165',
                'tasks': [],
            },
        }

        def get_worker_capabilities(group_id, member_id):
            return ToozFakeResult(jsonutils.dumps(workers[member_id]))

        get_member_capabilities = self.coordinator.get_member_capabilities
        get_member_capabilities.side_effect = get_worker_capabilities

        finder.start()
        self.assertEqual(2, finder.available_workers)

        self.coordinator.get_groups.return_value = ToozFakeResult([])
        finder.discover()
        self.assertEqual(0, finder.available_workers)

        self.assertEqual(2, self.coordinator.get_groups.call_count)

    @mock.patch('tooz.coordination.get_coordinator')
    def test_member_appear_disappear(self, get_coordinator_mock):
        get_coordinator_mock.return_value = self.coordinator
        workers = {
            'x': {
                'topic': '1-202-555-0122',
                'tasks': [],
            },
            'y': {
                'topic': '1-202-555-0165',
                'tasks': [],
            },
        }

        def get_worker_capabilities(group_id, member_id):
            return ToozFakeResult(jsonutils.dumps(workers[member_id]))

        finder = worker_types.ToozWorkerFinder("blah://", 'me', ['a'])
        self.coordinator.get_groups.return_value = ToozFakeResult(['a'])
        self.coordinator.get_members.return_value = ToozFakeResult(['x', 'y'])
        get_member_capabilities = self.coordinator.get_member_capabilities
        get_member_capabilities.side_effect = get_worker_capabilities

        finder.start()
        self.assertEqual(2, finder.available_workers)

        self.coordinator.get_groups.return_value = ToozFakeResult(['a'])
        self.coordinator.get_members.return_value = ToozFakeResult(['y'])
        finder.discover()
        self.assertEqual(1, finder.available_workers)

    @mock.patch('tooz.coordination.get_coordinator')
    def test_beat(self, get_coordinator_mock):
        get_coordinator_mock.return_value = self.coordinator
        finder = worker_types.ToozWorkerFinder("blah://", 'me', ['a'])
        finder.beat()
        self.assertEqual(1, self.coordinator.heartbeat.call_count)
