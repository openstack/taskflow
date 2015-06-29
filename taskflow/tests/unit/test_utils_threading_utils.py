# -*- coding: utf-8 -*-

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

import collections
import functools
import threading
import time

from taskflow import test
from taskflow.utils import threading_utils as tu


def _spinner(death):
    while not death.is_set():
        time.sleep(0.1)


class TestThreadHelpers(test.TestCase):
    def test_alive_thread_falsey(self):
        for v in [False, 0, None, ""]:
            self.assertFalse(tu.is_alive(v))

    def test_alive_thread(self):
        death = threading.Event()
        t = tu.daemon_thread(_spinner, death)
        self.assertFalse(tu.is_alive(t))
        t.start()
        self.assertTrue(tu.is_alive(t))
        death.set()
        t.join()
        self.assertFalse(tu.is_alive(t))

    def test_daemon_thread(self):
        death = threading.Event()
        t = tu.daemon_thread(_spinner, death)
        self.assertTrue(t.daemon)


class TestThreadBundle(test.TestCase):
    thread_count = 5

    def setUp(self):
        super(TestThreadBundle, self).setUp()
        self.bundle = tu.ThreadBundle()
        self.death = threading.Event()
        self.addCleanup(self.bundle.stop)
        self.addCleanup(self.death.set)

    def test_bind_invalid(self):
        self.assertRaises(ValueError, self.bundle.bind, 1)
        for k in ['after_start', 'before_start',
                  'before_join', 'after_join']:
            kwargs = {
                k: 1,
            }
            self.assertRaises(ValueError, self.bundle.bind,
                              lambda: tu.daemon_thread(_spinner, self.death),
                              **kwargs)

    def test_bundle_length(self):
        self.assertEqual(0, len(self.bundle))
        for i in range(0, self.thread_count):
            self.bundle.bind(lambda: tu.daemon_thread(_spinner, self.death))
            self.assertEqual(1, self.bundle.start())
            self.assertEqual(i + 1, len(self.bundle))
        self.death.set()
        self.assertEqual(self.thread_count, self.bundle.stop())
        self.assertEqual(self.thread_count, len(self.bundle))

    def test_start_stop_order(self):
        start_events = collections.deque()
        death_events = collections.deque()

        def before_start(i, t):
            start_events.append((i, 'bs'))

        def before_join(i, t):
            death_events.append((i, 'bj'))
            self.death.set()

        def after_start(i, t):
            start_events.append((i, 'as'))

        def after_join(i, t):
            death_events.append((i, 'aj'))

        for i in range(0, self.thread_count):
            self.bundle.bind(lambda: tu.daemon_thread(_spinner, self.death),
                             before_join=functools.partial(before_join, i),
                             after_join=functools.partial(after_join, i),
                             before_start=functools.partial(before_start, i),
                             after_start=functools.partial(after_start, i))
        self.assertEqual(self.thread_count, self.bundle.start())
        self.assertEqual(self.thread_count, len(self.bundle))
        self.assertEqual(self.thread_count, self.bundle.stop())
        self.assertEqual(0, self.bundle.stop())
        self.assertTrue(self.death.is_set())

        expected_start_events = []
        for i in range(0, self.thread_count):
            expected_start_events.extend([
                (i, 'bs'), (i, 'as'),
            ])
        self.assertEqual(expected_start_events, list(start_events))

        expected_death_events = []
        j = self.thread_count - 1
        for _i in range(0, self.thread_count):
            expected_death_events.extend([
                (j, 'bj'), (j, 'aj'),
            ])
            j -= 1
        self.assertEqual(expected_death_events, list(death_events))

    def test_start_stop(self):
        events = collections.deque()

        def before_start(t):
            events.append('bs')

        def before_join(t):
            events.append('bj')
            self.death.set()

        def after_start(t):
            events.append('as')

        def after_join(t):
            events.append('aj')

        for _i in range(0, self.thread_count):
            self.bundle.bind(lambda: tu.daemon_thread(_spinner, self.death),
                             before_join=before_join,
                             after_join=after_join,
                             before_start=before_start,
                             after_start=after_start)
        self.assertEqual(self.thread_count, self.bundle.start())
        self.assertEqual(self.thread_count, len(self.bundle))
        self.assertEqual(self.thread_count, self.bundle.stop())
        for event in ['as', 'bs', 'bj', 'aj']:
            self.assertEqual(self.thread_count,
                             len([e for e in events if e == event]))
        self.assertEqual(0, self.bundle.stop())
        self.assertTrue(self.death.is_set())
