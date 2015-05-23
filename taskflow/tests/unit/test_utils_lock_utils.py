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

import collections
import threading
import time

from taskflow import test
from taskflow.test import mock
from taskflow.utils import lock_utils
from taskflow.utils import misc
from taskflow.utils import threading_utils

# NOTE(harlowja): Sleep a little so now() can not be the same (which will
# cause false positives when our overlap detection code runs). If there are
# real overlaps then they will still exist.
NAPPY_TIME = 0.05

# We will spend this amount of time doing some "fake" work.
WORK_TIMES = [(0.01 + x / 100.0) for x in range(0, 5)]

# Try to use a more accurate time for overlap detection (one that should
# never go backwards and cause false positives during overlap detection...).
now = misc.find_monotonic(allow_time_time=True)


def _find_overlaps(times, start, end):
    overlaps = 0
    for (s, e) in times:
        if s >= start and e <= end:
            overlaps += 1
    return overlaps


class MultilockTest(test.TestCase):
    THREAD_COUNT = 20

    def test_empty_error(self):
        self.assertRaises(ValueError,
                          lock_utils.MultiLock, [])
        self.assertRaises(ValueError,
                          lock_utils.MultiLock, ())
        self.assertRaises(ValueError,
                          lock_utils.MultiLock, iter([]))

    def test_creation(self):
        locks = []
        for _i in range(0, 10):
            locks.append(threading.Lock())
        n_lock = lock_utils.MultiLock(locks)
        self.assertEqual(0, n_lock.obtained)
        self.assertEqual(len(locks), len(n_lock))

    def test_acquired(self):
        lock1 = threading.Lock()
        lock2 = threading.Lock()
        n_lock = lock_utils.MultiLock((lock1, lock2))
        self.assertTrue(n_lock.acquire())
        try:
            self.assertTrue(lock1.locked())
            self.assertTrue(lock2.locked())
        finally:
            n_lock.release()
        self.assertFalse(lock1.locked())
        self.assertFalse(lock2.locked())

    def test_acquired_context_manager(self):
        lock1 = threading.Lock()
        n_lock = lock_utils.MultiLock([lock1])
        with n_lock as gotten:
            self.assertTrue(gotten)
            self.assertTrue(lock1.locked())
        self.assertFalse(lock1.locked())
        self.assertEqual(0, n_lock.obtained)

    def test_partial_acquired(self):
        lock1 = threading.Lock()
        lock2 = mock.create_autospec(threading.Lock())
        lock2.acquire.return_value = False
        n_lock = lock_utils.MultiLock((lock1, lock2))
        with n_lock as gotten:
            self.assertFalse(gotten)
            self.assertTrue(lock1.locked())
            self.assertEqual(1, n_lock.obtained)
            self.assertEqual(2, len(n_lock))
        self.assertEqual(0, n_lock.obtained)

    def test_partial_acquired_failure(self):
        lock1 = threading.Lock()
        lock2 = mock.create_autospec(threading.Lock())
        lock2.acquire.side_effect = RuntimeError("Broke")
        n_lock = lock_utils.MultiLock((lock1, lock2))
        self.assertRaises(threading.ThreadError, n_lock.acquire)
        self.assertEqual(1, n_lock.obtained)
        n_lock.release()

    def test_release_failure(self):
        lock1 = threading.Lock()
        lock2 = mock.create_autospec(threading.Lock())
        lock2.acquire.return_value = True
        lock2.release.side_effect = RuntimeError("Broke")
        n_lock = lock_utils.MultiLock((lock1, lock2))
        self.assertTrue(n_lock.acquire())
        self.assertEqual(2, n_lock.obtained)
        self.assertRaises(threading.ThreadError, n_lock.release)
        self.assertEqual(2, n_lock.obtained)
        lock2.release.side_effect = None
        n_lock.release()
        self.assertEqual(0, n_lock.obtained)

    def test_release_partial_failure(self):
        lock1 = threading.Lock()
        lock2 = mock.create_autospec(threading.Lock())
        lock2.acquire.return_value = True
        lock2.release.side_effect = RuntimeError("Broke")
        lock3 = threading.Lock()
        n_lock = lock_utils.MultiLock((lock1, lock2, lock3))
        self.assertTrue(n_lock.acquire())
        self.assertEqual(3, n_lock.obtained)
        self.assertRaises(threading.ThreadError, n_lock.release)
        self.assertEqual(2, n_lock.obtained)
        lock2.release.side_effect = None
        n_lock.release()
        self.assertEqual(0, n_lock.obtained)

    def test_acquired_pass(self):
        activated = collections.deque()
        acquires = collections.deque()
        lock1 = threading.Lock()
        lock2 = threading.Lock()
        n_lock = lock_utils.MultiLock((lock1, lock2))

        def critical_section():
            start = now()
            time.sleep(NAPPY_TIME)
            end = now()
            activated.append((start, end))

        def run():
            with n_lock as gotten:
                acquires.append(gotten)
                critical_section()

        threads = []
        for _i in range(0, self.THREAD_COUNT):
            t = threading_utils.daemon_thread(run)
            threads.append(t)
            t.start()
        while threads:
            t = threads.pop()
            t.join()

        self.assertEqual(self.THREAD_COUNT, len(acquires))
        self.assertTrue(all(acquires))
        for (start, end) in activated:
            self.assertEqual(1, _find_overlaps(activated, start, end))
        self.assertFalse(lock1.locked())
        self.assertFalse(lock2.locked())

    def test_acquired_fail(self):
        activated = collections.deque()
        acquires = collections.deque()
        lock1 = threading.Lock()
        lock2 = threading.Lock()
        n_lock = lock_utils.MultiLock((lock1, lock2))

        def run():
            with n_lock as gotten:
                acquires.append(gotten)
                start = now()
                time.sleep(NAPPY_TIME)
                end = now()
                activated.append((start, end))

        def run_fail():
            try:
                with n_lock as gotten:
                    acquires.append(gotten)
                    raise RuntimeError()
            except RuntimeError:
                pass

        threads = []
        for i in range(0, self.THREAD_COUNT):
            if i % 2 == 1:
                target = run_fail
            else:
                target = run
            t = threading_utils.daemon_thread(target)
            threads.append(t)
            t.start()
        while threads:
            t = threads.pop()
            t.join()

        self.assertEqual(self.THREAD_COUNT, len(acquires))
        self.assertTrue(all(acquires))
        for (start, end) in activated:
            self.assertEqual(1, _find_overlaps(activated, start, end))
        self.assertFalse(lock1.locked())
        self.assertFalse(lock2.locked())

    def test_double_acquire_single(self):
        activated = collections.deque()
        acquires = []

        def run():
            start = now()
            time.sleep(NAPPY_TIME)
            end = now()
            activated.append((start, end))

        lock1 = threading.RLock()
        lock2 = threading.RLock()
        n_lock = lock_utils.MultiLock((lock1, lock2))
        with n_lock as gotten:
            acquires.append(gotten)
            run()
            with n_lock as gotten:
                acquires.append(gotten)
                run()
            run()

        self.assertTrue(all(acquires))
        self.assertEqual(2, len(acquires))
        for (start, end) in activated:
            self.assertEqual(1, _find_overlaps(activated, start, end))

    def test_double_acquire_many(self):
        activated = collections.deque()
        acquires = collections.deque()
        n_lock = lock_utils.MultiLock((threading.RLock(), threading.RLock()))

        def critical_section():
            start = now()
            time.sleep(NAPPY_TIME)
            end = now()
            activated.append((start, end))

        def run():
            with n_lock as gotten:
                acquires.append(gotten)
                critical_section()
                with n_lock as gotten:
                    acquires.append(gotten)
                    critical_section()
                critical_section()

        threads = []
        for i in range(0, self.THREAD_COUNT):
            t = threading_utils.daemon_thread(run)
            threads.append(t)
            t.start()
        while threads:
            t = threads.pop()
            t.join()

        self.assertTrue(all(acquires))
        self.assertEqual(self.THREAD_COUNT * 2, len(acquires))
        self.assertEqual(self.THREAD_COUNT * 3, len(activated))
        for (start, end) in activated:
            self.assertEqual(1, _find_overlaps(activated, start, end))

    def test_no_acquire_release(self):
        lock1 = threading.Lock()
        lock2 = threading.Lock()
        n_lock = lock_utils.MultiLock((lock1, lock2))
        self.assertRaises(threading.ThreadError, n_lock.release)
