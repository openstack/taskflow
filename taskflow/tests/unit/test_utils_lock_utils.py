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

from concurrent import futures

from taskflow import test
from taskflow.test import mock
from taskflow.tests import utils as test_utils
from taskflow.utils import lock_utils
from taskflow.utils import threading_utils

# NOTE(harlowja): Sleep a little so time.time() can not be the same (which will
# cause false positives when our overlap detection code runs). If there are
# real overlaps then they will still exist.
NAPPY_TIME = 0.05

# We will spend this amount of time doing some "fake" work.
WORK_TIMES = [(0.01 + x / 100.0) for x in range(0, 5)]


def _find_overlaps(times, start, end):
    overlaps = 0
    for (s, e) in times:
        if s >= start and e <= end:
            overlaps += 1
    return overlaps


def _spawn_variation(readers, writers, max_workers=None):
    start_stops = collections.deque()
    lock = lock_utils.ReaderWriterLock()

    def read_func(ident):
        with lock.read_lock():
            # TODO(harlowja): sometime in the future use a monotonic clock here
            # to avoid problems that can be caused by ntpd resyncing the clock
            # while we are actively running.
            enter_time = time.time()
            time.sleep(WORK_TIMES[ident % len(WORK_TIMES)])
            exit_time = time.time()
            start_stops.append((lock.READER, enter_time, exit_time))
            time.sleep(NAPPY_TIME)

    def write_func(ident):
        with lock.write_lock():
            enter_time = time.time()
            time.sleep(WORK_TIMES[ident % len(WORK_TIMES)])
            exit_time = time.time()
            start_stops.append((lock.WRITER, enter_time, exit_time))
            time.sleep(NAPPY_TIME)

    if max_workers is None:
        max_workers = max(0, readers) + max(0, writers)
    if max_workers > 0:
        with futures.ThreadPoolExecutor(max_workers=max_workers) as e:
            count = 0
            for _i in range(0, readers):
                e.submit(read_func, count)
                count += 1
            for _i in range(0, writers):
                e.submit(write_func, count)
                count += 1

    writer_times = []
    reader_times = []
    for (lock_type, start, stop) in list(start_stops):
        if lock_type == lock.WRITER:
            writer_times.append((start, stop))
        else:
            reader_times.append((start, stop))
    return (writer_times, reader_times)


class MultilockTest(test.TestCase):
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
        lock1 = threading.Lock()
        lock2 = threading.Lock()
        n_lock = lock_utils.MultiLock((lock1, lock2))

        def critical_section():
            start = time.time()
            time.sleep(0.05)
            end = time.time()
            activated.append((start, end))

        def run():
            with n_lock:
                critical_section()

        threads = []
        for _i in range(0, 20):
            t = threading_utils.daemon_thread(run)
            threads.append(t)
            t.start()
        while threads:
            t = threads.pop()
            t.join()
        for (start, end) in activated:
            self.assertEqual(1, _find_overlaps(activated, start, end))

        self.assertFalse(lock1.locked())
        self.assertFalse(lock2.locked())

    def test_acquired_fail(self):
        activated = collections.deque()
        lock1 = threading.Lock()
        lock2 = threading.Lock()
        n_lock = lock_utils.MultiLock((lock1, lock2))

        def run():
            with n_lock:
                start = time.time()
                time.sleep(0.05)
                end = time.time()
                activated.append((start, end))

        def run_fail():
            try:
                with n_lock:
                    raise RuntimeError()
            except RuntimeError:
                pass

        threads = []
        for i in range(0, 20):
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

        for (start, end) in activated:
            self.assertEqual(1, _find_overlaps(activated, start, end))
        self.assertFalse(lock1.locked())
        self.assertFalse(lock2.locked())

    def test_double_acquire_single(self):
        activated = collections.deque()

        def run():
            start = time.time()
            time.sleep(0.05)
            end = time.time()
            activated.append((start, end))

        lock1 = threading.RLock()
        lock2 = threading.RLock()
        n_lock = lock_utils.MultiLock((lock1, lock2))
        with n_lock:
            run()
            with n_lock:
                run()
            run()

        for (start, end) in activated:
            self.assertEqual(1, _find_overlaps(activated, start, end))

    def test_double_acquire_many(self):
        activated = collections.deque()
        n_lock = lock_utils.MultiLock((threading.RLock(), threading.RLock()))

        def critical_section():
            start = time.time()
            time.sleep(0.05)
            end = time.time()
            activated.append((start, end))

        def run():
            with n_lock:
                critical_section()
                with n_lock:
                    critical_section()
                critical_section()

        threads = []
        for i in range(0, 20):
            t = threading_utils.daemon_thread(run)
            threads.append(t)
            t.start()
        while threads:
            t = threads.pop()
            t.join()

        for (start, end) in activated:
            self.assertEqual(1, _find_overlaps(activated, start, end))

    def test_no_acquire_release(self):
        lock1 = threading.Lock()
        lock2 = threading.Lock()
        n_lock = lock_utils.MultiLock((lock1, lock2))
        self.assertRaises(threading.ThreadError, n_lock.release)


class ReadWriteLockTest(test.TestCase):
    def test_writer_abort(self):
        lock = lock_utils.ReaderWriterLock()
        self.assertFalse(lock.owner)

        def blow_up():
            with lock.write_lock():
                self.assertEqual(lock.WRITER, lock.owner)
                raise RuntimeError("Broken")

        self.assertRaises(RuntimeError, blow_up)
        self.assertFalse(lock.owner)

    def test_reader_abort(self):
        lock = lock_utils.ReaderWriterLock()
        self.assertFalse(lock.owner)

        def blow_up():
            with lock.read_lock():
                self.assertEqual(lock.READER, lock.owner)
                raise RuntimeError("Broken")

        self.assertRaises(RuntimeError, blow_up)
        self.assertFalse(lock.owner)

    def test_double_reader_abort(self):
        lock = lock_utils.ReaderWriterLock()
        activated = collections.deque()

        def double_bad_reader():
            with lock.read_lock():
                with lock.read_lock():
                    raise RuntimeError("Broken")

        def happy_writer():
            with lock.write_lock():
                activated.append(lock.owner)

        with futures.ThreadPoolExecutor(max_workers=20) as e:
            for i in range(0, 20):
                if i % 2 == 0:
                    e.submit(double_bad_reader)
                else:
                    e.submit(happy_writer)

        self.assertEqual(10, len([a for a in activated if a == 'w']))

    def test_double_reader_writer(self):
        lock = lock_utils.ReaderWriterLock()
        activated = collections.deque()
        active = threading_utils.Event()

        def double_reader():
            with lock.read_lock():
                active.set()
                while not lock.has_pending_writers:
                    time.sleep(0.001)
                with lock.read_lock():
                    activated.append(lock.owner)

        def happy_writer():
            with lock.write_lock():
                activated.append(lock.owner)

        reader = threading_utils.daemon_thread(double_reader)
        reader.start()
        self.assertTrue(active.wait(test_utils.WAIT_TIMEOUT))

        writer = threading_utils.daemon_thread(happy_writer)
        writer.start()

        reader.join()
        writer.join()
        self.assertEqual(2, len(activated))
        self.assertEqual(['r', 'w'], list(activated))

    def test_reader_chaotic(self):
        lock = lock_utils.ReaderWriterLock()
        activated = collections.deque()

        def chaotic_reader(blow_up):
            with lock.read_lock():
                if blow_up:
                    raise RuntimeError("Broken")
                else:
                    activated.append(lock.owner)

        def happy_writer():
            with lock.write_lock():
                activated.append(lock.owner)

        with futures.ThreadPoolExecutor(max_workers=20) as e:
            for i in range(0, 20):
                if i % 2 == 0:
                    e.submit(chaotic_reader, blow_up=bool(i % 4 == 0))
                else:
                    e.submit(happy_writer)

        writers = [a for a in activated if a == 'w']
        readers = [a for a in activated if a == 'r']
        self.assertEqual(10, len(writers))
        self.assertEqual(5, len(readers))

    def test_writer_chaotic(self):
        lock = lock_utils.ReaderWriterLock()
        activated = collections.deque()

        def chaotic_writer(blow_up):
            with lock.write_lock():
                if blow_up:
                    raise RuntimeError("Broken")
                else:
                    activated.append(lock.owner)

        def happy_reader():
            with lock.read_lock():
                activated.append(lock.owner)

        with futures.ThreadPoolExecutor(max_workers=20) as e:
            for i in range(0, 20):
                if i % 2 == 0:
                    e.submit(chaotic_writer, blow_up=bool(i % 4 == 0))
                else:
                    e.submit(happy_reader)

        writers = [a for a in activated if a == 'w']
        readers = [a for a in activated if a == 'r']
        self.assertEqual(5, len(writers))
        self.assertEqual(10, len(readers))

    def test_single_reader_writer(self):
        results = []
        lock = lock_utils.ReaderWriterLock()
        with lock.read_lock():
            self.assertTrue(lock.is_reader())
            self.assertEqual(0, len(results))
        with lock.write_lock():
            results.append(1)
            self.assertTrue(lock.is_writer())
        with lock.read_lock():
            self.assertTrue(lock.is_reader())
            self.assertEqual(1, len(results))
        self.assertFalse(lock.is_reader())
        self.assertFalse(lock.is_writer())

    def test_reader_to_writer(self):
        lock = lock_utils.ReaderWriterLock()

        def writer_func():
            with lock.write_lock():
                pass

        with lock.read_lock():
            self.assertRaises(RuntimeError, writer_func)
            self.assertFalse(lock.is_writer())

        self.assertFalse(lock.is_reader())
        self.assertFalse(lock.is_writer())

    def test_writer_to_reader(self):
        lock = lock_utils.ReaderWriterLock()

        def reader_func():
            with lock.read_lock():
                pass

        with lock.write_lock():
            self.assertRaises(RuntimeError, reader_func)
            self.assertFalse(lock.is_reader())

        self.assertFalse(lock.is_reader())
        self.assertFalse(lock.is_writer())

    def test_double_writer(self):
        lock = lock_utils.ReaderWriterLock()
        with lock.write_lock():
            self.assertFalse(lock.is_reader())
            self.assertTrue(lock.is_writer())
            with lock.write_lock():
                self.assertTrue(lock.is_writer())
            self.assertTrue(lock.is_writer())

        self.assertFalse(lock.is_reader())
        self.assertFalse(lock.is_writer())

    def test_double_reader(self):
        lock = lock_utils.ReaderWriterLock()
        with lock.read_lock():
            self.assertTrue(lock.is_reader())
            self.assertFalse(lock.is_writer())
            with lock.read_lock():
                self.assertTrue(lock.is_reader())
            self.assertTrue(lock.is_reader())

        self.assertFalse(lock.is_reader())
        self.assertFalse(lock.is_writer())

    def test_multi_reader_multi_writer(self):
        writer_times, reader_times = _spawn_variation(10, 10)
        self.assertEqual(10, len(writer_times))
        self.assertEqual(10, len(reader_times))
        for (start, stop) in writer_times:
            self.assertEqual(0, _find_overlaps(reader_times, start, stop))
            self.assertEqual(1, _find_overlaps(writer_times, start, stop))
        for (start, stop) in reader_times:
            self.assertEqual(0, _find_overlaps(writer_times, start, stop))

    def test_multi_reader_single_writer(self):
        writer_times, reader_times = _spawn_variation(9, 1)
        self.assertEqual(1, len(writer_times))
        self.assertEqual(9, len(reader_times))
        start, stop = writer_times[0]
        self.assertEqual(0, _find_overlaps(reader_times, start, stop))

    def test_multi_writer(self):
        writer_times, reader_times = _spawn_variation(0, 10)
        self.assertEqual(10, len(writer_times))
        self.assertEqual(0, len(reader_times))
        for (start, stop) in writer_times:
            self.assertEqual(1, _find_overlaps(writer_times, start, stop))
