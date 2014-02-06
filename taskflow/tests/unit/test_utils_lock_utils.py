# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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
from taskflow.utils import lock_utils


def _find_overlaps(times, start, end):
    overlaps = 0
    for (s, e) in times:
        if s >= start and e <= end:
            overlaps += 1
    return overlaps


def _spawn_variation(readers, writers, max_workers=None):
    start_stops = collections.deque()
    lock = lock_utils.ReaderWriterLock()

    def read_func():
        with lock.read_lock():
            start_stops.append(('r', time.time(), time.time()))

    def write_func():
        with lock.write_lock():
            start_stops.append(('w', time.time(), time.time()))

    if max_workers is None:
        max_workers = max(0, readers) + max(0, writers)
    if max_workers > 0:
        with futures.ThreadPoolExecutor(max_workers=max_workers) as e:
            for i in range(0, readers):
                e.submit(read_func)
            for i in range(0, writers):
                e.submit(write_func)

    writer_times = []
    reader_times = []
    for (t, start, stop) in list(start_stops):
        if t == 'w':
            writer_times.append((start, stop))
        else:
            reader_times.append((start, stop))
    return (writer_times, reader_times)


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
        active = threading.Event()

        def double_reader():
            with lock.read_lock():
                active.set()
                while lock.pending_writers == 0:
                    time.sleep(0.001)
                with lock.read_lock():
                    activated.append(lock.owner)

        def happy_writer():
            with lock.write_lock():
                activated.append(lock.owner)

        reader = threading.Thread(target=double_reader)
        reader.start()
        active.wait()

        writer = threading.Thread(target=happy_writer)
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
