# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

import testtools

from taskflow import test
from taskflow.types import futures
from taskflow.utils import eventlet_utils as eu

try:
    from eventlet.green import threading as greenthreading
    from eventlet.green import time as greentime
except ImportError:
    pass


def _noop():
    pass


def _blowup():
    raise IOError("Broke!")


def _return_given(given):
    return given


def _return_one():
    return 1


def _double(x):
    return x * 2


class _SimpleFuturesTestMixin(object):
    # This exists to test basic functionality, mainly required to test the
    # process executor which has a very restricted set of things it can
    # execute (no lambda functions, no instance methods...)
    def _make_executor(self, max_workers):
        raise NotImplementedError("Not implemented")

    def test_invalid_workers(self):
        self.assertRaises(ValueError, self._make_executor, -1)
        self.assertRaises(ValueError, self._make_executor, 0)

    def test_exception_transfer(self):
        with self._make_executor(2) as e:
            f = e.submit(_blowup)
        self.assertRaises(IOError, f.result)
        self.assertEqual(1, e.statistics.failures)

    def test_accumulator(self):
        created = []
        with self._make_executor(5) as e:
            for _i in range(0, 10):
                created.append(e.submit(_return_one))
        results = [f.result() for f in created]
        self.assertEqual(10, sum(results))
        self.assertEqual(10, e.statistics.executed)

    def test_map(self):
        count = [i for i in range(0, 100)]
        with self._make_executor(5) as e:
            results = list(e.map(_double, count))
        initial = sum(count)
        self.assertEqual(2 * initial, sum(results))

    def test_alive(self):
        e = self._make_executor(1)
        self.assertTrue(e.alive)
        e.shutdown()
        self.assertFalse(e.alive)
        with self._make_executor(1) as e2:
            self.assertTrue(e2.alive)
        self.assertFalse(e2.alive)


class _FuturesTestMixin(_SimpleFuturesTestMixin):
    def _delay(self, secs):
        raise NotImplementedError("Not implemented")

    def _make_lock(self):
        raise NotImplementedError("Not implemented")

    def _make_funcs(self, called, amount):
        mutator = self._make_lock()

        def store_call(ident):
            with mutator:
                called[ident] += 1

        for i in range(0, amount):
            yield functools.partial(store_call, ident=i)

    def test_func_calls(self):
        called = collections.defaultdict(int)

        with self._make_executor(2) as e:
            for f in self._make_funcs(called, 2):
                e.submit(f)

        self.assertEqual(1, called[0])
        self.assertEqual(1, called[1])
        self.assertEqual(2, e.statistics.executed)

    def test_result_callback(self):
        called = collections.defaultdict(int)
        mutator = self._make_lock()

        def callback(future):
            with mutator:
                called[future] += 1

        funcs = list(self._make_funcs(called, 1))
        with self._make_executor(2) as e:
            for func in funcs:
                f = e.submit(func)
                f.add_done_callback(callback)

        self.assertEqual(2, len(called))

    def test_result_transfer(self):
        create_am = 50
        with self._make_executor(2) as e:
            fs = []
            for i in range(0, create_am):
                fs.append(e.submit(functools.partial(_return_given, i)))
        self.assertEqual(create_am, len(fs))
        self.assertEqual(create_am, e.statistics.executed)
        for i in range(0, create_am):
            result = fs[i].result()
            self.assertEqual(i, result)

    def test_called_restricted_size(self):
        create_am = 100
        called = collections.defaultdict(int)

        with self._make_executor(1) as e:
            for f in self._make_funcs(called, create_am):
                e.submit(f)

        self.assertFalse(e.alive)
        self.assertEqual(create_am, len(called))
        self.assertEqual(create_am, e.statistics.executed)


class ThreadPoolExecutorTest(test.TestCase, _FuturesTestMixin):
    def _make_executor(self, max_workers):
        return futures.ThreadPoolExecutor(max_workers=max_workers)

    def _delay(self, secs):
        time.sleep(secs)

    def _make_lock(self):
        return threading.Lock()


class ProcessPoolExecutorTest(test.TestCase, _SimpleFuturesTestMixin):
    def _make_executor(self, max_workers):
        return futures.ProcessPoolExecutor(max_workers=max_workers)


class SynchronousExecutorTest(test.TestCase, _FuturesTestMixin):
    def _make_executor(self, max_workers):
        return futures.SynchronousExecutor()

    def _delay(self, secs):
        time.sleep(secs)

    def _make_lock(self):
        return threading.Lock()

    def test_invalid_workers(self):
        pass


@testtools.skipIf(not eu.EVENTLET_AVAILABLE, 'eventlet is not available')
class GreenThreadPoolExecutorTest(test.TestCase, _FuturesTestMixin):
    def _make_executor(self, max_workers):
        return futures.GreenThreadPoolExecutor(max_workers=max_workers)

    def _delay(self, secs):
        greentime.sleep(secs)

    def _make_lock(self):
        return greenthreading.Lock()

    def test_cancellation(self):
        called = collections.defaultdict(int)

        fs = []
        with self._make_executor(2) as e:
            for func in self._make_funcs(called, 2):
                fs.append(e.submit(func))
            # Greenthreads don't start executing until we wait for them
            # to, since nothing here does IO, this will work out correctly.
            #
            # If something here did a blocking call, then eventlet could swap
            # one of the executors threads in, but nothing in this test does.
            for f in fs:
                self.assertFalse(f.running())
                f.cancel()

        self.assertEqual(0, len(called))
        self.assertEqual(2, len(fs))
        self.assertEqual(2, e.statistics.cancelled)
        for f in fs:
            self.assertTrue(f.cancelled())
            self.assertTrue(f.done())
