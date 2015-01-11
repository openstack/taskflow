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

import testtools

from taskflow import test
from taskflow.types import futures
from taskflow.utils import async_utils as au
from taskflow.utils import eventlet_utils as eu


class WaitForAnyTestsMixin(object):
    timeout = 0.001

    def test_waits_and_finishes(self):
        def foo():
            pass

        with self._make_executor(2) as e:
            fs = [e.submit(foo), e.submit(foo)]
            # this test assumes that our foo will end within 10 seconds
            done, not_done = au.wait_for_any(fs, 10)
            self.assertIn(len(done), (1, 2))
            self.assertTrue(any(f in done for f in fs))

    def test_not_done_futures(self):
        fs = [futures.Future(), futures.Future()]
        done, not_done = au.wait_for_any(fs, self.timeout)
        self.assertEqual(len(done), 0)
        self.assertEqual(len(not_done), 2)

    def test_mixed_futures(self):
        f1 = futures.Future()
        f2 = futures.Future()
        f2.set_result(1)
        done, not_done = au.wait_for_any([f1, f2], self.timeout)
        self.assertEqual(len(done), 1)
        self.assertEqual(len(not_done), 1)
        self.assertIs(not_done.pop(), f1)
        self.assertIs(done.pop(), f2)


@testtools.skipIf(not eu.EVENTLET_AVAILABLE, 'eventlet is not available')
class AsyncUtilsEventletTest(test.TestCase,
                             WaitForAnyTestsMixin):
    def _make_executor(self, max_workers):
        return futures.GreenThreadPoolExecutor(max_workers=max_workers)


class AsyncUtilsThreadedTest(test.TestCase,
                             WaitForAnyTestsMixin):
    def _make_executor(self, max_workers):
        return futures.ThreadPoolExecutor(max_workers=max_workers)


class MakeCompletedFutureTest(test.TestCase):

    def test_make_completed_future(self):
        result = object()
        future = au.make_completed_future(result)
        self.assertTrue(future.done())
        self.assertIs(future.result(), result)

    def test_make_completed_future_exception(self):
        result = IOError("broken")
        future = au.make_completed_future(result, exception=True)
        self.assertTrue(future.done())
        self.assertRaises(IOError, future.result)
        self.assertIsNotNone(future.exception())


class AsyncUtilsSynchronousTest(test.TestCase,
                                WaitForAnyTestsMixin):
    def _make_executor(self, max_workers):
        return futures.SynchronousExecutor()
