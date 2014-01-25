# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

from concurrent import futures

from taskflow import test
from taskflow.utils import async_utils as au
from taskflow.utils import eventlet_utils as eu


class WaitForAnyTestsMixin(object):
    timeout = 0.001

    def test_waits_and_finishes(self):
        def foo():
            pass

        with self.executor_cls(2) as e:
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


class WaiterTestsMixin(object):

    def test_add_result(self):
        waiter = au._Waiter(self.is_green)
        self.assertFalse(waiter.event.is_set())
        waiter.add_result(futures.Future())
        self.assertTrue(waiter.event.is_set())

    def test_add_exception(self):
        waiter = au._Waiter(self.is_green)
        self.assertFalse(waiter.event.is_set())
        waiter.add_exception(futures.Future())
        self.assertTrue(waiter.event.is_set())

    def test_add_cancelled(self):
        waiter = au._Waiter(self.is_green)
        self.assertFalse(waiter.event.is_set())
        waiter.add_cancelled(futures.Future())
        self.assertTrue(waiter.event.is_set())


@testtools.skipIf(not eu.EVENTLET_AVAILABLE, 'eventlet is not available')
class AsyncUtilsEventletTest(test.TestCase,
                             WaitForAnyTestsMixin,
                             WaiterTestsMixin):
    executor_cls = eu.GreenExecutor
    is_green = True


class AsyncUtilsThreadedTest(test.TestCase,
                             WaitForAnyTestsMixin,
                             WaiterTestsMixin):
    executor_cls = futures.ThreadPoolExecutor
    is_green = False


class MakeCompletedFutureTest(test.TestCase):

    def test_make_completed_future(self):
        result = object()
        future = au.make_completed_future(result)
        self.assertTrue(future.done())
        self.assertIs(future.result(), result)
