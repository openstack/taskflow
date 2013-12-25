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


class WaitForAnyTestCase(test.TestCase):

    @testtools.skipIf(not eu.EVENTLET_AVAILABLE, 'eventlet is not available')
    def test_green_waits_and_finishes(self):
        def foo():
            pass

        e = eu.GreenExecutor()

        f1 = e.submit(foo)
        f2 = e.submit(foo)
        # this test assumes that our foo will end within 10 seconds
        done, not_done = au.wait_for_any([f1, f2], 10)
        self.assertIn(len(done), (1, 2))
        self.assertTrue(any((f1 in done, f2 in done)))

    def test_threaded_waits_and_finishes(self):
        def foo():
            pass

        e = futures.ThreadPoolExecutor(2)
        try:
            f1 = e.submit(foo)
            f2 = e.submit(foo)
            # this test assumes that our foo will end within 10 seconds
            done, not_done = au.wait_for_any([f1, f2], 10)
            self.assertIn(len(done), (1, 2))
            self.assertTrue(any((f1 in done, f2 in done)))
        finally:
            e.shutdown()


class MakeCompletedFutureTestCase(test.TestCase):

    def test_make_completed_future(self):
        result = object()
        future = au.make_completed_future(result)
        self.assertTrue(future.done())
        self.assertIs(future.result(), result)
