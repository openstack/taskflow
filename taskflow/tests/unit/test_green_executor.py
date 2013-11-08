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

import collections
import functools

from taskflow import test

from taskflow.utils import eventlet_utils as eu


class GreenExecutorTest(test.TestCase):
    def make_funcs(self, called, amount):

        def store_call(name):
            called[name] += 1

        for i in range(0, amount):
            yield functools.partial(store_call, name=int(i))

    def test_func_calls(self):
        called = collections.defaultdict(int)

        with eu.GreenExecutor(2) as e:
            for f in self.make_funcs(called, 2):
                e.submit(f)

        self.assertEqual(1, called[0])
        self.assertEqual(1, called[1])

    def test_no_construction(self):
        self.assertRaises(AssertionError, eu.GreenExecutor, 0)
        self.assertRaises(AssertionError, eu.GreenExecutor, -1)
        self.assertRaises(AssertionError, eu.GreenExecutor, "-1")

    def test_result_callback(self):
        called = collections.defaultdict(int)

        def call_back(future):
            called[future] += 1

        funcs = list(self.make_funcs(called, 1))
        with eu.GreenExecutor(2) as e:
            f = e.submit(funcs[0])
            f.add_done_callback(call_back)

        self.assertEqual(2, len(called))

    def test_exception_transfer(self):

        def blowup():
            raise IOError("Broke!")

        with eu.GreenExecutor(2) as e:
            f = e.submit(blowup)

        self.assertRaises(IOError, f.result)

    def test_result_transfer(self):

        def return_given(given):
            return given

        create_am = 50
        with eu.GreenExecutor(2) as e:
            futures = []
            for i in range(0, create_am):
                futures.append(e.submit(functools.partial(return_given, i)))

        self.assertEqual(create_am, len(futures))
        for i in range(0, create_am):
            result = futures[i].result()
            self.assertEqual(i, result)

    def test_func_cancellation(self):
        called = collections.defaultdict(int)

        futures = []
        with eu.GreenExecutor(2) as e:
            for func in self.make_funcs(called, 2):
                futures.append(e.submit(func))
            # Greenthreads don't start executing until we wait for them
            # to, since nothing here does IO, this will work out correctly.
            #
            # If something here did a blocking call, then eventlet could swap
            # one of the executors threads in, but nothing in this test does.
            for f in futures:
                self.assertFalse(f.running())
                f.cancel()

        self.assertEqual(0, len(called))
        for f in futures:
            self.assertTrue(f.cancelled())
            self.assertTrue(f.done())
