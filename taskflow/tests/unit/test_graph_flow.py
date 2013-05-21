# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import functools
import unittest

from taskflow import exceptions as excp
from taskflow import states
from taskflow import task
from taskflow import wrappers

from taskflow.patterns import graph_flow as gw


def null_functor(*args, **kwargs):
    return None


class GraphFlowTest(unittest.TestCase):
    def testRevertPath(self):
        flo = gw.Flow("test-flow")
        reverted = []

        def run1(context, *args, **kwargs):
            return {
                'a': 1,
            }

        def run1_revert(context, result, cause):
            reverted.append('run1')
            self.assertEquals(states.REVERTING, cause.flow.state)
            self.assertEquals(result, {'a': 1})

        def run2(context, a, *args, **kwargs):
            raise Exception('Dead')

        flo.add(wrappers.FunctorTask(None, run1, run1_revert,
                                     provides_what=['a'],
                                     extract_requires=True))
        flo.add(wrappers.FunctorTask(None, run2, null_functor,
                                     provides_what=['c'],
                                     extract_requires=True))

        self.assertEquals(states.PENDING, flo.state)
        self.assertRaises(Exception, flo.run, {})
        self.assertEquals(states.FAILURE, flo.state)
        self.assertEquals(['run1'], reverted)

    def testConnectRequirementFailure(self):

        def run1(context, *args, **kwargs):
            return {
                'a': 1,
            }

        def run2(context, b, c, d, *args, **kwargs):
            return None

        flo = gw.Flow("test-flow")
        flo.add(wrappers.FunctorTask(None, run1, null_functor,
                                     provides_what=['a'],
                                     extract_requires=True))
        flo.add(wrappers.FunctorTask(None, run2, null_functor,
                                     extract_requires=True))

        self.assertRaises(excp.InvalidStateException, flo.connect)
        self.assertRaises(excp.InvalidStateException, flo.run, {})
        self.assertRaises(excp.InvalidStateException, flo.order)

    def testHappyPath(self):
        flo = gw.Flow("test-flow")

        run_order = []
        f_args = {}

        def run1(context, *args, **kwargs):
            run_order.append('ran1')
            return {
                'a': 1,
            }

        def run2(context, a, *args, **kwargs):
            run_order.append('ran2')
            return {
                'c': 3,
            }

        def run3(context, a, *args, **kwargs):
            run_order.append('ran3')
            return {
                'b': 2,
            }

        def run4(context, b, c, *args, **kwargs):
            run_order.append('ran4')
            f_args['b'] = b
            f_args['c'] = c

        flo.add(wrappers.FunctorTask(None, run1, null_functor,
                                     provides_what=['a'],
                                     extract_requires=True))
        flo.add(wrappers.FunctorTask(None, run2, null_functor,
                                     provides_what=['c'],
                                     extract_requires=True))
        flo.add(wrappers.FunctorTask(None, run3, null_functor,
                                     provides_what=['b'],
                                     extract_requires=True))
        flo.add(wrappers.FunctorTask(None, run4, null_functor,
                                     extract_requires=True))

        flo.run({})
        self.assertEquals(['ran1', 'ran2', 'ran3', 'ran4'], sorted(run_order))
        self.assertEquals('ran1', run_order[0])
        self.assertEquals('ran4', run_order[-1])
        self.assertEquals({'b': 2, 'c': 3}, f_args)
