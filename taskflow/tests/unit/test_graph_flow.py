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

import collections

from taskflow import decorators
from taskflow import exceptions as excp
from taskflow.patterns import graph_flow as gw
from taskflow import states
from taskflow import test
from taskflow.tests import utils


class GraphFlowTest(test.TestCase):
    def test_reverting_flow(self):
        flo = gw.Flow("test-flow")
        reverted = []

        def run1_revert(context, result, cause):  # pylint: disable=W0613
            reverted.append('run1')
            self.assertEquals(states.REVERTING, cause.flow.state)
            self.assertEquals(result, {'a': 1})

        @decorators.task(revert=run1_revert, provides=['a'])
        def run1(context):  # pylint: disable=W0613
            return {
                'a': 1,
            }

        @decorators.task(provides=['c'])
        def run2(context, a):  # pylint: disable=W0613,C0103
            raise Exception('Dead')

        flo.add(run1)
        flo.add(run2)

        self.assertEquals(states.PENDING, flo.state)
        self.assertRaises(Exception, flo.run, {})
        self.assertEquals(states.FAILURE, flo.state)
        self.assertEquals(['run1'], reverted)

    def test_no_requires_provider(self):
        flo = gw.Flow("test-flow")
        flo.add(utils.ProvidesRequiresTask('test1',
                                           provides=['a', 'b'],
                                           requires=['c', 'd']))
        self.assertEquals(states.PENDING, flo.state)
        self.assertRaises(excp.InvalidStateException, flo.run, {})
        self.assertEquals(states.FAILURE, flo.state)

    def test_looping_flow(self):
        flo = gw.Flow("test-flow")
        flo.add(utils.ProvidesRequiresTask('test1',
                                           provides=['a', 'b'],
                                           requires=['c', 'd', 'e']))
        flo.add(utils.ProvidesRequiresTask('test2',
                                           provides=['c', 'd', 'e'],
                                           requires=['a', 'b']))
        ctx = collections.defaultdict(list)
        self.assertEquals(states.PENDING, flo.state)
        self.assertRaises(excp.InvalidStateException, flo.run, ctx)
        self.assertEquals(states.FAILURE, flo.state)

    def test_complicated_inputs_outputs(self):
        flo = gw.Flow("test-flow")
        flo.add(utils.ProvidesRequiresTask('test1',
                                           provides=['a', 'b'],
                                           requires=['c', 'd', 'e']))
        flo.add(utils.ProvidesRequiresTask('test2',
                                           provides=['c', 'd', 'e'],
                                           requires=[]))
        flo.add(utils.ProvidesRequiresTask('test3',
                                           provides=['c', 'd'],
                                           requires=[]))
        flo.add(utils.ProvidesRequiresTask('test4',
                                           provides=['z'],
                                           requires=['a', 'b', 'c', 'd', 'e']))
        flo.add(utils.ProvidesRequiresTask('test5',
                                           provides=['y'],
                                           requires=['z']))
        flo.add(utils.ProvidesRequiresTask('test6',
                                           provides=[],
                                           requires=['y']))

        self.assertEquals(states.PENDING, flo.state)
        ctx = collections.defaultdict(list)
        flo.run(ctx)
        self.assertEquals(states.SUCCESS, flo.state)
        run_order = ctx[utils.ORDER_KEY]

        # Order isn't deterministic so that's why we sort it
        self.assertEquals(['test2', 'test3'], sorted(run_order[0:2]))

        # This order is deterministic
        self.assertEquals(['test1', 'test4', 'test5', 'test6'], run_order[2:])

    def test_connect_requirement_failure(self):

        @decorators.task(provides=['a'])
        def run1(context):  # pylint: disable=W0613
            return {
                'a': 1,
            }

        @decorators.task
        def run2(context, b, c, d):  # pylint: disable=W0613,C0103
            return None

        flo = gw.Flow("test-flow")
        flo.add(run1)
        flo.add(run2)

        self.assertRaises(excp.InvalidStateException, flo.run, {})

    def test_manual_dependencies(self):
        flo = gw.Flow("test-flow")
        run_order = []

        @decorators.task
        def run1(context):  # pylint: disable=W0613,C0103
            run_order.append('ran1')

        @decorators.task
        def run2(context):  # pylint: disable=W0613,C0103
            run_order.append('ran2')

        @decorators.task
        def run3(context):  # pylint: disable=W0613,C0103
            run_order.append('ran3')

        (uuid1, uuid2, uuid3) = flo.add_many([run1, run2, run3])
        flo.add_dependency(uuid3, uuid2)
        flo.add_dependency(uuid2, uuid1)
        self.assertRaises(ValueError, flo.add_dependency, uuid2, uuid2)
        self.assertRaises(ValueError, flo.add_dependency,
                          uuid2 + "blah", uuid3)

        flo.run({})
        self.assertEquals(['ran3', 'ran2', 'ran1'], run_order)

    def test_manual_providing_dependencies(self):
        flo = gw.Flow("test-flow")

        @decorators.task(provides=['a'])
        def run1(context):
            return {
                'a': 2,
            }

        @decorators.task
        def run2(context, a):
            pass

        uuid1 = flo.add(run1)
        uuid2 = flo.add(run2, infer=False)
        self.assertRaises(excp.MissingDependencies,
                          flo.run, {})
        flo.reset()
        flo.add_dependency(uuid1, uuid2)
        flo.run({})

    def test_happy_flow(self):
        flo = gw.Flow("test-flow")

        run_order = []
        f_args = {}

        @decorators.task(provides=['a'])
        def run1(context):  # pylint: disable=W0613,C0103
            run_order.append('ran1')
            return {
                'a': 1,
            }

        @decorators.task(provides=['c'])
        def run2(context, a):  # pylint: disable=W0613,C0103
            run_order.append('ran2')
            return {
                'c': 3,
            }

        @decorators.task(provides=['b'])
        def run3(context, a):  # pylint: disable=W0613,C0103
            run_order.append('ran3')
            return {
                'b': 2,
            }

        @decorators.task
        def run4(context, b, c):  # pylint: disable=W0613,C0103
            run_order.append('ran4')
            f_args['b'] = b
            f_args['c'] = c

        flo.add(run1)
        flo.add(run2)
        flo.add(run3)
        flo.add(run4)

        flo.run({})
        self.assertEquals(['ran1', 'ran2', 'ran3', 'ran4'], sorted(run_order))
        self.assertEquals('ran1', run_order[0])
        self.assertEquals('ran4', run_order[-1])
        self.assertEquals({'b': 2, 'c': 3}, f_args)
