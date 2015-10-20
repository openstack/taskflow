# -*- coding: utf-8 -*-

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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

import taskflow.engines
from taskflow.patterns import linear_flow
from taskflow import task as base
from taskflow import test


def add(a, b):
    return a + b


class BunchOfFunctions(object):

    def __init__(self, values):
        self.values = values

    def run_one(self, *args, **kwargs):
        self.values.append('one')

    def revert_one(self, *args, **kwargs):
        self.values.append('revert one')

    def run_fail(self, *args, **kwargs):
        self.values.append('fail')
        raise RuntimeError('Woot!')


five = lambda: 5

multiply = lambda x, y: x * y


class FunctorTaskTest(test.TestCase):

    def test_simple(self):
        task = base.FunctorTask(add)
        self.assertEqual(__name__ + '.add', task.name)

    def test_other_name(self):
        task = base.FunctorTask(add, name='my task')
        self.assertEqual('my task', task.name)

    def test_it_runs(self):
        values = []
        bof = BunchOfFunctions(values)
        t = base.FunctorTask

        flow = linear_flow.Flow('test')
        flow.add(
            t(bof.run_one, revert=bof.revert_one),
            t(bof.run_fail)
        )
        self.assertRaisesRegexp(RuntimeError, '^Woot',
                                taskflow.engines.run, flow)
        self.assertEqual(['one', 'fail', 'revert one'], values)

    def test_lambda_functors(self):
        t = base.FunctorTask

        flow = linear_flow.Flow('test')
        flow.add(
            t(five, provides='five', name='five'),
            t(multiply, provides='product', name='product')
        )

        flow_store = {
            'x': 2,
            'y': 3
        }

        result = taskflow.engines.run(flow, store=flow_store)

        expected = flow_store.copy()
        expected.update({
            'five': 5,
            'product': 6
        })

        self.assertDictEqual(expected, result)
