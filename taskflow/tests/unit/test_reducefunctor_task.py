# -*- coding: utf-8 -*-

#    Copyright 2015 Hewlett-Packard Development Company, L.P.
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

import taskflow.engines as engines
from taskflow.patterns import linear_flow
from taskflow import task as base
from taskflow import test


def sum(x, y):
    return x + y

multiply = lambda x, y: x * y


class ReduceFunctorTaskTest(test.TestCase):

    def setUp(self):
        super(ReduceFunctorTaskTest, self).setUp()

        self.flow_store = {
            'a': 1,
            'b': 2,
            'c': 3,
            'd': 4,
            'e': 5,
        }

    def test_sum_array(self):
        expected = self.flow_store.copy()
        expected.update({
            'sum': 15
        })

        requires = self.flow_store.keys()
        provides = 'sum'

        flow = linear_flow.Flow("sum array flow")
        flow.add(base.ReduceFunctorTask(sum, requires=requires,
                                        provides=provides))

        result = engines.run(flow, store=self.flow_store)
        self.assertDictEqual(expected, result)

    def test_multiply_array(self):
        expected = self.flow_store.copy()
        expected.update({
            'product': 120
        })

        requires = self.flow_store.keys()
        provides = 'product'

        flow = linear_flow.Flow("square array flow")
        flow.add(base.ReduceFunctorTask(multiply, requires=requires,
                                        provides=provides))

        result = engines.run(flow, store=self.flow_store)
        self.assertDictEqual(expected, result)
