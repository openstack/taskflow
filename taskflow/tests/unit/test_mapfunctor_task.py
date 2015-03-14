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


def double(x):
    return x * 2

square = lambda x: x * x


class MapFunctorTaskTest(test.TestCase):

    def setUp(self):
        super(MapFunctorTaskTest, self).setUp()

        self.flow_store = {
            'a': 1,
            'b': 2,
            'c': 3,
            'd': 4,
            'e': 5,
        }

    def test_double_array(self):
        expected = self.flow_store.copy()
        expected.update({
            'double_a': 2,
            'double_b': 4,
            'double_c': 6,
            'double_d': 8,
            'double_e': 10,
        })

        requires = self.flow_store.keys()
        provides = ["double_%s" % k for k in requires]

        flow = linear_flow.Flow("double array flow")
        flow.add(base.MapFunctorTask(double, requires=requires,
                                     provides=provides))

        result = engines.run(flow, store=self.flow_store)
        self.assertDictEqual(expected, result)

    def test_square_array(self):
        expected = self.flow_store.copy()
        expected.update({
            'square_a': 1,
            'square_b': 4,
            'square_c': 9,
            'square_d': 16,
            'square_e': 25,
        })

        requires = self.flow_store.keys()
        provides = ["square_%s" % k for k in requires]

        flow = linear_flow.Flow("square array flow")
        flow.add(base.MapFunctorTask(square, requires=requires,
                                     provides=provides))

        result = engines.run(flow, store=self.flow_store)
        self.assertDictEqual(expected, result)
