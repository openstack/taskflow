# -*- coding: utf-8 -*-
#    Copyright (C) 2015 Hewlett-Packard Development Company, L.P.
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

import unittest

from taskflow import engines
from taskflow.patterns import linear_flow
from taskflow import task


class TestTask(task.Task):
    def execute(self, a, b=5):
        result = a * b
        return result

flow_no_inject = linear_flow.Flow("flow").add(TestTask(provides='result'))
flow_inject_a = linear_flow.Flow("flow").add(TestTask(provides='result',
                                                      inject={'a': 10}))
flow_inject_b = linear_flow.Flow("flow").add(TestTask(provides='result',
                                                      inject={'b': 1000}))

ASSERT = True


class MyTest(unittest.TestCase):
    def test_my_test(self):
        print("Expected result = 15")
        result = engines.run(flow_no_inject, store={'a': 3})
        print(result)
        if ASSERT:
            self.assertEqual(result,
                             {'a': 3, 'result': 15}
                             )

        print("Expected result = 39")
        result = engines.run(flow_no_inject, store={'a': 3, 'b': 7})
        print(result)
        if ASSERT:
            self.assertEqual(
                result,
                {'a': 3, 'b': 7, 'result': 21}
            )

        print("Expected result = 200")
        result = engines.run(flow_inject_a, store={'a': 3})
        print(result)
        if ASSERT:
            self.assertEqual(
                result,
                {'a': 3, 'result': 50}
            )

        print("Expected result = 400")
        result = engines.run(flow_inject_a, store={'a': 3, 'b': 7})
        print(result)
        if ASSERT:
            self.assertEqual(
                result,
                {'a': 3, 'b': 7, 'result': 70}
            )

        print("Expected result = 40")
        result = engines.run(flow_inject_b, store={'a': 3})
        print(result)
        if ASSERT:
            self.assertEqual(
                result,
                {'a': 3, 'result': 3000}
            )

        print("Expected result = 40")
        result = engines.run(flow_inject_b, store={'a': 3, 'b': 7})
        print(result)
        if ASSERT:
            self.assertEqual(
                result,
                {'a': 3, 'b': 7, 'result': 3000}
            )

if __name__ == '__main__':
    unittest.main()
