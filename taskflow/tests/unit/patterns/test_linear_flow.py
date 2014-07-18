# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

from taskflow.patterns import linear_flow as lf
from taskflow import retry
from taskflow import test
from taskflow.tests import utils


def _task(name, provides=None, requires=None):
    return utils.ProvidesRequiresTask(name, provides, requires)


class LinearFlowTest(test.TestCase):

    def test_linear_flow_starts_as_empty(self):
        f = lf.Flow('test')

        self.assertEqual(len(f), 0)
        self.assertEqual(list(f), [])
        self.assertEqual(list(f.iter_links()), [])

        self.assertEqual(f.requires, set())
        self.assertEqual(f.provides, set())

        expected = 'taskflow.patterns.linear_flow.Flow: test; 0'
        self.assertEqual(str(f), expected)

    def test_linear_flow_add_nothing(self):
        f = lf.Flow('test')
        result = f.add()
        self.assertIs(f, result)
        self.assertEqual(len(f), 0)

    def test_linear_flow_one_task(self):
        f = lf.Flow('test')
        task = _task(name='task1', requires=['a', 'b'], provides=['c', 'd'])
        result = f.add(task)

        self.assertIs(f, result)

        self.assertEqual(len(f), 1)
        self.assertEqual(list(f), [task])
        self.assertEqual(list(f.iter_links()), [])
        self.assertEqual(f.requires, set(['a', 'b']))
        self.assertEqual(f.provides, set(['c', 'd']))

    def test_linear_flow_two_independent_tasks(self):
        task1 = _task(name='task1')
        task2 = _task(name='task2')
        f = lf.Flow('test').add(task1, task2)

        self.assertEqual(len(f), 2)
        self.assertEqual(list(f), [task1, task2])
        self.assertEqual(list(f.iter_links()), [
            (task1, task2, {'invariant': True})
        ])

    def test_linear_flow_two_dependent_tasks(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = lf.Flow('test').add(task1, task2)

        self.assertEqual(len(f), 2)
        self.assertEqual(list(f), [task1, task2])
        self.assertEqual(list(f.iter_links()), [
            (task1, task2, {'invariant': True})
        ])

        self.assertEqual(f.requires, set())
        self.assertEqual(f.provides, set(['a']))

    def test_linear_flow_two_dependent_tasks_two_different_calls(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = lf.Flow('test').add(task1).add(task2)

        self.assertEqual(len(f), 2)
        self.assertEqual(list(f), [task1, task2])
        self.assertEqual(list(f.iter_links()), [
            (task1, task2, {'invariant': True})
        ])

    def test_linear_flow_three_tasks(self):
        task1 = _task(name='task1')
        task2 = _task(name='task2')
        task3 = _task(name='task3')
        f = lf.Flow('test').add(task1, task2, task3)

        self.assertEqual(len(f), 3)
        self.assertEqual(list(f), [task1, task2, task3])
        self.assertEqual(list(f.iter_links()), [
            (task1, task2, {'invariant': True}),
            (task2, task3, {'invariant': True})
        ])

        expected = 'taskflow.patterns.linear_flow.Flow: test; 3'
        self.assertEqual(str(f), expected)

    def test_linear_flow_with_retry(self):
        ret = retry.AlwaysRevert(requires=['a'], provides=['b'])
        f = lf.Flow('test', ret)
        self.assertIs(f.retry, ret)
        self.assertEqual(ret.name, 'test_retry')

        self.assertEqual(f.requires, set(['a']))
        self.assertEqual(f.provides, set(['b']))
