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

    def test_linear_flow_stringy(self):
        f = lf.Flow('test')
        expected = 'linear_flow.Flow: test(len=0)'
        self.assertEqual(expected, str(f))

        task1 = _task(name='task1')
        task2 = _task(name='task2')
        task3 = _task(name='task3')
        f = lf.Flow('test')
        f.add(task1, task2, task3)
        expected = 'linear_flow.Flow: test(len=3)'
        self.assertEqual(expected, str(f))

    def test_linear_flow_starts_as_empty(self):
        f = lf.Flow('test')

        self.assertEqual(0, len(f))
        self.assertEqual([], list(f))
        self.assertEqual([], list(f.iter_links()))

        self.assertEqual(set(), f.requires)
        self.assertEqual(set(), f.provides)

    def test_linear_flow_add_nothing(self):
        f = lf.Flow('test')
        result = f.add()
        self.assertIs(f, result)
        self.assertEqual(0, len(f))

    def test_linear_flow_one_task(self):
        f = lf.Flow('test')
        task = _task(name='task1', requires=['a', 'b'], provides=['c', 'd'])
        result = f.add(task)

        self.assertIs(f, result)

        self.assertEqual(1, len(f))
        self.assertEqual([task], list(f))
        self.assertEqual([], list(f.iter_links()))
        self.assertEqual(set(['a', 'b']), f.requires)
        self.assertEqual(set(['c', 'd']), f.provides)

    def test_linear_flow_two_independent_tasks(self):
        task1 = _task(name='task1')
        task2 = _task(name='task2')
        f = lf.Flow('test').add(task1, task2)

        self.assertEqual(2, len(f))
        self.assertEqual([task1, task2], list(f))
        self.assertEqual([(task1, task2, {'invariant': True})],
                         list(f.iter_links()))

    def test_linear_flow_two_dependent_tasks(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = lf.Flow('test').add(task1, task2)

        self.assertEqual(2, len(f))
        self.assertEqual([task1, task2], list(f))
        self.assertEqual([(task1, task2, {'invariant': True})],
                         list(f.iter_links()))

        self.assertEqual(set(), f.requires)
        self.assertEqual(set(['a']), f.provides)

    def test_linear_flow_two_dependent_tasks_two_different_calls(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = lf.Flow('test').add(task1).add(task2)

        self.assertEqual(2, len(f))
        self.assertEqual([task1, task2], list(f))
        self.assertEqual([(task1, task2, {'invariant': True})],
                         list(f.iter_links()), )

    def test_linear_flow_three_tasks(self):
        task1 = _task(name='task1')
        task2 = _task(name='task2')
        task3 = _task(name='task3')
        f = lf.Flow('test').add(task1, task2, task3)

        self.assertEqual(3, len(f))
        self.assertEqual([task1, task2, task3], list(f))
        self.assertEqual([
            (task1, task2, {'invariant': True}),
            (task2, task3, {'invariant': True})
        ], list(f.iter_links()))

    def test_linear_flow_with_retry(self):
        ret = retry.AlwaysRevert(requires=['a'], provides=['b'])
        f = lf.Flow('test', ret)
        self.assertIs(f.retry, ret)
        self.assertEqual('test_retry', ret.name)

        self.assertEqual(set(['a']), f.requires)
        self.assertEqual(set(['b']), f.provides)

    def test_iter_nodes(self):
        task1 = _task(name='task1')
        task2 = _task(name='task2')
        task3 = _task(name='task3')
        f = lf.Flow('test').add(task1, task2, task3)
        tasks = set([task1, task2, task3])
        for (node, data) in f.iter_nodes():
            self.assertTrue(node in tasks)
            self.assertDictEqual({}, data)

    def test_iter_links(self):
        task1 = _task(name='task1')
        task2 = _task(name='task2')
        task3 = _task(name='task3')
        f = lf.Flow('test').add(task1, task2, task3)
        tasks = set([task1, task2, task3])
        for (u, v, data) in f.iter_links():
            self.assertTrue(u in tasks)
            self.assertTrue(v in tasks)
            self.assertDictEqual({'invariant': True}, data)
