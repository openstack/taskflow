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

from taskflow.patterns import unordered_flow as uf
from taskflow import retry
from taskflow import test
from taskflow.tests import utils


def _task(name, provides=None, requires=None):
    return utils.ProvidesRequiresTask(name, provides, requires)


class UnorderedFlowTest(test.TestCase):

    def test_unordered_flow_stringy(self):
        f = uf.Flow('test')
        expected = '"unordered_flow.Flow: test(len=0)"'
        self.assertEqual(expected, str(f))

        task1 = _task(name='task1')
        task2 = _task(name='task2')
        task3 = _task(name='task3')
        f = uf.Flow('test')
        f.add(task1, task2, task3)
        expected = '"unordered_flow.Flow: test(len=3)"'
        self.assertEqual(expected, str(f))

    def test_unordered_flow_starts_as_empty(self):
        f = uf.Flow('test')

        self.assertEqual(0, len(f))
        self.assertEqual([], list(f))
        self.assertEqual([], list(f.iter_links()))

        self.assertEqual(set(), f.requires)
        self.assertEqual(set(), f.provides)

    def test_unordered_flow_add_nothing(self):
        f = uf.Flow('test')
        result = f.add()
        self.assertIs(f, result)
        self.assertEqual(0, len(f))

    def test_unordered_flow_one_task(self):
        f = uf.Flow('test')
        task = _task(name='task1', requires=['a', 'b'], provides=['c', 'd'])
        result = f.add(task)

        self.assertIs(f, result)

        self.assertEqual(1, len(f))
        self.assertEqual([task], list(f))
        self.assertEqual([], list(f.iter_links()))
        self.assertEqual(set(['a', 'b']), f.requires)
        self.assertEqual(set(['c', 'd']), f.provides)

    def test_unordered_flow_two_tasks(self):
        task1 = _task(name='task1')
        task2 = _task(name='task2')
        f = uf.Flow('test').add(task1, task2)

        self.assertEqual(2, len(f))
        self.assertEqual(set([task1, task2]), set(f))
        self.assertEqual([], list(f.iter_links()))

    def test_unordered_flow_two_tasks_two_different_calls(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = uf.Flow('test').add(task1)
        f.add(task2)
        self.assertEqual(2, len(f))
        self.assertEqual(set(['a']), f.requires)
        self.assertEqual(set(['a']), f.provides)

    def test_unordered_flow_two_tasks_reverse_order(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = uf.Flow('test').add(task2).add(task1)
        self.assertEqual(2, len(f))
        self.assertEqual(set(['a']), f.requires)
        self.assertEqual(set(['a']), f.provides)

    def test_unordered_flow_two_task_same_provide(self):
        task1 = _task(name='task1', provides=['a', 'b'])
        task2 = _task(name='task2', provides=['a', 'c'])
        f = uf.Flow('test')
        f.add(task2, task1)
        self.assertEqual(2, len(f))

    def test_unordered_flow_with_retry(self):
        ret = retry.AlwaysRevert(requires=['a'], provides=['b'])
        f = uf.Flow('test', ret)
        self.assertIs(f.retry, ret)
        self.assertEqual('test_retry', ret.name)

        self.assertEqual(set(['a']), f.requires)
        self.assertEqual(set(['b']), f.provides)

    def test_unordered_flow_with_retry_fully_satisfies(self):
        ret = retry.AlwaysRevert(provides=['b', 'a'])
        f = uf.Flow('test', ret)
        f.add(_task(name='task1', requires=['a']))
        self.assertIs(f.retry, ret)
        self.assertEqual('test_retry', ret.name)
        self.assertEqual(set([]), f.requires)
        self.assertEqual(set(['b', 'a']), f.provides)

    def test_iter_nodes(self):
        task1 = _task(name='task1', provides=['a', 'b'])
        task2 = _task(name='task2', provides=['a', 'c'])
        tasks = set([task1, task2])
        f = uf.Flow('test')
        f.add(task2, task1)
        for (node, data) in f.iter_nodes():
            self.assertIn(node, tasks)
            self.assertDictEqual({}, data)

    def test_iter_links(self):
        task1 = _task(name='task1', provides=['a', 'b'])
        task2 = _task(name='task2', provides=['a', 'c'])
        f = uf.Flow('test')
        f.add(task2, task1)
        for (u, v, data) in f.iter_links():
            raise AssertionError('links iterator should be empty')
