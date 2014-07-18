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

    def test_unordered_flow_starts_as_empty(self):
        f = uf.Flow('test')

        self.assertEqual(len(f), 0)
        self.assertEqual(list(f), [])
        self.assertEqual(list(f.iter_links()), [])

        self.assertEqual(f.requires, set())
        self.assertEqual(f.provides, set())

        expected = 'taskflow.patterns.unordered_flow.Flow: test; 0'
        self.assertEqual(str(f), expected)

    def test_unordered_flow_add_nothing(self):
        f = uf.Flow('test')
        result = f.add()
        self.assertIs(f, result)
        self.assertEqual(len(f), 0)

    def test_unordered_flow_one_task(self):
        f = uf.Flow('test')
        task = _task(name='task1', requires=['a', 'b'], provides=['c', 'd'])
        result = f.add(task)

        self.assertIs(f, result)

        self.assertEqual(len(f), 1)
        self.assertEqual(list(f), [task])
        self.assertEqual(list(f.iter_links()), [])
        self.assertEqual(f.requires, set(['a', 'b']))
        self.assertEqual(f.provides, set(['c', 'd']))

    def test_unordered_flow_two_tasks(self):
        task1 = _task(name='task1')
        task2 = _task(name='task2')
        f = uf.Flow('test').add(task1, task2)

        self.assertEqual(len(f), 2)
        self.assertEqual(set(f), set([task1, task2]))
        self.assertEqual(list(f.iter_links()), [])

    def test_unordered_flow_two_tasks_two_different_calls(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = uf.Flow('test').add(task1)
        f.add(task2)
        self.assertEqual(len(f), 2)
        self.assertEqual(set(['a']), f.requires)
        self.assertEqual(set(['a']), f.provides)

    def test_unordered_flow_two_tasks_reverse_order(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = uf.Flow('test').add(task2).add(task1)
        self.assertEqual(len(f), 2)
        self.assertEqual(set(['a']), f.requires)
        self.assertEqual(set(['a']), f.provides)

    def test_unordered_flow_two_task_same_provide(self):
        task1 = _task(name='task1', provides=['a', 'b'])
        task2 = _task(name='task2', provides=['a', 'c'])
        f = uf.Flow('test')
        f.add(task2, task1)
        self.assertEqual(len(f), 2)

    def test_unordered_flow_with_retry(self):
        ret = retry.AlwaysRevert(requires=['a'], provides=['b'])
        f = uf.Flow('test', ret)
        self.assertIs(f.retry, ret)
        self.assertEqual(ret.name, 'test_retry')

        self.assertEqual(f.requires, set(['a']))
        self.assertEqual(f.provides, set(['b']))

    def test_unordered_flow_with_retry_fully_satisfies(self):
        ret = retry.AlwaysRevert(provides=['b', 'a'])
        f = uf.Flow('test', ret)
        f.add(_task(name='task1', requires=['a']))
        self.assertIs(f.retry, ret)
        self.assertEqual(ret.name, 'test_retry')
        self.assertEqual(f.requires, set([]))
        self.assertEqual(f.provides, set(['b', 'a']))
