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

from taskflow import exceptions as exc
from taskflow.patterns import graph_flow as gf
from taskflow import retry
from taskflow import test
from taskflow.tests import utils


def _task(name, provides=None, requires=None):
    return utils.ProvidesRequiresTask(name, provides, requires)


class GraphFlowTest(test.TestCase):

    def test_graph_flow_starts_as_empty(self):
        f = gf.Flow('test')

        self.assertEqual(len(f), 0)
        self.assertEqual(list(f), [])
        self.assertEqual(list(f.iter_links()), [])

        self.assertEqual(f.requires, set())
        self.assertEqual(f.provides, set())

        expected = 'taskflow.patterns.graph_flow.Flow: test; 0'
        self.assertEqual(str(f), expected)

    def test_graph_flow_add_nothing(self):
        f = gf.Flow('test')
        result = f.add()
        self.assertIs(f, result)
        self.assertEqual(len(f), 0)

    def test_graph_flow_one_task(self):
        f = gf.Flow('test')
        task = _task(name='task1', requires=['a', 'b'], provides=['c', 'd'])
        result = f.add(task)

        self.assertIs(f, result)

        self.assertEqual(len(f), 1)
        self.assertEqual(list(f), [task])
        self.assertEqual(list(f.iter_links()), [])
        self.assertEqual(f.requires, set(['a', 'b']))
        self.assertEqual(f.provides, set(['c', 'd']))

    def test_graph_flow_two_independent_tasks(self):
        task1 = _task(name='task1')
        task2 = _task(name='task2')
        f = gf.Flow('test').add(task1, task2)

        self.assertEqual(len(f), 2)
        self.assertItemsEqual(f, [task1, task2])
        self.assertEqual(list(f.iter_links()), [])

    def test_graph_flow_two_dependent_tasks(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = gf.Flow('test').add(task1, task2)

        self.assertEqual(len(f), 2)
        self.assertItemsEqual(f, [task1, task2])
        self.assertEqual(list(f.iter_links()), [
            (task1, task2, {'reasons': set(['a'])})
        ])

        self.assertEqual(f.requires, set())
        self.assertEqual(f.provides, set(['a']))

    def test_graph_flow_two_dependent_tasks_two_different_calls(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = gf.Flow('test').add(task1).add(task2)

        self.assertEqual(len(f), 2)
        self.assertItemsEqual(f, [task1, task2])
        self.assertEqual(list(f.iter_links()), [
            (task1, task2, {'reasons': set(['a'])})
        ])

    def test_graph_flow_two_task_same_provide(self):
        task1 = _task(name='task1', provides=['a', 'b'])
        task2 = _task(name='task2', provides=['a', 'c'])
        f = gf.Flow('test')
        f.add(task2, task1)
        self.assertEqual(set(['a', 'b', 'c']), f.provides)

    def test_graph_flow_ambiguous_provides(self):
        task1 = _task(name='task1', provides=['a', 'b'])
        task2 = _task(name='task2', provides=['a'])
        f = gf.Flow('test')
        f.add(task1, task2)
        self.assertEqual(set(['a', 'b']), f.provides)
        task3 = _task(name='task3', requires=['a'])
        self.assertRaises(exc.AmbiguousDependency, f.add, task3)

    def test_graph_flow_no_resolve_requires(self):
        task1 = _task(name='task1', provides=['a', 'b', 'c'])
        task2 = _task(name='task2', requires=['a', 'b'])
        f = gf.Flow('test')
        f.add(task1, task2, resolve_requires=False)
        self.assertEqual(set(['a', 'b']), f.requires)

    def test_graph_flow_no_resolve_existing(self):
        task1 = _task(name='task1', requires=['a', 'b'])
        task2 = _task(name='task2', provides=['a', 'b'])
        f = gf.Flow('test')
        f.add(task1)
        f.add(task2, resolve_existing=False)
        self.assertEqual(set(['a', 'b']), f.requires)

    def test_graph_flow_resolve_existing(self):
        task1 = _task(name='task1', requires=['a', 'b'])
        task2 = _task(name='task2', provides=['a', 'b'])
        f = gf.Flow('test')
        f.add(task1)
        f.add(task2, resolve_existing=True)
        self.assertEqual(set([]), f.requires)

    def test_graph_flow_with_retry(self):
        ret = retry.AlwaysRevert(requires=['a'], provides=['b'])
        f = gf.Flow('test', ret)
        self.assertIs(f.retry, ret)
        self.assertEqual(ret.name, 'test_retry')

        self.assertEqual(f.requires, set(['a']))
        self.assertEqual(f.provides, set(['b']))

    def test_graph_flow_ordering(self):
        task1 = _task('task1', provides=set(['a', 'b']))
        task2 = _task('task2', provides=['c'], requires=['a', 'b'])
        task3 = _task('task3', provides=[], requires=['c'])
        f = gf.Flow('test').add(task1, task2, task3)

        self.assertEqual(3, len(f))

        self.assertItemsEqual(list(f.iter_links()), [
            (task1, task2, {'reasons': set(['a', 'b'])}),
            (task2, task3, {'reasons': set(['c'])})
        ])

    def test_graph_flow_links(self):
        task1 = _task('task1')
        task2 = _task('task2')
        f = gf.Flow('test').add(task1, task2)
        linked = f.link(task1, task2)
        self.assertIs(linked, f)
        self.assertItemsEqual(list(f.iter_links()), [
            (task1, task2, {'manual': True})
        ])

    def test_graph_flow_links_and_dependencies(self):
        task1 = _task('task1', provides=['a'])
        task2 = _task('task2', requires=['a'])
        f = gf.Flow('test').add(task1, task2)
        linked = f.link(task1, task2)
        self.assertIs(linked, f)
        expected_meta = {
            'manual': True,
            'reasons': set(['a'])
        }
        self.assertItemsEqual(list(f.iter_links()), [
            (task1, task2, expected_meta)
        ])

    def test_graph_flow_link_from_unknown_node(self):
        task1 = _task('task1')
        task2 = _task('task2')
        f = gf.Flow('test').add(task2)
        self.assertRaisesRegexp(ValueError, 'Item .* not found to link from',
                                f.link, task1, task2)

    def test_graph_flow_link_to_unknown_node(self):
        task1 = _task('task1')
        task2 = _task('task2')
        f = gf.Flow('test').add(task1)
        self.assertRaisesRegexp(ValueError, 'Item .* not found to link to',
                                f.link, task1, task2)

    def test_graph_flow_link_raises_on_cycle(self):
        task1 = _task('task1', provides=['a'])
        task2 = _task('task2', requires=['a'])
        f = gf.Flow('test').add(task1, task2)
        self.assertRaises(exc.DependencyFailure, f.link, task2, task1)

    def test_graph_flow_link_raises_on_link_cycle(self):
        task1 = _task('task1')
        task2 = _task('task2')
        f = gf.Flow('test').add(task1, task2)
        f.link(task1, task2)
        self.assertRaises(exc.DependencyFailure, f.link, task2, task1)

    def test_graph_flow_dependency_cycle(self):
        task1 = _task('task1', provides=['a'], requires=['c'])
        task2 = _task('task2', provides=['b'], requires=['a'])
        task3 = _task('task3', provides=['c'], requires=['b'])
        f = gf.Flow('test').add(task1, task2)
        self.assertRaises(exc.DependencyFailure, f.add, task3)


class TargetedGraphFlowTest(test.TestCase):

    def test_targeted_flow_restricts(self):
        f = gf.TargetedFlow("test")
        task1 = _task('task1', provides=['a'], requires=[])
        task2 = _task('task2', provides=['b'], requires=['a'])
        task3 = _task('task3', provides=[], requires=['b'])
        task4 = _task('task4', provides=[], requires=['b'])
        f.add(task1, task2, task3, task4)
        f.set_target(task3)
        self.assertEqual(len(f), 3)
        self.assertItemsEqual(f, [task1, task2, task3])
        self.assertNotIn('c', f.provides)

    def test_targeted_flow_reset(self):
        f = gf.TargetedFlow("test")
        task1 = _task('task1', provides=['a'], requires=[])
        task2 = _task('task2', provides=['b'], requires=['a'])
        task3 = _task('task3', provides=[], requires=['b'])
        task4 = _task('task4', provides=['c'], requires=['b'])
        f.add(task1, task2, task3, task4)
        f.set_target(task3)
        f.reset_target()
        self.assertEqual(len(f), 4)
        self.assertItemsEqual(f, [task1, task2, task3, task4])
        self.assertIn('c', f.provides)

    def test_targeted_flow_bad_target(self):
        f = gf.TargetedFlow("test")
        task1 = _task('task1', provides=['a'], requires=[])
        task2 = _task('task2', provides=['b'], requires=['a'])
        f.add(task1)
        self.assertRaisesRegexp(ValueError, '^Item .* not found',
                                f.set_target, task2)

    def test_targeted_flow_one_node(self):
        f = gf.TargetedFlow("test")
        task1 = _task('task1', provides=['a'], requires=[])
        f.add(task1)
        f.set_target(task1)
        self.assertEqual(len(f), 1)
        self.assertItemsEqual(f, [task1])

    def test_recache_on_add(self):
        f = gf.TargetedFlow("test")
        task1 = _task('task1', provides=[], requires=['a'])
        f.add(task1)
        f.set_target(task1)
        self.assertEqual(1, len(f))
        task2 = _task('task2', provides=['a'], requires=[])
        f.add(task2)
        self.assertEqual(2, len(f))

    def test_recache_on_add_no_deps(self):
        f = gf.TargetedFlow("test")
        task1 = _task('task1', provides=[], requires=[])
        f.add(task1)
        f.set_target(task1)
        self.assertEqual(1, len(f))
        task2 = _task('task2', provides=[], requires=[])
        f.add(task2)
        self.assertEqual(1, len(f))

    def test_recache_on_link(self):
        f = gf.TargetedFlow("test")
        task1 = _task('task1', provides=[], requires=[])
        task2 = _task('task2', provides=[], requires=[])
        f.add(task1, task2)
        f.set_target(task1)
        self.assertEqual(1, len(f))

        f.link(task2, task1)
        self.assertEqual(2, len(f))
        self.assertEqual(list(f.iter_links()), [
            (task2, task1, {'manual': True})
        ])
