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
    def test_invalid_decider_depth(self):
        g_1 = utils.ProgressingTask(name='g-1')
        g_2 = utils.ProgressingTask(name='g-2')
        for not_a_depth in ['not-a-depth', object(), 2, 3.4, False]:
            flow = gf.Flow('g')
            flow.add(g_1, g_2)
            self.assertRaises((ValueError, TypeError),
                              flow.link, g_1, g_2,
                              decider=lambda history: False,
                              decider_depth=not_a_depth)

    def test_graph_flow_stringy(self):
        f = gf.Flow('test')
        expected = 'graph_flow.Flow: test(len=0)'
        self.assertEqual(expected, str(f))

        task1 = _task(name='task1')
        task2 = _task(name='task2')
        task3 = _task(name='task3')
        f = gf.Flow('test')
        f.add(task1, task2, task3)
        expected = 'graph_flow.Flow: test(len=3)'
        self.assertEqual(expected, str(f))

    def test_graph_flow_starts_as_empty(self):
        f = gf.Flow('test')

        self.assertEqual(0, len(f))
        self.assertEqual([], list(f))
        self.assertEqual([], list(f.iter_links()))

        self.assertEqual(set(), f.requires)
        self.assertEqual(set(), f.provides)

    def test_graph_flow_add_nothing(self):
        f = gf.Flow('test')
        result = f.add()
        self.assertIs(f, result)
        self.assertEqual(0, len(f))

    def test_graph_flow_one_task(self):
        f = gf.Flow('test')
        task = _task(name='task1', requires=['a', 'b'], provides=['c', 'd'])
        result = f.add(task)

        self.assertIs(f, result)

        self.assertEqual(1, len(f))
        self.assertEqual([task], list(f))
        self.assertEqual([], list(f.iter_links()))
        self.assertEqual(set(['a', 'b']), f.requires)
        self.assertEqual(set(['c', 'd']), f.provides)

    def test_graph_flow_two_independent_tasks(self):
        task1 = _task(name='task1')
        task2 = _task(name='task2')
        f = gf.Flow('test').add(task1, task2)

        self.assertEqual(2, len(f))
        self.assertItemsEqual(f, [task1, task2])
        self.assertEqual([], list(f.iter_links()))

    def test_graph_flow_two_dependent_tasks(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = gf.Flow('test').add(task1, task2)

        self.assertEqual(2, len(f))
        self.assertItemsEqual(f, [task1, task2])
        self.assertEqual([(task1, task2, {'reasons': set(['a'])})],
                         list(f.iter_links()))

        self.assertEqual(set(), f.requires)
        self.assertEqual(set(['a']), f.provides)

    def test_graph_flow_two_dependent_tasks_two_different_calls(self):
        task1 = _task(name='task1', provides=['a'])
        task2 = _task(name='task2', requires=['a'])
        f = gf.Flow('test').add(task1).add(task2)

        self.assertEqual(2, len(f))
        self.assertItemsEqual(f, [task1, task2])
        self.assertEqual([(task1, task2, {'reasons': set(['a'])})],
                         list(f.iter_links()))

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
        self.assertEqual('test_retry', ret.name)

        self.assertEqual(set(['a']), f.requires)
        self.assertEqual(set(['b']), f.provides)

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
        self.assertRaisesRegexp(ValueError, 'Node .* not found to link from',
                                f.link, task1, task2)

    def test_graph_flow_link_to_unknown_node(self):
        task1 = _task('task1')
        task2 = _task('task2')
        f = gf.Flow('test').add(task1)
        self.assertRaisesRegexp(ValueError, 'Node .* not found to link to',
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

    def test_iter_nodes(self):
        task1 = _task('task1', provides=['a'], requires=['c'])
        task2 = _task('task2', provides=['b'], requires=['a'])
        task3 = _task('task3', provides=['c'])
        f1 = gf.Flow('nested')
        f1.add(task3)
        tasks = set([task1, task2, f1])
        f = gf.Flow('test').add(task1, task2, f1)
        for (n, data) in f.iter_nodes():
            self.assertTrue(n in tasks)
            self.assertDictEqual({}, data)

    def test_iter_links(self):
        task1 = _task('task1')
        task2 = _task('task2')
        task3 = _task('task3')
        f1 = gf.Flow('nested')
        f1.add(task3)
        tasks = set([task1, task2, f1])
        f = gf.Flow('test').add(task1, task2, f1)
        for (u, v, data) in f.iter_links():
            self.assertTrue(u in tasks)
            self.assertTrue(v in tasks)
            self.assertDictEqual({}, data)


class TargetedGraphFlowTest(test.TestCase):

    def test_targeted_flow_restricts(self):
        f = gf.TargetedFlow("test")
        task1 = _task('task1', provides=['a'], requires=[])
        task2 = _task('task2', provides=['b'], requires=['a'])
        task3 = _task('task3', provides=[], requires=['b'])
        task4 = _task('task4', provides=[], requires=['b'])
        f.add(task1, task2, task3, task4)
        f.set_target(task3)
        self.assertEqual(3, len(f))
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
        self.assertEqual(4, len(f))
        self.assertItemsEqual(f, [task1, task2, task3, task4])
        self.assertIn('c', f.provides)

    def test_targeted_flow_bad_target(self):
        f = gf.TargetedFlow("test")
        task1 = _task('task1', provides=['a'], requires=[])
        task2 = _task('task2', provides=['b'], requires=['a'])
        f.add(task1)
        self.assertRaisesRegexp(ValueError, '^Node .* not found',
                                f.set_target, task2)

    def test_targeted_flow_one_node(self):
        f = gf.TargetedFlow("test")
        task1 = _task('task1', provides=['a'], requires=[])
        f.add(task1)
        f.set_target(task1)
        self.assertEqual(1, len(f))
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
        self.assertEqual([(task2, task1, {'manual': True})],
                         list(f.iter_links()), )
