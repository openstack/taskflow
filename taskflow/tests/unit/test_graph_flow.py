# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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

import collections

import taskflow.engines

from taskflow.patterns import graph_flow as gw
from taskflow.utils import flow_utils as fu
from taskflow.utils import graph_utils as gu

from taskflow import test
from taskflow.tests import utils


class GraphFlowTest(test.TestCase):
    def _make_engine(self, flow):
        return taskflow.engines.load(flow, store={})

    def _capture_states(self):
        # TODO(harlowja): move function to shared helper.
        capture_where = collections.defaultdict(list)

        def do_capture(state, details):
            task_uuid = details.get('task_uuid')
            if not task_uuid:
                return
            capture_where[task_uuid].append(state)

        return (do_capture, capture_where)

    def test_ordering(self):
        wf = gw.Flow("the-test-action")
        test_1 = utils.ProvidesRequiresTask('test-1',
                                            requires=[],
                                            provides=set(['a', 'b']))
        test_2 = utils.ProvidesRequiresTask('test-2',
                                            provides=['c'],
                                            requires=['a', 'b'])
        test_3 = utils.ProvidesRequiresTask('test-3',
                                            provides=[],
                                            requires=['c'])
        wf.add(test_1, test_2, test_3)
        self.assertTrue(wf.graph.has_edge(test_1, test_2))
        self.assertTrue(wf.graph.has_edge(test_2, test_3))
        self.assertEqual(3, len(wf.graph))
        self.assertEqual([test_1], list(gu.get_no_predecessors(wf.graph)))
        self.assertEqual([test_3], list(gu.get_no_successors(wf.graph)))

    def test_basic_edge_reasons(self):
        wf = gw.Flow("the-test-action")
        test_1 = utils.ProvidesRequiresTask('test-1',
                                            requires=[],
                                            provides=set(['a', 'b']))
        test_2 = utils.ProvidesRequiresTask('test-2',
                                            provides=['c'],
                                            requires=['a', 'b'])
        wf.add(test_1, test_2)
        self.assertTrue(wf.graph.has_edge(test_1, test_2))

        edge_attrs = gu.get_edge_attrs(wf.graph, test_1, test_2)
        self.assertTrue(len(edge_attrs) > 0)
        self.assertIn('reasons', edge_attrs)
        self.assertEqual(set(['a', 'b']), edge_attrs['reasons'])

        # 2 -> 1 should not be linked, and therefore have no attrs
        no_edge_attrs = gu.get_edge_attrs(wf.graph, test_2, test_1)
        self.assertFalse(no_edge_attrs)

    def test_linked_edge_reasons(self):
        wf = gw.Flow("the-test-action")
        test_1 = utils.ProvidesRequiresTask('test-1',
                                            requires=[],
                                            provides=[])
        test_2 = utils.ProvidesRequiresTask('test-2',
                                            provides=[],
                                            requires=[])
        wf.add(test_1, test_2)
        self.assertFalse(wf.graph.has_edge(test_1, test_2))
        wf.link(test_1, test_2)
        self.assertTrue(wf.graph.has_edge(test_1, test_2))

        edge_attrs = gu.get_edge_attrs(wf.graph, test_1, test_2)
        self.assertTrue(len(edge_attrs) > 0)
        self.assertTrue(edge_attrs.get('manual'))

    def test_flatten_attribute(self):
        wf = gw.Flow("the-test-action")
        test_1 = utils.ProvidesRequiresTask('test-1',
                                            requires=[],
                                            provides=[])
        test_2 = utils.ProvidesRequiresTask('test-2',
                                            provides=[],
                                            requires=[])
        wf.add(test_1, test_2)
        wf.link(test_1, test_2)
        g = fu.flatten(wf)
        self.assertEqual(2, len(g))
        edge_attrs = gu.get_edge_attrs(g, test_1, test_2)
        self.assertTrue(edge_attrs.get('manual'))
        self.assertTrue(edge_attrs.get('flatten'))


class TargetedGraphFlowTest(test.TestCase):

    def test_targeted_flow(self):
        wf = gw.TargetedFlow("test")
        test_1 = utils.ProvidesRequiresTask('test-1',
                                            provides=['a'], requires=[])
        test_2 = utils.ProvidesRequiresTask('test-2',
                                            provides=['b'], requires=['a'])
        test_3 = utils.ProvidesRequiresTask('test-3',
                                            provides=[], requires=['b'])
        test_4 = utils.ProvidesRequiresTask('test-4',
                                            provides=[], requires=['b'])
        wf.add(test_1, test_2, test_3, test_4)
        wf.set_target(test_3)
        g = fu.flatten(wf)
        self.assertEqual(3, len(g))
        self.assertFalse(g.has_node(test_4))
        self.assertFalse('c' in wf.provides)

    def test_targeted_flow_reset(self):
        wf = gw.TargetedFlow("test")
        test_1 = utils.ProvidesRequiresTask('test-1',
                                            provides=['a'], requires=[])
        test_2 = utils.ProvidesRequiresTask('test-2',
                                            provides=['b'], requires=['a'])
        test_3 = utils.ProvidesRequiresTask('test-3',
                                            provides=[], requires=['b'])
        test_4 = utils.ProvidesRequiresTask('test-4',
                                            provides=['c'], requires=['b'])
        wf.add(test_1, test_2, test_3, test_4)
        wf.set_target(test_3)
        wf.reset_target()
        g = fu.flatten(wf)
        self.assertEqual(4, len(g))
        self.assertTrue(g.has_node(test_4))

    def test_targeted_flow_bad_target(self):
        wf = gw.TargetedFlow("test")
        test_1 = utils.ProvidesRequiresTask('test-1',
                                            provides=['a'], requires=[])
        test_2 = utils.ProvidesRequiresTask('test-2',
                                            provides=['b'], requires=['a'])
        wf.add(test_1)
        self.assertRaisesRegexp(ValueError, '^Item .* not found',
                                wf.set_target, test_2)

    def test_targeted_flow_one_node(self):
        wf = gw.TargetedFlow("test")
        test_1 = utils.ProvidesRequiresTask('test-1',
                                            provides=['a'], requires=[])
        wf.add(test_1)
        wf.set_target(test_1)
        g = fu.flatten(wf)
        self.assertEqual(1, len(g))
        self.assertTrue(g.has_node(test_1))

    def test_recache_on_add(self):
        wf = gw.TargetedFlow("test")
        test_1 = utils.ProvidesRequiresTask('test-1',
                                            provides=[], requires=['a'])
        wf.add(test_1)
        wf.set_target(test_1)
        self.assertEqual(1, len(wf.graph))
        test_2 = utils.ProvidesRequiresTask('test-2',
                                            provides=['a'], requires=[])
        wf.add(test_2)
        self.assertEqual(2, len(wf.graph))

    def test_recache_on_add_no_deps(self):
        wf = gw.TargetedFlow("test")
        test_1 = utils.ProvidesRequiresTask('test-1',
                                            provides=[], requires=[])
        wf.add(test_1)
        wf.set_target(test_1)
        self.assertEqual(1, len(wf.graph))
        test_2 = utils.ProvidesRequiresTask('test-2',
                                            provides=[], requires=[])
        wf.add(test_2)
        self.assertEqual(1, len(wf.graph))

    def test_recache_on_link(self):
        wf = gw.TargetedFlow("test")
        test_1 = utils.ProvidesRequiresTask('test-1',
                                            provides=[], requires=[])
        test_2 = utils.ProvidesRequiresTask('test-2',
                                            provides=[], requires=[])
        wf.add(test_1, test_2)
        wf.set_target(test_1)
        self.assertEqual(1, len(wf.graph))
        wf.link(test_2, test_1)
        self.assertEqual(2, len(wf.graph))
        self.assertEqual([(test_2, test_1)], list(wf.graph.edges()))
