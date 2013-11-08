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

from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf

from taskflow import exceptions
from taskflow import test
from taskflow.tests import utils


class FlowDependenciesTest(test.TestCase):

    def test_task_without_dependencies(self):
        flow = utils.TaskNoRequiresNoReturns()
        self.assertEqual(flow.requires, set())
        self.assertEqual(flow.provides, set())

    def test_task_requires_default_values(self):
        flow = utils.TaskMultiArg()
        self.assertEqual(flow.requires, set(['x', 'y', 'z']))
        self.assertEqual(flow.provides, set())

    def test_task_requires_rebinded_mapped(self):
        flow = utils.TaskMultiArg(rebind={'x': 'a', 'y': 'b', 'z': 'c'})
        self.assertEqual(flow.requires, set(['a', 'b', 'c']))
        self.assertEqual(flow.provides, set())

    def test_task_requires_additional_values(self):
        flow = utils.TaskMultiArg(requires=['a', 'b'])
        self.assertEqual(flow.requires, set(['a', 'b', 'x', 'y', 'z']))
        self.assertEqual(flow.provides, set())

    def test_task_provides_values(self):
        flow = utils.TaskMultiReturn(provides=['a', 'b', 'c'])
        self.assertEqual(flow.requires, set())
        self.assertEqual(flow.provides, set(['a', 'b', 'c']))

    def test_task_provides_and_requires_values(self):
        flow = utils.TaskMultiArgMultiReturn(provides=['a', 'b', 'c'])
        self.assertEqual(flow.requires, set(['x', 'y', 'z']))
        self.assertEqual(flow.provides, set(['a', 'b', 'c']))

    def test_linear_flow_without_dependencies(self):
        flow = lf.Flow('lf').add(
            utils.TaskNoRequiresNoReturns('task1'),
            utils.TaskNoRequiresNoReturns('task2'))
        self.assertEqual(flow.requires, set())
        self.assertEqual(flow.provides, set())

    def test_linear_flow_reuires_values(self):
        flow = lf.Flow('lf').add(
            utils.TaskOneArg('task1'),
            utils.TaskMultiArg('task2'))
        self.assertEqual(flow.requires, set(['x', 'y', 'z']))
        self.assertEqual(flow.provides, set())

    def test_linear_flow_reuires_rebind_values(self):
        flow = lf.Flow('lf').add(
            utils.TaskOneArg('task1', rebind=['q']),
            utils.TaskMultiArg('task2'))
        self.assertEqual(flow.requires, set(['x', 'y', 'z', 'q']))
        self.assertEqual(flow.provides, set())

    def test_linear_flow_provides_values(self):
        flow = lf.Flow('lf').add(
            utils.TaskOneReturn('task1', provides='x'),
            utils.TaskMultiReturn('task2', provides=['a', 'b', 'c']))
        self.assertEqual(flow.requires, set())
        self.assertEqual(flow.provides, set(['x', 'a', 'b', 'c']))

    def test_linear_flow_provides_out_of_order(self):
        with self.assertRaises(exceptions.InvariantViolationException):
            lf.Flow('lf').add(
                utils.TaskOneArg('task2'),
                utils.TaskOneReturn('task1', provides='x'))

    def test_linear_flow_provides_required_values(self):
        flow = lf.Flow('lf').add(
            utils.TaskOneReturn('task1', provides='x'),
            utils.TaskOneArg('task2'))
        self.assertEqual(flow.requires, set())
        self.assertEqual(flow.provides, set(['x']))

    def test_linear_flow_multi_provides_and_requires_values(self):
        flow = lf.Flow('lf').add(
            utils.TaskMultiArgMultiReturn('task1',
                                          rebind=['a', 'b', 'c'],
                                          provides=['x', 'y', 'q']),
            utils.TaskMultiArgMultiReturn('task2',
                                          provides=['i', 'j', 'k']))
        self.assertEqual(flow.requires, set(['a', 'b', 'c', 'z']))
        self.assertEqual(flow.provides, set(['x', 'y', 'q', 'i', 'j', 'k']))

    def test_linear_flow_self_requires(self):
        flow = lf.Flow('uf')
        with self.assertRaises(exceptions.InvariantViolationException):
            flow.add(utils.TaskNoRequiresNoReturns(rebind=['x'], provides='x'))

    def test_unordered_flow_without_dependencies(self):
        flow = uf.Flow('uf').add(
            utils.TaskNoRequiresNoReturns('task1'),
            utils.TaskNoRequiresNoReturns('task2'))
        self.assertEqual(flow.requires, set())
        self.assertEqual(flow.provides, set())

    def test_unordered_flow_self_requires(self):
        flow = uf.Flow('uf')
        with self.assertRaises(exceptions.InvariantViolationException):
            flow.add(utils.TaskNoRequiresNoReturns(rebind=['x'], provides='x'))

    def test_unordered_flow_reuires_values(self):
        flow = uf.Flow('uf').add(
            utils.TaskOneArg('task1'),
            utils.TaskMultiArg('task2'))
        self.assertEqual(flow.requires, set(['x', 'y', 'z']))
        self.assertEqual(flow.provides, set())

    def test_unordered_flow_reuires_rebind_values(self):
        flow = uf.Flow('uf').add(
            utils.TaskOneArg('task1', rebind=['q']),
            utils.TaskMultiArg('task2'))
        self.assertEqual(flow.requires, set(['x', 'y', 'z', 'q']))
        self.assertEqual(flow.provides, set())

    def test_unordered_flow_provides_values(self):
        flow = uf.Flow('uf').add(
            utils.TaskOneReturn('task1', provides='x'),
            utils.TaskMultiReturn('task2', provides=['a', 'b', 'c']))
        self.assertEqual(flow.requires, set())
        self.assertEqual(flow.provides, set(['x', 'a', 'b', 'c']))

    def test_unordered_flow_provides_required_values(self):
        with self.assertRaises(exceptions.InvariantViolationException):
            uf.Flow('uf').add(
                utils.TaskOneReturn('task1', provides='x'),
                utils.TaskOneArg('task2'))

    def test_unordered_flow_requires_provided_value_other_call(self):
        flow = uf.Flow('uf')
        flow.add(utils.TaskOneReturn('task1', provides='x'))
        with self.assertRaises(exceptions.InvariantViolationException):
            flow.add(utils.TaskOneArg('task2'))

    def test_unordered_flow_provides_required_value_other_call(self):
        flow = uf.Flow('uf')
        flow.add(utils.TaskOneArg('task2'))
        with self.assertRaises(exceptions.InvariantViolationException):
            flow.add(utils.TaskOneReturn('task1', provides='x'))

    def test_unordered_flow_multi_provides_and_requires_values(self):
        flow = uf.Flow('uf').add(
            utils.TaskMultiArgMultiReturn('task1',
                                          rebind=['a', 'b', 'c'],
                                          provides=['d', 'e', 'f']),
            utils.TaskMultiArgMultiReturn('task2',
                                          provides=['i', 'j', 'k']))
        self.assertEqual(flow.requires, set(['a', 'b', 'c', 'x', 'y', 'z']))
        self.assertEqual(flow.provides, set(['d', 'e', 'f', 'i', 'j', 'k']))

    def test_nested_flows_requirements(self):
        flow = uf.Flow('uf').add(
            lf.Flow('lf').add(
                utils.TaskOneArgOneReturn('task1',
                                          rebind=['a'], provides=['x']),
                utils.TaskOneArgOneReturn('task2', provides=['y'])),
            uf.Flow('uf').add(
                utils.TaskOneArgOneReturn('task3',
                                          rebind=['b'], provides=['z']),
                utils.TaskOneArgOneReturn('task4', rebind=['c'],
                                          provides=['q'])))
        self.assertEqual(flow.requires, set(['a', 'b', 'c']))
        self.assertEqual(flow.provides, set(['x', 'y', 'z', 'q']))

    def test_graph_flow_without_dependencies(self):
        flow = gf.Flow('gf').add(
            utils.TaskNoRequiresNoReturns('task1'),
            utils.TaskNoRequiresNoReturns('task2'))
        self.assertEqual(flow.requires, set())
        self.assertEqual(flow.provides, set())

    def test_graph_flow_self_requires(self):
        with self.assertRaisesRegexp(exceptions.DependencyFailure, '^No path'):
            gf.Flow('g-1-req-error').add(
                utils.TaskOneArgOneReturn(requires=['a'], provides='a'))

    def test_graph_flow_reuires_values(self):
        flow = gf.Flow('gf').add(
            utils.TaskOneArg('task1'),
            utils.TaskMultiArg('task2'))
        self.assertEqual(flow.requires, set(['x', 'y', 'z']))
        self.assertEqual(flow.provides, set())

    def test_graph_flow_reuires_rebind_values(self):
        flow = gf.Flow('gf').add(
            utils.TaskOneArg('task1', rebind=['q']),
            utils.TaskMultiArg('task2'))
        self.assertEqual(flow.requires, set(['x', 'y', 'z', 'q']))
        self.assertEqual(flow.provides, set())

    def test_graph_flow_provides_values(self):
        flow = gf.Flow('gf').add(
            utils.TaskOneReturn('task1', provides='x'),
            utils.TaskMultiReturn('task2', provides=['a', 'b', 'c']))
        self.assertEqual(flow.requires, set())
        self.assertEqual(flow.provides, set(['x', 'a', 'b', 'c']))

    def test_graph_flow_provides_required_values(self):
        flow = gf.Flow('gf').add(
            utils.TaskOneReturn('task1', provides='x'),
            utils.TaskOneArg('task2'))
        self.assertEqual(flow.requires, set())
        self.assertEqual(flow.provides, set(['x']))

    def test_graph_flow_provides_provided_value_other_call(self):
        flow = gf.Flow('gf')
        flow.add(utils.TaskOneReturn('task1', provides='x'))
        with self.assertRaises(exceptions.DependencyFailure):
            flow.add(utils.TaskOneReturn('task2', provides='x'))

    def test_graph_flow_multi_provides_and_requires_values(self):
        flow = gf.Flow('gf').add(
            utils.TaskMultiArgMultiReturn('task1',
                                          rebind=['a', 'b', 'c'],
                                          provides=['d', 'e', 'f']),
            utils.TaskMultiArgMultiReturn('task2',
                                          provides=['i', 'j', 'k']))
        self.assertEqual(flow.requires, set(['a', 'b', 'c', 'x', 'y', 'z']))
        self.assertEqual(flow.provides, set(['d', 'e', 'f', 'i', 'j', 'k']))

    def test_graph_cyclic_dependency(self):
        with self.assertRaisesRegexp(exceptions.DependencyFailure, '^No path'):
            gf.Flow('g-3-cyclic').add(
                utils.TaskOneArgOneReturn(provides='a', requires=['b']),
                utils.TaskOneArgOneReturn(provides='b', requires=['c']),
                utils.TaskOneArgOneReturn(provides='c', requires=['a']))
