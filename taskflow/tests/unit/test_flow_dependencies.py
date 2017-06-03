# -*- coding: utf-8 -*-

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

from taskflow import exceptions
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow import retry
from taskflow import test
from taskflow.tests import utils


class FlowDependenciesTest(test.TestCase):

    def test_task_without_dependencies(self):
        flow = utils.TaskNoRequiresNoReturns()
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_task_requires_default_values(self):
        flow = utils.TaskMultiArg()
        self.assertEqual(set(['x', 'y', 'z']), flow.requires)
        self.assertEqual(set(), flow.provides, )

    def test_task_requires_rebinded_mapped(self):
        flow = utils.TaskMultiArg(rebind={'x': 'a', 'y': 'b', 'z': 'c'})
        self.assertEqual(set(['a', 'b', 'c']), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_task_requires_additional_values(self):
        flow = utils.TaskMultiArg(requires=['a', 'b'])
        self.assertEqual(set(['a', 'b', 'x', 'y', 'z']), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_task_provides_values(self):
        flow = utils.TaskMultiReturn(provides=['a', 'b', 'c'])
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(['a', 'b', 'c']), flow.provides)

    def test_task_provides_and_requires_values(self):
        flow = utils.TaskMultiArgMultiReturn(provides=['a', 'b', 'c'])
        self.assertEqual(set(['x', 'y', 'z']), flow.requires)
        self.assertEqual(set(['a', 'b', 'c']), flow.provides)

    def test_linear_flow_without_dependencies(self):
        flow = lf.Flow('lf').add(
            utils.TaskNoRequiresNoReturns('task1'),
            utils.TaskNoRequiresNoReturns('task2'))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_linear_flow_requires_values(self):
        flow = lf.Flow('lf').add(
            utils.TaskOneArg('task1'),
            utils.TaskMultiArg('task2'))
        self.assertEqual(set(['x', 'y', 'z']), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_linear_flow_requires_rebind_values(self):
        flow = lf.Flow('lf').add(
            utils.TaskOneArg('task1', rebind=['q']),
            utils.TaskMultiArg('task2'))
        self.assertEqual(set(['x', 'y', 'z', 'q']), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_linear_flow_provides_values(self):
        flow = lf.Flow('lf').add(
            utils.TaskOneReturn('task1', provides='x'),
            utils.TaskMultiReturn('task2', provides=['a', 'b', 'c']))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(['x', 'a', 'b', 'c']), flow.provides)

    def test_linear_flow_provides_required_values(self):
        flow = lf.Flow('lf').add(
            utils.TaskOneReturn('task1', provides='x'),
            utils.TaskOneArg('task2'))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(['x']), flow.provides)

    def test_linear_flow_multi_provides_and_requires_values(self):
        flow = lf.Flow('lf').add(
            utils.TaskMultiArgMultiReturn('task1',
                                          rebind=['a', 'b', 'c'],
                                          provides=['x', 'y', 'q']),
            utils.TaskMultiArgMultiReturn('task2',
                                          provides=['i', 'j', 'k']))
        self.assertEqual(set(['a', 'b', 'c', 'z']), flow.requires)
        self.assertEqual(set(['x', 'y', 'q', 'i', 'j', 'k']), flow.provides)

    def test_unordered_flow_without_dependencies(self):
        flow = uf.Flow('uf').add(
            utils.TaskNoRequiresNoReturns('task1'),
            utils.TaskNoRequiresNoReturns('task2'))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_unordered_flow_requires_values(self):
        flow = uf.Flow('uf').add(
            utils.TaskOneArg('task1'),
            utils.TaskMultiArg('task2'))
        self.assertEqual(set(['x', 'y', 'z']), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_unordered_flow_requires_rebind_values(self):
        flow = uf.Flow('uf').add(
            utils.TaskOneArg('task1', rebind=['q']),
            utils.TaskMultiArg('task2'))
        self.assertEqual(set(['x', 'y', 'z', 'q']), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_unordered_flow_provides_values(self):
        flow = uf.Flow('uf').add(
            utils.TaskOneReturn('task1', provides='x'),
            utils.TaskMultiReturn('task2', provides=['a', 'b', 'c']))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(['x', 'a', 'b', 'c']), flow.provides)

    def test_unordered_flow_provides_required_values(self):
        flow = uf.Flow('uf')
        flow.add(utils.TaskOneReturn('task1', provides='x'),
                 utils.TaskOneArg('task2'))
        flow.add(utils.TaskOneReturn('task1', provides='x'),
                 utils.TaskOneArg('task2'))
        self.assertEqual(set(['x']), flow.provides)
        self.assertEqual(set(['x']), flow.requires)

    def test_unordered_flow_requires_provided_value_other_call(self):
        flow = uf.Flow('uf')
        flow.add(utils.TaskOneReturn('task1', provides='x'))
        flow.add(utils.TaskOneArg('task2'))
        self.assertEqual(set(['x']), flow.provides)
        self.assertEqual(set(['x']), flow.requires)

    def test_unordered_flow_provides_required_value_other_call(self):
        flow = uf.Flow('uf')
        flow.add(utils.TaskOneArg('task2'))
        flow.add(utils.TaskOneReturn('task1', provides='x'))
        self.assertEqual(2, len(flow))
        self.assertEqual(set(['x']), flow.provides)
        self.assertEqual(set(['x']), flow.requires)

    def test_unordered_flow_multi_provides_and_requires_values(self):
        flow = uf.Flow('uf').add(
            utils.TaskMultiArgMultiReturn('task1',
                                          rebind=['a', 'b', 'c'],
                                          provides=['d', 'e', 'f']),
            utils.TaskMultiArgMultiReturn('task2',
                                          provides=['i', 'j', 'k']))
        self.assertEqual(set(['a', 'b', 'c', 'x', 'y', 'z']), flow.requires)
        self.assertEqual(set(['d', 'e', 'f', 'i', 'j', 'k']), flow.provides)

    def test_unordered_flow_provides_same_values(self):
        flow = uf.Flow('uf').add(utils.TaskOneReturn(provides='x'))
        flow.add(utils.TaskOneReturn(provides='x'))
        self.assertEqual(set(['x']), flow.provides)

    def test_unordered_flow_provides_same_values_one_add(self):
        flow = uf.Flow('uf')
        flow.add(utils.TaskOneReturn(provides='x'),
                 utils.TaskOneReturn(provides='x'))
        self.assertEqual(set(['x']), flow.provides)

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
        self.assertEqual(set(['a', 'b', 'c']), flow.requires)
        self.assertEqual(set(['x', 'y', 'z', 'q']), flow.provides)

    def test_graph_flow_requires_values(self):
        flow = gf.Flow('gf').add(
            utils.TaskOneArg('task1'),
            utils.TaskMultiArg('task2'))
        self.assertEqual(set(['x', 'y', 'z']), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_graph_flow_requires_rebind_values(self):
        flow = gf.Flow('gf').add(
            utils.TaskOneArg('task1', rebind=['q']),
            utils.TaskMultiArg('task2'))
        self.assertEqual(set(['x', 'y', 'z', 'q']), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_graph_flow_provides_values(self):
        flow = gf.Flow('gf').add(
            utils.TaskOneReturn('task1', provides='x'),
            utils.TaskMultiReturn('task2', provides=['a', 'b', 'c']))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(['x', 'a', 'b', 'c']), flow.provides)

    def test_graph_flow_provides_required_values(self):
        flow = gf.Flow('gf').add(
            utils.TaskOneReturn('task1', provides='x'),
            utils.TaskOneArg('task2'))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(['x']), flow.provides)

    def test_graph_flow_provides_provided_value_other_call(self):
        flow = gf.Flow('gf')
        flow.add(utils.TaskOneReturn('task1', provides='x'))
        flow.add(utils.TaskOneReturn('task2', provides='x'))
        self.assertEqual(set(['x']), flow.provides)

    def test_graph_flow_multi_provides_and_requires_values(self):
        flow = gf.Flow('gf').add(
            utils.TaskMultiArgMultiReturn('task1',
                                          rebind=['a', 'b', 'c'],
                                          provides=['d', 'e', 'f']),
            utils.TaskMultiArgMultiReturn('task2',
                                          provides=['i', 'j', 'k']))
        self.assertEqual(set(['a', 'b', 'c', 'x', 'y', 'z']), flow.requires)
        self.assertEqual(set(['d', 'e', 'f', 'i', 'j', 'k']), flow.provides)

    def test_graph_cyclic_dependency(self):
        flow = gf.Flow('g-3-cyclic')
        self.assertRaisesRegex(exceptions.DependencyFailure, '^No path',
                               flow.add,
                               utils.TaskOneArgOneReturn(provides='a',
                                                         requires=['b']),
                               utils.TaskOneArgOneReturn(provides='b',
                                                         requires=['c']),
                               utils.TaskOneArgOneReturn(provides='c',
                                                         requires=['a']))

    def test_task_requires_and_provides_same_values(self):
        flow = lf.Flow('lf', utils.TaskOneArgOneReturn('rt', requires='x',
                                                       provides='x'))
        self.assertEqual(set('x'), flow.requires)
        self.assertEqual(set('x'), flow.provides)

    def test_retry_in_linear_flow_no_requirements_no_provides(self):
        flow = lf.Flow('lf', retry.AlwaysRevert('rt'))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_retry_in_linear_flow_with_requirements(self):
        flow = lf.Flow('lf', retry.AlwaysRevert('rt', requires=['x', 'y']))
        self.assertEqual(set(['x', 'y']), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_retry_in_linear_flow_with_provides(self):
        flow = lf.Flow('lf', retry.AlwaysRevert('rt', provides=['x', 'y']))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(['x', 'y']), flow.provides)

    def test_retry_in_linear_flow_requires_and_provides(self):
        flow = lf.Flow('lf', retry.AlwaysRevert('rt',
                                                requires=['x', 'y'],
                                                provides=['a', 'b']))
        self.assertEqual(set(['x', 'y']), flow.requires)
        self.assertEqual(set(['a', 'b']), flow.provides)

    def test_retry_requires_and_provides_same_value(self):
        flow = lf.Flow('lf', retry.AlwaysRevert('rt',
                                                requires=['x', 'y'],
                                                provides=['x', 'y']))
        self.assertEqual(set(['x', 'y']), flow.requires)
        self.assertEqual(set(['x', 'y']), flow.provides)

    def test_retry_in_unordered_flow_no_requirements_no_provides(self):
        flow = uf.Flow('uf', retry.AlwaysRevert('rt'))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_retry_in_unordered_flow_with_requirements(self):
        flow = uf.Flow('uf', retry.AlwaysRevert('rt', requires=['x', 'y']))
        self.assertEqual(set(['x', 'y']), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_retry_in_unordered_flow_with_provides(self):
        flow = uf.Flow('uf', retry.AlwaysRevert('rt', provides=['x', 'y']))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(['x', 'y']), flow.provides)

    def test_retry_in_unordered_flow_requires_and_provides(self):
        flow = uf.Flow('uf', retry.AlwaysRevert('rt',
                                                requires=['x', 'y'],
                                                provides=['a', 'b']))
        self.assertEqual(set(['x', 'y']), flow.requires)
        self.assertEqual(set(['a', 'b']), flow.provides)

    def test_retry_in_graph_flow_no_requirements_no_provides(self):
        flow = gf.Flow('gf', retry.AlwaysRevert('rt'))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_retry_in_graph_flow_with_requirements(self):
        flow = gf.Flow('gf', retry.AlwaysRevert('rt', requires=['x', 'y']))
        self.assertEqual(set(['x', 'y']), flow.requires)
        self.assertEqual(set(), flow.provides)

    def test_retry_in_graph_flow_with_provides(self):
        flow = gf.Flow('gf', retry.AlwaysRevert('rt', provides=['x', 'y']))
        self.assertEqual(set(), flow.requires)
        self.assertEqual(set(['x', 'y']), flow.provides)

    def test_retry_in_graph_flow_requires_and_provides(self):
        flow = gf.Flow('gf', retry.AlwaysRevert('rt',
                                                requires=['x', 'y'],
                                                provides=['a', 'b']))
        self.assertEqual(set(['x', 'y']), flow.requires)
        self.assertEqual(set(['a', 'b']), flow.provides)

    def test_linear_flow_retry_and_task(self):
        flow = lf.Flow('lf', retry.AlwaysRevert('rt',
                                                requires=['x', 'y'],
                                                provides=['a', 'b']))
        flow.add(utils.TaskMultiArgOneReturn(rebind=['a', 'x', 'c'],
                                             provides=['z']))

        self.assertEqual(set(['x', 'y', 'c']), flow.requires)
        self.assertEqual(set(['a', 'b', 'z']), flow.provides)

    def test_unordered_flow_retry_and_task(self):
        flow = uf.Flow('uf', retry.AlwaysRevert('rt',
                                                requires=['x', 'y'],
                                                provides=['a', 'b']))
        flow.add(utils.TaskMultiArgOneReturn(rebind=['a', 'x', 'c'],
                                             provides=['z']))

        self.assertEqual(set(['x', 'y', 'c']), flow.requires)
        self.assertEqual(set(['a', 'b', 'z']), flow.provides)

    def test_unordered_flow_retry_and_task_same_requires_provides(self):
        flow = uf.Flow('uf', retry.AlwaysRevert('rt', requires=['x']))
        flow.add(utils.TaskOneReturn(provides=['x']))
        self.assertEqual(set(['x']), flow.requires)
        self.assertEqual(set(['x']), flow.provides)

    def test_unordered_flow_retry_and_task_provide_same_value(self):
        flow = uf.Flow('uf', retry.AlwaysRevert('rt', provides=['x']))
        flow.add(utils.TaskOneReturn('t1', provides=['x']))
        self.assertEqual(set(['x']), flow.provides)

    def test_unordered_flow_retry_two_tasks_provide_same_value(self):
        flow = uf.Flow('uf', retry.AlwaysRevert('rt', provides=['y']))
        flow.add(utils.TaskOneReturn('t1', provides=['x']),
                 utils.TaskOneReturn('t2', provides=['x']))
        self.assertEqual(set(['x', 'y']), flow.provides)

    def test_graph_flow_retry_and_task(self):
        flow = gf.Flow('gf', retry.AlwaysRevert('rt',
                                                requires=['x', 'y'],
                                                provides=['a', 'b']))
        flow.add(utils.TaskMultiArgOneReturn(rebind=['a', 'x', 'c'],
                                             provides=['z']))

        self.assertEqual(set(['x', 'y', 'c']), flow.requires)
        self.assertEqual(set(['a', 'b', 'z']), flow.provides)

    def test_graph_flow_retry_and_task_dependency_provide_require(self):
        flow = gf.Flow('gf', retry.AlwaysRevert('rt', requires=['x']))
        flow.add(utils.TaskOneReturn(provides=['x']))
        self.assertEqual(set(['x']), flow.provides)
        self.assertEqual(set(['x']), flow.requires)

    def test_graph_flow_retry_and_task_provide_same_value(self):
        flow = gf.Flow('gf', retry.AlwaysRevert('rt', provides=['x']))
        flow.add(utils.TaskOneReturn('t1', provides=['x']))
        self.assertEqual(set(['x']), flow.provides)

    def test_builtin_retry_args(self):

        class FullArgsRetry(retry.AlwaysRevert):
            def execute(self, history, **kwargs):
                pass

            def revert(self, history, **kwargs):
                pass

        flow = lf.Flow('lf', retry=FullArgsRetry(requires='a'))
        self.assertEqual(set(['a']), flow.requires)
