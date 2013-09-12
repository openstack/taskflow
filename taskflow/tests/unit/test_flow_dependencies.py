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

from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf

from taskflow import exceptions
from taskflow import task
from taskflow import test


class TaskNoRequiresNoReturns(task.Task):

    def execute(self, **kwargs):
        pass

    def revert(self, **kwargs):
        pass


class TaskOneArg(task.Task):

    def execute(self, x, **kwargs):
        pass

    def revert(self, x, **kwargs):
        pass


class TaskMultiArg(task.Task):

    def execute(self, x, y, z, **kwargs):
        pass

    def revert(self, x, y, z, **kwargs):
        pass


class TaskOneReturn(task.Task):

    def execute(self, **kwargs):
        return 1

    def revert(self, **kwargs):
        pass


class TaskMultiReturn(task.Task):

    def execute(self, **kwargs):
        return 1, 3, 5

    def revert(self, **kwargs):
        pass


class TaskOneArgOneReturn(task.Task):

    def execute(self, x, **kwargs):
        return 1

    def revert(self, x, **kwargs):
        pass


class TaskMultiArgMultiReturn(task.Task):

    def execute(self, x, y, z, **kwargs):
        return 1, 3, 5

    def revert(self, x, y, z, **kwargs):
        pass


class FlowDependenciesTest(test.TestCase):

    def test_task_without_dependencies(self):
        flow = TaskNoRequiresNoReturns()
        self.assertEquals(flow.requires, set())
        self.assertEquals(flow.provides, set())

    def test_task_requires_default_values(self):
        flow = TaskMultiArg()
        self.assertEquals(flow.requires, set(['x', 'y', 'z']))
        self.assertEquals(flow.provides, set())

    def test_task_requires_rebinded_mapped(self):
        flow = TaskMultiArg(rebind={'x': 'a', 'y': 'b', 'z': 'c'})
        self.assertEquals(flow.requires, set(['a', 'b', 'c']))
        self.assertEquals(flow.provides, set())

    def test_task_requires_additional_values(self):
        flow = TaskMultiArg(requires=['a', 'b'])
        self.assertEquals(flow.requires, set(['a', 'b', 'x', 'y', 'z']))
        self.assertEquals(flow.provides, set())

    def test_task_provides_values(self):
        flow = TaskMultiReturn(provides=['a', 'b', 'c'])
        self.assertEquals(flow.requires, set())
        self.assertEquals(flow.provides, set(['a', 'b', 'c']))

    def test_task_provides_and_requires_values(self):
        flow = TaskMultiArgMultiReturn(provides=['a', 'b', 'c'])
        self.assertEquals(flow.requires, set(['x', 'y', 'z']))
        self.assertEquals(flow.provides, set(['a', 'b', 'c']))

    def test_linear_flow_without_dependencies(self):
        flow = lf.Flow('lf').add(
            TaskNoRequiresNoReturns('task1'),
            TaskNoRequiresNoReturns('task2'))
        self.assertEquals(flow.requires, set())
        self.assertEquals(flow.provides, set())

    def test_linear_flow_reuires_values(self):
        flow = lf.Flow('lf').add(
            TaskOneArg('task1'),
            TaskMultiArg('task2'))
        self.assertEquals(flow.requires, set(['x', 'y', 'z']))
        self.assertEquals(flow.provides, set())

    def test_linear_flow_reuires_rebind_values(self):
        flow = lf.Flow('lf').add(
            TaskOneArg('task1', rebind=['q']),
            TaskMultiArg('task2'))
        self.assertEquals(flow.requires, set(['x', 'y', 'z', 'q']))
        self.assertEquals(flow.provides, set())

    def test_linear_flow_provides_values(self):
        flow = lf.Flow('lf').add(
            TaskOneReturn('task1', provides='x'),
            TaskMultiReturn('task2', provides=['a', 'b', 'c']))
        self.assertEquals(flow.requires, set())
        self.assertEquals(flow.provides, set(['x', 'a', 'b', 'c']))

    def test_linear_flow_provides_out_of_order(self):
        with self.assertRaises(exceptions.InvariantViolationException):
            lf.Flow('lf').add(
                TaskOneArg('task2'),
                TaskOneReturn('task1', provides='x'))

    def test_linear_flow_provides_required_values(self):
        flow = lf.Flow('lf').add(
            TaskOneReturn('task1', provides='x'),
            TaskOneArg('task2'))
        self.assertEquals(flow.requires, set())
        self.assertEquals(flow.provides, set(['x']))

    def test_linear_flow_multi_provides_and_requires_values(self):
        flow = lf.Flow('lf').add(
            TaskMultiArgMultiReturn('task1',
                                    rebind=['a', 'b', 'c'],
                                    provides=['x', 'y', 'q']),
            TaskMultiArgMultiReturn('task2',
                                    provides=['i', 'j', 'k']))
        self.assertEquals(flow.requires, set(['a', 'b', 'c', 'z']))
        self.assertEquals(flow.provides, set(['x', 'y', 'q', 'i', 'j', 'k']))

    def test_linear_flow_self_requires(self):
        flow = lf.Flow('uf')
        with self.assertRaises(exceptions.InvariantViolationException):
            flow.add(TaskNoRequiresNoReturns(rebind=['x'], provides='x'))

    def test_unordered_flow_without_dependencies(self):
        flow = uf.Flow('uf').add(
            TaskNoRequiresNoReturns('task1'),
            TaskNoRequiresNoReturns('task2'))
        self.assertEquals(flow.requires, set())
        self.assertEquals(flow.provides, set())

    def test_unordered_flow_self_requires(self):
        flow = uf.Flow('uf')
        with self.assertRaises(exceptions.InvariantViolationException):
            flow.add(TaskNoRequiresNoReturns(rebind=['x'], provides='x'))

    def test_unordered_flow_reuires_values(self):
        flow = uf.Flow('uf').add(
            TaskOneArg('task1'),
            TaskMultiArg('task2'))
        self.assertEquals(flow.requires, set(['x', 'y', 'z']))
        self.assertEquals(flow.provides, set())

    def test_unordered_flow_reuires_rebind_values(self):
        flow = uf.Flow('uf').add(
            TaskOneArg('task1', rebind=['q']),
            TaskMultiArg('task2'))
        self.assertEquals(flow.requires, set(['x', 'y', 'z', 'q']))
        self.assertEquals(flow.provides, set())

    def test_unordered_flow_provides_values(self):
        flow = uf.Flow('uf').add(
            TaskOneReturn('task1', provides='x'),
            TaskMultiReturn('task2', provides=['a', 'b', 'c']))
        self.assertEquals(flow.requires, set())
        self.assertEquals(flow.provides, set(['x', 'a', 'b', 'c']))

    def test_unordered_flow_provides_required_values(self):
        with self.assertRaises(exceptions.InvariantViolationException):
            uf.Flow('uf').add(
                TaskOneReturn('task1', provides='x'),
                TaskOneArg('task2'))

    def test_unordered_flow_requires_provided_value_other_call(self):
        flow = uf.Flow('uf')
        flow.add(TaskOneReturn('task1', provides='x'))
        with self.assertRaises(exceptions.InvariantViolationException):
            flow.add(TaskOneArg('task2'))

    def test_unordered_flow_provides_required_value_other_call(self):
        flow = uf.Flow('uf')
        flow.add(TaskOneArg('task2'))
        with self.assertRaises(exceptions.InvariantViolationException):
            flow.add(TaskOneReturn('task1', provides='x'))

    def test_unordered_flow_multi_provides_and_requires_values(self):
        flow = uf.Flow('uf').add(
            TaskMultiArgMultiReturn('task1',
                                    rebind=['a', 'b', 'c'],
                                    provides=['d', 'e', 'f']),
            TaskMultiArgMultiReturn('task2',
                                    provides=['i', 'j', 'k']))
        self.assertEquals(flow.requires, set(['a', 'b', 'c', 'x', 'y', 'z']))
        self.assertEquals(flow.provides, set(['d', 'e', 'f', 'i', 'j', 'k']))

    def test_nested_flows_requirements(self):
        flow = uf.Flow('uf').add(
            lf.Flow('lf').add(
                TaskOneArgOneReturn('task1', rebind=['a'], provides=['x']),
                TaskOneArgOneReturn('task2', provides=['y'])),
            uf.Flow('uf').add(
                TaskOneArgOneReturn('task3', rebind=['b'], provides=['z']),
                TaskOneArgOneReturn('task4', rebind=['c'], provides=['q'])))
        self.assertEquals(flow.requires, set(['a', 'b', 'c']))
        self.assertEquals(flow.provides, set(['x', 'y', 'z', 'q']))
