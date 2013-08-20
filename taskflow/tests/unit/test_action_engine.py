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

from taskflow import blocks
from taskflow import task
from taskflow import test

from taskflow.engines.action_engine import engine as eng


class TestTask(task.Task):

    def __init__(self, values, name):
        super(TestTask, self).__init__(name)
        self.values = values

    def execute(self, **kwargs):
        self.values.append(self.name)
        return 5

    def revert(self, **kwargs):
        self.values.append(self.name + ' reverted')


class FailingTask(TestTask):
    def execute(self, **kwargs):
        raise RuntimeError('Woot!')


class NeverRunningTask(task.Task):
    def execute(self, **kwargs):
        assert False, 'This method should not be called'

    def revert(self, **kwargs):
        assert False, 'This method should not be called'


class NastyTask(task.Task):
    def execute(self, **kwargs):
        pass

    def revert(self, **kwargs):
        raise RuntimeError('Gotcha!')


class EngineTestBase(object):
    def setUp(self):
        super(EngineTestBase, self).setUp()
        self.values = []

    def _make_engine(self, _flow):
        raise NotImplementedError()


class EngineTaskTest(EngineTestBase):

    def test_run_task_as_flow(self):
        flow = blocks.Task(TestTask(self.values, name='task1'))
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['task1'])

    def test_invalid_block_raises(self):
        value = 'i am string, not block, sorry'
        flow = blocks.LinearFlow().add(value)
        with self.assertRaises(ValueError) as err:
            self._make_engine(flow)
        self.assertIn(value, str(err.exception))


class EngineLinearFlowTest(EngineTestBase):

    def test_sequential_flow_one_task(self):
        flow = blocks.LinearFlow().add(
            blocks.Task(TestTask(self.values, name='task1'))
        )
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['task1'])

    def test_sequential_flow_two_tasks(self):
        flow = blocks.LinearFlow().add(
            blocks.Task(TestTask(self.values, name='task1')),
            blocks.Task(TestTask(self.values, name='task2'))
        )
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['task1', 'task2'])

    def test_sequential_flow_nested_blocks(self):
        flow = blocks.LinearFlow().add(
            blocks.Task(TestTask(self.values, 'task1')),
            blocks.LinearFlow().add(
                blocks.Task(TestTask(self.values, 'task2'))
            )
        )
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['task1', 'task2'])

    def test_revert_exception_is_reraised(self):
        flow = blocks.LinearFlow().add(
            blocks.Task(NastyTask),
            blocks.Task(FailingTask(self.values, 'fail'))
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()

    def test_revert_not_run_task_is_not_reverted(self):
        flow = blocks.LinearFlow().add(
            blocks.Task(FailingTask(self.values, 'fail')),
            blocks.Task(NeverRunningTask)
        )
        self._make_engine(flow).run()
        self.assertEquals(self.values, ['fail reverted'])

    def test_correctly_reverts_children(self):
        flow = blocks.LinearFlow().add(
            blocks.Task(TestTask(self.values, 'task1')),
            blocks.LinearFlow().add(
                blocks.Task(TestTask(self.values, 'task2')),
                blocks.Task(FailingTask(self.values, 'fail'))
            )
        )
        engine = self._make_engine(flow)
        engine.run()
        self.assertEquals(self.values, ['task1', 'task2',
                                        'fail reverted',
                                        'task2 reverted', 'task1 reverted'])


class SingleThreadedEngineTest(EngineTaskTest,
                               EngineLinearFlowTest,
                               test.TestCase):
    def _make_engine(self, flow):
        return eng.SingleThreadedActionEngine(flow)
