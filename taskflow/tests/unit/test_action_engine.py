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
from taskflow import exceptions
from taskflow.persistence import taskdetail
from taskflow import states
from taskflow import storage
from taskflow import task
from taskflow import test

from taskflow.engines.action_engine import engine as eng


class TestTask(task.Task):

    def __init__(self, values=None, name=None):
        super(TestTask, self).__init__(name)
        if values is None:
            self.values = []
        else:
            self.values = values

    def execute(self, **kwargs):
        self.values.append(self.name)
        return 5

    def revert(self, **kwargs):
        self.values.append(self.name + ' reverted(%s)'
                           % kwargs.get('result'))


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


class MultiReturnTask(task.Task):
    def execute(self, **kwargs):
        return 12, 2, 1


class MultiargsTask(task.Task):
    def execute(self, a, b, c):
        return a + b + c


class EngineTestBase(object):
    def setUp(self):
        super(EngineTestBase, self).setUp()
        self.values = []

    def _make_engine(self, _flow, _flow_detail=None):
        raise NotImplementedError()


class EngineTaskTest(EngineTestBase):

    def test_run_task_as_flow(self):
        flow = blocks.Task(TestTask(self.values, name='task1'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEquals(self.values, ['task1'])

    @staticmethod
    def _callback(state, values, details):
        name = details.get('task_name', '<unknown>')
        values.append('%s %s' % (name, state))

    @staticmethod
    def _flow_callback(state, values, details):
        values.append('flow %s' % state)

    def test_run_task_with_notifications(self):
        flow = blocks.Task(TestTask(self.values, name='task1'))
        engine = self._make_engine(flow)
        engine.notifier.register('*', self._flow_callback,
                                 kwargs={'values': self.values})
        engine.task_notifier.register('*', self._callback,
                                      kwargs={'values': self.values})
        engine.run()
        self.assertEquals(self.values,
                          ['flow RUNNING',
                           'task1 RUNNING',
                           'task1',
                           'task1 SUCCESS',
                           'flow SUCCESS'])

    def test_failing_task_with_notifications(self):
        flow = blocks.Task(FailingTask(self.values, 'fail'))
        engine = self._make_engine(flow)
        engine.notifier.register('*', self._flow_callback,
                                 kwargs={'values': self.values})
        engine.task_notifier.register('*', self._callback,
                                      kwargs={'values': self.values})
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEquals(self.values,
                          ['flow RUNNING',
                           'fail RUNNING',
                           'fail FAILURE',
                           'flow REVERTING',
                           'fail REVERTING',
                           'fail reverted(Failure: RuntimeError: Woot!)',
                           'fail PENDING',
                           'flow REVERTED'])

    def test_invalid_block_raises(self):
        value = 'i am string, not block, sorry'
        flow = blocks.LinearFlow().add(value)
        with self.assertRaises(ValueError) as err:
            self._make_engine(flow)
        self.assertIn(value, str(err.exception))

    def test_save_as(self):
        flow = blocks.Task(TestTask(self.values, name='task1'),
                           save_as='first_data')
        engine = self._make_engine(flow)
        engine.run()
        self.assertEquals(self.values, ['task1'])
        self.assertEquals(engine.storage.fetch_all(), {'first_data': 5})

    def test_save_all_in_one(self):
        flow = blocks.Task(MultiReturnTask, save_as='all_data')
        engine = self._make_engine(flow)
        engine.run()
        self.assertEquals(engine.storage.fetch_all(),
                          {'all_data': (12, 2, 1)})

    def test_save_several_values(self):
        flow = blocks.Task(MultiReturnTask,
                           save_as=('badger', 'mushroom', 'snake'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEquals(engine.storage.fetch_all(), {
            'badger': 12,
            'mushroom': 2,
            'snake': 1
        })

    def test_bad_save_as_value(self):
        with self.assertRaises(TypeError):
            blocks.Task(TestTask(name='task1'),
                        save_as=object())

    def test_arguments_passing(self):
        flow = blocks.Task(MultiargsTask, save_as='result')
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'c': 9, 'x': 17})
        engine.run()
        self.assertEquals(engine.storage.fetch_all(), {
            'a': 1, 'b': 4, 'c': 9, 'x': 17,
            'result': 14,
        })

    def test_arguments_missing(self):
        flow = blocks.Task(MultiargsTask, save_as='result')
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'x': 17})
        with self.assertRaisesRegexp(exceptions.NotFound,
                                     "^Name 'c' is not mapped"):
            engine.run()

    def test_partial_arguments_mapping(self):
        flow = blocks.Task(MultiargsTask(name='task1'),
                           save_as='result',
                           rebind_args={'b': 'x'})
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'c': 9, 'x': 17})
        engine.run()
        self.assertEquals(engine.storage.fetch_all(), {
            'a': 1, 'b': 4, 'c': 9, 'x': 17,
            'result': 27,
        })

    def test_all_arguments_mapping(self):
        flow = blocks.Task(MultiargsTask(name='task1'),
                           save_as='result',
                           rebind_args=['x', 'y', 'z'])
        engine = self._make_engine(flow)
        engine.storage.inject({
            'a': 1, 'b': 2, 'c': 3, 'x': 4, 'y': 5, 'z': 6
        })
        engine.run()
        self.assertEquals(engine.storage.fetch_all(), {
            'a': 1, 'b': 2, 'c': 3, 'x': 4, 'y': 5, 'z': 6,
            'result': 15,
        })

    def test_not_enough_arguments_for_task(self):
        msg = '^Task task1 takes 3 positional arguments'
        with self.assertRaisesRegexp(ValueError, msg):
            blocks.Task(MultiargsTask(name='task1'),
                        save_as='result',
                        rebind_args=['x', 'y'])

    def test_invalid_argument_name_map(self):
        flow = blocks.Task(MultiargsTask(name='task1'),
                           save_as='result',
                           rebind_args={'b': 'z'})
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'c': 9, 'x': 17})
        with self.assertRaisesRegexp(exceptions.NotFound,
                                     "Name 'z' is not mapped"):
            engine.run()

    def test_invalid_argument_name_list(self):
        flow = blocks.Task(MultiargsTask(name='task1'),
                           save_as='result',
                           rebind_args=['a', 'z', 'b'])
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'c': 9, 'x': 17})
        with self.assertRaisesRegexp(exceptions.NotFound,
                                     "Name 'z' is not mapped"):
            engine.run()

    def test_bad_rebind_args_value(self):
        with self.assertRaises(TypeError):
            blocks.Task(TestTask(name='task1'),
                        rebind_args=object())


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

    def test_revert_removes_data(self):
        flow = blocks.LinearFlow().add(
            blocks.Task(TestTask, save_as='one'),
            blocks.Task(MultiReturnTask, save_as=('a', 'b', 'c')),
            blocks.Task(FailingTask(name='fail'))
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEquals(engine.storage.fetch_all(), {})

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
            blocks.Task(FailingTask(name='fail'))
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Gotcha'):
            engine.run()

    def test_revert_not_run_task_is_not_reverted(self):
        flow = blocks.LinearFlow().add(
            blocks.Task(FailingTask(self.values, 'fail')),
            blocks.Task(NeverRunningTask)
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEquals(self.values,
                          ['fail reverted(Failure: RuntimeError: Woot!)'])

    def test_correctly_reverts_children(self):
        flow = blocks.LinearFlow().add(
            blocks.Task(TestTask(self.values, 'task1')),
            blocks.LinearFlow().add(
                blocks.Task(TestTask(self.values, 'task2')),
                blocks.Task(FailingTask(self.values, 'fail'))
            )
        )
        engine = self._make_engine(flow)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEquals(self.values,
                          ['task1', 'task2',
                           'fail reverted(Failure: RuntimeError: Woot!)',
                           'task2 reverted(5)', 'task1 reverted(5)'])

    def test_sequential_flow_two_tasks_with_resumption(self):
        flow = blocks.LinearFlow().add(
            blocks.Task(TestTask(self.values, name='task1'), save_as='x1'),
            blocks.Task(TestTask(self.values, name='task2'), save_as='x2')
        )

        # Create FlowDetail as if we already run task1
        fd = storage.temporary_flow_detail()
        td = taskdetail.TaskDetail(name='task1', uuid='42')
        td.state = states.SUCCESS
        td.results = 17
        fd.add(td)
        fd.save()
        td.save()

        engine = self._make_engine(flow, fd)
        engine.run()
        self.assertEquals(self.values, ['task2'])
        self.assertEquals(engine.storage.fetch_all(),
                          {'x1': 17, 'x2': 5})


class SingleThreadedEngineTest(EngineTaskTest,
                               EngineLinearFlowTest,
                               test.TestCase):
    def _make_engine(self, flow, flow_detail=None):
        return eng.SingleThreadedActionEngine(flow, flow_detail=flow_detail)
