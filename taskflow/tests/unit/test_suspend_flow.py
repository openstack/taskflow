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

import time

from taskflow.patterns import linear_flow as lf

import taskflow.engines

from taskflow import exceptions as exc
from taskflow import states
from taskflow import task
from taskflow import test
from taskflow.tests import utils


class TestTask(task.Task):

    def __init__(self, values=None, name=None, sleep=None,
                 provides=None, rebind=None, requires=None):
        super(TestTask, self).__init__(name=name, provides=provides,
                                       rebind=rebind, requires=requires)
        if values is None:
            self.values = []
        else:
            self.values = values
        self._sleep = sleep

    def execute(self, **kwargs):
        self.update_progress(0.0)
        if self._sleep:
            time.sleep(self._sleep)
        self.values.append(self.name)
        self.update_progress(1.0)
        return 5

    def revert(self, **kwargs):
        self.update_progress(0)
        if self._sleep:
            time.sleep(self._sleep)
        self.values.append(self.name + ' reverted(%s)'
                           % kwargs.get('result'))
        self.update_progress(1.0)


class FailingTask(TestTask):

    def execute(self, **kwargs):
        self.update_progress(0)
        if self._sleep:
            time.sleep(self._sleep)
        self.update_progress(0.99)
        raise RuntimeError('Woot!')


class AutoSuspendingTask(TestTask):

    def execute(self, engine):
        result = super(AutoSuspendingTask, self).execute()
        engine.suspend()
        return result

    def revert(self, engine, result, flow_failures):
        super(AutoSuspendingTask, self).revert(**{'result': result})


class AutoSuspendingTaskOnRevert(TestTask):

    def execute(self, engine):
        return super(AutoSuspendingTaskOnRevert, self).execute()

    def revert(self, engine, result, flow_failures):
        super(AutoSuspendingTaskOnRevert, self).revert(**{'result': result})
        engine.suspend()


class SuspendFlowTest(utils.EngineTestBase):

    def test_suspend_one_task(self):
        flow = AutoSuspendingTask(self.values, 'a')
        engine = self._make_engine(flow)
        engine.storage.inject({'engine': engine})
        engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUCCESS)
        self.assertEqual(self.values, ['a'])
        engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUCCESS)
        self.assertEqual(self.values, ['a'])

    def test_suspend_linear_flow(self):
        flow = lf.Flow('linear').add(
            TestTask(self.values, 'a'),
            AutoSuspendingTask(self.values, 'b'),
            TestTask(self.values, 'c')
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'engine': engine})
        engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUSPENDED)
        self.assertEqual(self.values, ['a', 'b'])
        engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUCCESS)
        self.assertEqual(self.values, ['a', 'b', 'c'])

    def test_suspend_linear_flow_on_revert(self):
        flow = lf.Flow('linear').add(
            TestTask(self.values, 'a'),
            AutoSuspendingTaskOnRevert(self.values, 'b'),
            FailingTask(self.values, 'c')
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'engine': engine})
        engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUSPENDED)
        self.assertEqual(
            self.values,
            ['a', 'b',
             'c reverted(Failure: RuntimeError: Woot!)',
             'b reverted(5)'])
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.REVERTED)
        self.assertEqual(
            self.values,
            ['a',
             'b',
             'c reverted(Failure: RuntimeError: Woot!)',
             'b reverted(5)',
             'a reverted(5)'])

    def test_suspend_and_resume_linear_flow_on_revert(self):
        flow = lf.Flow('linear').add(
            TestTask(self.values, 'a'),
            AutoSuspendingTaskOnRevert(self.values, 'b'),
            FailingTask(self.values, 'c')
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'engine': engine})
        engine.run()

        # pretend we are resuming
        engine2 = self._make_engine(flow, engine.storage._flowdetail)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine2.run()
        self.assertEqual(engine2.storage.get_flow_state(), states.REVERTED)
        self.assertEqual(
            self.values,
            ['a',
             'b',
             'c reverted(Failure: RuntimeError: Woot!)',
             'b reverted(5)',
             'a reverted(5)'])

    def test_suspend_and_revert_even_if_task_is_gone(self):
        flow = lf.Flow('linear').add(
            TestTask(self.values, 'a'),
            AutoSuspendingTaskOnRevert(self.values, 'b'),
            FailingTask(self.values, 'c')
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'engine': engine})
        engine.run()

        # pretend we are resuming, but task 'c' gone when flow got updated
        flow2 = lf.Flow('linear').add(
            TestTask(self.values, 'a'),
            AutoSuspendingTaskOnRevert(self.values, 'b')
        )
        engine2 = self._make_engine(flow2, engine.storage._flowdetail)
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            engine2.run()
        self.assertEqual(engine2.storage.get_flow_state(), states.REVERTED)
        self.assertEqual(
            self.values,
            ['a',
             'b',
             'c reverted(Failure: RuntimeError: Woot!)',
             'b reverted(5)',
             'a reverted(5)'])

    def test_storage_is_rechecked(self):
        flow = lf.Flow('linear').add(
            AutoSuspendingTask(self.values, 'b'),
            TestTask(self.values, name='c')
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'engine': engine, 'boo': True})
        engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUSPENDED)
        # uninject engine
        engine.storage.save(
            engine.storage.get_uuid_by_name(engine.storage.injector_name),
            None,
            states.FAILURE)
        with self.assertRaises(exc.MissingDependencies):
            engine.run()


class SingleThreadedEngineTest(SuspendFlowTest,
                               test.TestCase):
    def _make_engine(self, flow, flow_detail=None):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine_conf='serial',
                                     backend=self.backend)


class MultiThreadedEngineTest(SuspendFlowTest,
                              test.TestCase):
    def _make_engine(self, flow, flow_detail=None, executor=None):
        engine_conf = dict(engine='parallel',
                           executor=executor)
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     engine_conf=engine_conf,
                                     backend=self.backend)
