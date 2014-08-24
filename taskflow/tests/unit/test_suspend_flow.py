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

import testtools

import taskflow.engines
from taskflow import exceptions as exc
from taskflow.listeners import base as lbase
from taskflow.patterns import linear_flow as lf
from taskflow import states
from taskflow import test
from taskflow.tests import utils
from taskflow.types import futures
from taskflow.utils import async_utils as au


class SuspendingListener(lbase.ListenerBase):

    def __init__(self, engine, task_name, task_state):
        super(SuspendingListener, self).__init__(
            engine, task_listen_for=(task_state,))
        self._task_name = task_name

    def _task_receiver(self, state, details):
        if details['task_name'] == self._task_name:
            self._engine.suspend()


class SuspendFlowTest(utils.EngineTestBase):

    def test_suspend_one_task(self):
        flow = utils.SaveOrderTask('a')
        engine = self._make_engine(flow)
        with SuspendingListener(engine, task_name='b',
                                task_state=states.SUCCESS):
            engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUCCESS)
        self.assertEqual(self.values, ['a'])
        engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUCCESS)
        self.assertEqual(self.values, ['a'])

    def test_suspend_linear_flow(self):
        flow = lf.Flow('linear').add(
            utils.SaveOrderTask('a'),
            utils.SaveOrderTask('b'),
            utils.SaveOrderTask('c')
        )
        engine = self._make_engine(flow)
        with SuspendingListener(engine, task_name='b',
                                task_state=states.SUCCESS):
            engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUSPENDED)
        self.assertEqual(self.values, ['a', 'b'])
        engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUCCESS)
        self.assertEqual(self.values, ['a', 'b', 'c'])

    def test_suspend_linear_flow_on_revert(self):
        flow = lf.Flow('linear').add(
            utils.SaveOrderTask('a'),
            utils.SaveOrderTask('b'),
            utils.FailingTask('c')
        )
        engine = self._make_engine(flow)
        with SuspendingListener(engine, task_name='b',
                                task_state=states.REVERTED):
            engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUSPENDED)
        self.assertEqual(
            self.values,
            ['a', 'b',
             'c reverted(Failure: RuntimeError: Woot!)',
             'b reverted(5)'])
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
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
            utils.SaveOrderTask('a'),
            utils.SaveOrderTask('b'),
            utils.FailingTask('c')
        )
        engine = self._make_engine(flow)

        with SuspendingListener(engine, task_name='b',
                                task_state=states.REVERTED):
            engine.run()

        # pretend we are resuming
        engine2 = self._make_engine(flow, engine.storage._flowdetail)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine2.run)
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
            utils.SaveOrderTask('a'),
            utils.SaveOrderTask('b'),
            utils.FailingTask('c')
        )
        engine = self._make_engine(flow)

        with SuspendingListener(engine, task_name='b',
                                task_state=states.REVERTED):
            engine.run()

        expected_values = ['a', 'b',
                           'c reverted(Failure: RuntimeError: Woot!)',
                           'b reverted(5)']
        self.assertEqual(self.values, expected_values)

        # pretend we are resuming, but task 'c' gone when flow got updated
        flow2 = lf.Flow('linear').add(
            utils.SaveOrderTask('a'),
            utils.SaveOrderTask('b')
        )
        engine2 = self._make_engine(flow2, engine.storage._flowdetail)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine2.run)
        self.assertEqual(engine2.storage.get_flow_state(), states.REVERTED)
        expected_values.append('a reverted(5)')
        self.assertEqual(self.values, expected_values)

    def test_storage_is_rechecked(self):
        flow = lf.Flow('linear').add(
            utils.SaveOrderTask('b', requires=['foo']),
            utils.SaveOrderTask('c')
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'foo': 'bar'})
        with SuspendingListener(engine, task_name='b',
                                task_state=states.SUCCESS):
            engine.run()
        self.assertEqual(engine.storage.get_flow_state(), states.SUSPENDED)
        # uninject everything:
        engine.storage.save(engine.storage.injector_name,
                            {}, states.SUCCESS)
        self.assertRaises(exc.MissingDependencies, engine.run)


class SingleThreadedEngineTest(SuspendFlowTest,
                               test.TestCase):
    def _make_engine(self, flow, flow_detail=None):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine='serial',
                                     backend=self.backend)


class MultiThreadedEngineTest(SuspendFlowTest,
                              test.TestCase):
    def _make_engine(self, flow, flow_detail=None, executor=None):
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     engine='parallel',
                                     backend=self.backend,
                                     executor=executor)


@testtools.skipIf(not au.EVENTLET_AVAILABLE, 'eventlet is not available')
class ParallelEngineWithEventletTest(SuspendFlowTest,
                                     test.TestCase):

    def _make_engine(self, flow, flow_detail=None, executor=None):
        if executor is None:
            executor = futures.GreenThreadPoolExecutor()
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     engine='parallel',
                                     backend=self.backend,
                                     executor=executor)
