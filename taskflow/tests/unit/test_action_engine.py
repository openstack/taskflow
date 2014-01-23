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

import contextlib
import networkx
import testtools

from concurrent import futures

from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf

import taskflow.engines

from taskflow.engines.action_engine import engine as eng
from taskflow import exceptions as exc
from taskflow.persistence import logbook
from taskflow import states
from taskflow import task
from taskflow import test
from taskflow.tests import utils

from taskflow.utils import eventlet_utils as eu
from taskflow.utils import misc
from taskflow.utils import persistence_utils as p_utils


class EngineTaskTest(utils.EngineTestBase):

    def test_run_task_as_flow(self):
        flow = utils.SaveOrderTask(name='task1')
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(self.values, ['task1'])

    @staticmethod
    def _callback(state, values, details):
        name = details.get('task_name', '<unknown>')
        values.append('%s %s' % (name, state))

    @staticmethod
    def _flow_callback(state, values, details):
        values.append('flow %s' % state)

    def test_run_task_with_notifications(self):
        flow = utils.SaveOrderTask(name='task1')
        engine = self._make_engine(flow)
        engine.notifier.register('*', self._flow_callback,
                                 kwargs={'values': self.values})
        engine.task_notifier.register('*', self._callback,
                                      kwargs={'values': self.values})
        engine.run()
        self.assertEqual(self.values,
                         ['flow RUNNING',
                          'task1 RUNNING',
                          'task1',
                          'task1 SUCCESS',
                          'flow SUCCESS'])

    def test_failing_task_with_notifications(self):
        flow = utils.FailingTask('fail')
        engine = self._make_engine(flow)
        engine.notifier.register('*', self._flow_callback,
                                 kwargs={'values': self.values})
        engine.task_notifier.register('*', self._callback,
                                      kwargs={'values': self.values})
        expected = ['flow RUNNING',
                    'fail RUNNING',
                    'fail FAILURE',
                    'flow FAILURE',
                    'flow REVERTING',
                    'fail REVERTING',
                    'fail reverted(Failure: RuntimeError: Woot!)',
                    'fail REVERTED',
                    'flow REVERTED']
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(self.values, expected)
        self.assertEqual(engine.storage.get_flow_state(), states.REVERTED)

        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        now_expected = expected + ['fail PENDING', 'flow PENDING'] + expected
        self.assertEqual(self.values, now_expected)
        self.assertEqual(engine.storage.get_flow_state(), states.REVERTED)

    def test_invalid_flow_raises(self):

        def compile_bad(value):
            engine = self._make_engine(value)
            engine.compile()

        value = 'i am string, not task/flow, sorry'
        err = self.assertRaises(TypeError, compile_bad, value)
        self.assertIn(value, str(err))

    def test_invalid_flow_raises_from_run(self):

        def run_bad(value):
            engine = self._make_engine(value)
            engine.run()

        value = 'i am string, not task/flow, sorry'
        err = self.assertRaises(TypeError, run_bad, value)
        self.assertIn(value, str(err))

    def test_nasty_failing_task_exception_reraised(self):
        flow = utils.NastyFailingTask()
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Gotcha', engine.run)


class EngineLinearFlowTest(utils.EngineTestBase):

    def test_run_empty_flow(self):
        flow = lf.Flow('flow-1')
        engine = self._make_engine(flow)
        self.assertRaises(exc.EmptyFlow, engine.run)

    def test_sequential_flow_one_task(self):
        flow = lf.Flow('flow-1').add(
            utils.SaveOrderTask(name='task1')
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1'])

    def test_sequential_flow_two_tasks(self):
        flow = lf.Flow('flow-2').add(
            utils.SaveOrderTask(name='task1'),
            utils.SaveOrderTask(name='task2')
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1', 'task2'])
        self.assertEqual(len(flow), 2)

    def test_revert_removes_data(self):
        flow = lf.Flow('revert-removes').add(
            utils.TaskOneReturn(provides='one'),
            utils.TaskMultiReturn(provides=('a', 'b', 'c')),
            utils.FailingTask(name='fail')
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(engine.storage.fetch_all(), {})

    def test_sequential_flow_nested_blocks(self):
        flow = lf.Flow('nested-1').add(
            utils.SaveOrderTask('task1'),
            lf.Flow('inner-1').add(
                utils.SaveOrderTask('task2')
            )
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1', 'task2'])

    def test_revert_exception_is_reraised(self):
        flow = lf.Flow('revert-1').add(
            utils.NastyTask(),
            utils.FailingTask(name='fail')
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Gotcha', engine.run)

    def test_revert_not_run_task_is_not_reverted(self):
        flow = lf.Flow('revert-not-run').add(
            utils.FailingTask('fail'),
            utils.NeverRunningTask(),
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(
            self.values,
            ['fail reverted(Failure: RuntimeError: Woot!)'])

    def test_correctly_reverts_children(self):
        flow = lf.Flow('root-1').add(
            utils.SaveOrderTask('task1'),
            lf.Flow('child-1').add(
                utils.SaveOrderTask('task2'),
                utils.FailingTask('fail')
            )
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(
            self.values,
            ['task1', 'task2',
             'fail reverted(Failure: RuntimeError: Woot!)',
             'task2 reverted(5)', 'task1 reverted(5)'])

    def test_flow_failures_are_passed_to_revert(self):
        class CheckingTask(task.Task):
            def execute(m_self):
                return 'RESULT'

            def revert(m_self, result, flow_failures):
                self.assertEqual(result, 'RESULT')
                self.assertEqual(list(flow_failures.keys()), ['fail1'])
                fail = flow_failures['fail1']
                self.assertIsInstance(fail, misc.Failure)
                self.assertEqual(str(fail), 'Failure: RuntimeError: Woot!')

        flow = lf.Flow('test').add(
            CheckingTask(),
            utils.FailingTask('fail1')
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)


class EngineParallelFlowTest(utils.EngineTestBase):

    def test_run_empty_flow(self):
        flow = uf.Flow('p-1')
        engine = self._make_engine(flow)
        self.assertRaises(exc.EmptyFlow, engine.run)

    def test_parallel_flow_one_task(self):
        flow = uf.Flow('p-1').add(
            utils.SaveOrderTask(name='task1')
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1'])

    def test_parallel_flow_two_tasks(self):
        flow = uf.Flow('p-2').add(
            utils.SaveOrderTask(name='task1'),
            utils.SaveOrderTask(name='task2')
        )
        self._make_engine(flow).run()

        result = set(self.values)
        self.assertEqual(result, set(['task1', 'task2']))
        self.assertEqual(len(flow), 2)

    def test_parallel_revert(self):
        flow = uf.Flow('p-r-3').add(
            utils.TaskNoRequiresNoReturns(name='task1'),
            utils.FailingTask(name='fail'),
            utils.TaskNoRequiresNoReturns(name='task2')
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        self.assertIn('fail reverted(Failure: RuntimeError: Woot!)',
                      self.values)

    def test_parallel_revert_exception_is_reraised(self):
        # NOTE(imelnikov): if we put NastyTask and FailingTask
        # into the same unordered flow, it is not guaranteed
        # that NastyTask execution would be attempted before
        # FailingTask fails.
        flow = lf.Flow('p-r-r-l').add(
            uf.Flow('p-r-r').add(
                utils.TaskNoRequiresNoReturns(name='task1'),
                utils.NastyTask()
            ),
            utils.FailingTask()
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Gotcha', engine.run)

    def test_sequential_flow_two_tasks_with_resumption(self):
        flow = lf.Flow('lf-2-r').add(
            utils.SaveOrderTask(name='task1', provides='x1'),
            utils.SaveOrderTask(name='task2', provides='x2')
        )

        # Create FlowDetail as if we already run task1
        _lb, fd = p_utils.temporary_flow_detail(self.backend)
        td = logbook.TaskDetail(name='task1', uuid='42')
        td.state = states.SUCCESS
        td.results = 17
        fd.add(td)

        with contextlib.closing(self.backend.get_connection()) as conn:
            fd.update(conn.update_flow_details(fd))
            td.update(conn.update_task_details(td))

        engine = self._make_engine(flow, fd)
        engine.run()
        self.assertEqual(self.values, ['task2'])
        self.assertEqual(engine.storage.fetch_all(),
                         {'x1': 17, 'x2': 5})


class EngineLinearAndUnorderedExceptionsTest(utils.EngineTestBase):

    def test_revert_ok_for_unordered_in_linear(self):
        flow = lf.Flow('p-root').add(
            utils.SaveOrderTask(name='task1'),
            utils.SaveOrderTask(name='task2'),
            uf.Flow('p-inner').add(
                utils.SaveOrderTask(name='task3'),
                utils.FailingTask('fail')
            )
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)

        # NOTE(imelnikov): we don't know if task 3 was run, but if it was,
        # it should have been reverted in correct order.
        possible_values_no_task3 = [
            'task1', 'task2',
            'fail reverted(Failure: RuntimeError: Woot!)',
            'task2 reverted(5)', 'task1 reverted(5)'
        ]
        self.assertIsSuperAndSubsequence(self.values,
                                         possible_values_no_task3)
        if 'task3' in self.values:
            possible_values_task3 = [
                'task1', 'task2', 'task3',
                'task3 reverted(5)', 'task2 reverted(5)', 'task1 reverted(5)'
            ]
            self.assertIsSuperAndSubsequence(self.values,
                                             possible_values_task3)

    def test_revert_raises_for_unordered_in_linear(self):
        flow = lf.Flow('p-root').add(
            utils.SaveOrderTask(name='task1'),
            utils.SaveOrderTask(name='task2'),
            uf.Flow('p-inner').add(
                utils.SaveOrderTask(name='task3'),
                utils.NastyFailingTask()
            )
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Gotcha', engine.run)

        # NOTE(imelnikov): we don't know if task 3 was run, but if it was,
        # it should have been reverted in correct order.
        possible_values = ['task1', 'task2', 'task3',
                           'task3 reverted(5)']
        self.assertIsSuperAndSubsequence(possible_values, self.values)
        possible_values_no_task3 = ['task1', 'task2']
        self.assertIsSuperAndSubsequence(self.values,
                                         possible_values_no_task3)

    def test_revert_ok_for_linear_in_unordered(self):
        flow = uf.Flow('p-root').add(
            utils.SaveOrderTask(name='task1'),
            lf.Flow('p-inner').add(
                utils.SaveOrderTask(name='task2'),
                utils.FailingTask('fail')
            )
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        self.assertIn('fail reverted(Failure: RuntimeError: Woot!)',
                      self.values)

        # NOTE(imelnikov): if task1 was run, it should have been reverted.
        if 'task1' in self.values:
            task1_story = ['task1', 'task1 reverted(5)']
            self.assertIsSuperAndSubsequence(self.values, task1_story)
        # NOTE(imelnikov): task2 should have been run and reverted
        task2_story = ['task2', 'task2 reverted(5)']
        self.assertIsSuperAndSubsequence(self.values, task2_story)

    def test_revert_raises_for_linear_in_unordered(self):
        flow = uf.Flow('p-root').add(
            utils.SaveOrderTask(name='task1'),
            lf.Flow('p-inner').add(
                utils.SaveOrderTask(name='task2'),
                utils.NastyFailingTask()
            )
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Gotcha', engine.run)
        self.assertNotIn('task2 reverted(5)', self.values)


class EngineGraphFlowTest(utils.EngineTestBase):

    def test_run_empty_flow(self):
        flow = gf.Flow('g-1')
        engine = self._make_engine(flow)
        self.assertRaises(exc.EmptyFlow, engine.run)

    def test_run_nested_empty_flows(self):
        flow = gf.Flow('g-1').add(lf.Flow('l-1'),
                                  gf.Flow('g-2'))
        engine = self._make_engine(flow)
        self.assertRaises(exc.EmptyFlow, engine.run)

    def test_graph_flow_one_task(self):
        flow = gf.Flow('g-1').add(
            utils.SaveOrderTask(name='task1')
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1'])

    def test_graph_flow_two_independent_tasks(self):
        flow = gf.Flow('g-2').add(
            utils.SaveOrderTask(name='task1'),
            utils.SaveOrderTask(name='task2')
        )
        self._make_engine(flow).run()
        self.assertEqual(set(self.values), set(['task1', 'task2']))
        self.assertEqual(len(flow), 2)

    def test_graph_flow_two_tasks(self):
        flow = gf.Flow('g-1-1').add(
            utils.SaveOrderTask(name='task2', requires=['a']),
            utils.SaveOrderTask(name='task1', provides='a')
        )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1', 'task2'])

    def test_graph_flow_four_tasks_added_separately(self):
        flow = (gf.Flow('g-4')
                .add(utils.SaveOrderTask(name='task4',
                                         provides='d', requires=['c']))
                .add(utils.SaveOrderTask(name='task2',
                                         provides='b', requires=['a']))
                .add(utils.SaveOrderTask(name='task3',
                                         provides='c', requires=['b']))
                .add(utils.SaveOrderTask(name='task1',
                                         provides='a'))
                )
        self._make_engine(flow).run()
        self.assertEqual(self.values, ['task1', 'task2', 'task3', 'task4'])

    def test_graph_flow_four_tasks_revert(self):
        flow = gf.Flow('g-4-failing').add(
            utils.SaveOrderTask(name='task4',
                                provides='d', requires=['c']),
            utils.SaveOrderTask(name='task2',
                                provides='b', requires=['a']),
            utils.FailingTask(name='task3',
                              provides='c', requires=['b']),
            utils.SaveOrderTask(name='task1', provides='a'))

        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(
            self.values,
            ['task1', 'task2',
             'task3 reverted(Failure: RuntimeError: Woot!)',
             'task2 reverted(5)', 'task1 reverted(5)'])
        self.assertEqual(engine.storage.get_flow_state(), states.REVERTED)

    def test_graph_flow_four_tasks_revert_failure(self):
        flow = gf.Flow('g-3-nasty').add(
            utils.NastyTask(name='task2', provides='b', requires=['a']),
            utils.FailingTask(name='task3', requires=['b']),
            utils.SaveOrderTask(name='task1', provides='a'))

        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Gotcha', engine.run)
        self.assertEqual(engine.storage.get_flow_state(), states.FAILURE)

    def test_graph_flow_with_multireturn_and_multiargs_tasks(self):
        flow = gf.Flow('g-3-multi').add(
            utils.TaskMultiArgOneReturn(name='task1',
                                        rebind=['a', 'b', 'y'], provides='z'),
            utils.TaskMultiReturn(name='task2', provides=['a', 'b', 'c']),
            utils.TaskMultiArgOneReturn(name='task3',
                                        rebind=['c', 'b', 'x'], provides='y'))

        engine = self._make_engine(flow)
        engine.storage.inject({'x': 30})
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {
            'a': 1,
            'b': 3,
            'c': 5,
            'x': 30,
            'y': 38,
            'z': 42
        })

    def test_task_graph_property(self):
        flow = gf.Flow('test').add(
            utils.TaskNoRequiresNoReturns(name='task1'),
            utils.TaskNoRequiresNoReturns(name='task2'))

        engine = self._make_engine(flow)
        graph = engine.execution_graph
        self.assertIsInstance(graph, networkx.DiGraph)

    def test_task_graph_property_for_one_task(self):
        flow = utils.TaskNoRequiresNoReturns(name='task1')

        engine = self._make_engine(flow)
        graph = engine.execution_graph
        self.assertIsInstance(graph, networkx.DiGraph)


class SingleThreadedEngineTest(EngineTaskTest,
                               EngineLinearFlowTest,
                               EngineParallelFlowTest,
                               EngineLinearAndUnorderedExceptionsTest,
                               EngineGraphFlowTest,
                               test.TestCase):
    def _make_engine(self, flow, flow_detail=None):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine_conf='serial',
                                     backend=self.backend)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.SingleThreadedActionEngine)

    def test_singlethreaded_is_the_default(self):
        engine = taskflow.engines.load(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.SingleThreadedActionEngine)


class MultiThreadedEngineTest(EngineTaskTest,
                              EngineLinearFlowTest,
                              EngineParallelFlowTest,
                              EngineLinearAndUnorderedExceptionsTest,
                              EngineGraphFlowTest,
                              test.TestCase):
    def _make_engine(self, flow, flow_detail=None, executor=None):
        engine_conf = dict(engine='parallel',
                           executor=executor)
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     engine_conf=engine_conf,
                                     backend=self.backend)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.MultiThreadedActionEngine)
        self.assertIs(engine._executor, None)

    def test_using_common_executor(self):
        flow = utils.TaskNoRequiresNoReturns(name='task1')
        executor = futures.ThreadPoolExecutor(2)
        try:
            e1 = self._make_engine(flow, executor=executor)
            e2 = self._make_engine(flow, executor=executor)
            self.assertIs(e1._executor, e2._executor)
        finally:
            executor.shutdown(wait=True)


@testtools.skipIf(not eu.EVENTLET_AVAILABLE, 'eventlet is not available')
class ParallelEngineWithEventletTest(EngineTaskTest,
                                     EngineLinearFlowTest,
                                     EngineParallelFlowTest,
                                     EngineLinearAndUnorderedExceptionsTest,
                                     EngineGraphFlowTest,
                                     test.TestCase):

    def _make_engine(self, flow, flow_detail=None, executor=None):
        if executor is None:
            executor = eu.GreenExecutor()
        engine_conf = dict(engine='parallel',
                           executor=executor)
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     engine_conf=engine_conf,
                                     backend=self.backend)
