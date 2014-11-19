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

import contextlib

import testtools

import taskflow.engines
from taskflow.engines.action_engine import engine as eng
from taskflow.engines.worker_based import engine as w_eng
from taskflow.engines.worker_based import worker as wkr
from taskflow import exceptions as exc
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow.persistence import logbook
from taskflow import states
from taskflow import task
from taskflow import test
from taskflow.tests import utils
from taskflow.types import failure
from taskflow.types import futures
from taskflow.types import graph as gr
from taskflow.utils import async_utils as au
from taskflow.utils import persistence_utils as p_utils
from taskflow.utils import threading_utils as tu


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
        utils.register_notifiers(engine, self.values)
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
        utils.register_notifiers(engine, self.values)
        expected = ['flow RUNNING',
                    'fail RUNNING',
                    'fail FAILURE',
                    'fail REVERTING',
                    'fail reverted(Failure: RuntimeError: Woot!)',
                    'fail REVERTED',
                    'flow REVERTED']
        self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(self.values, expected)
        self.assertEqual(engine.storage.get_flow_state(), states.REVERTED)

        self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
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
        self.assertFailuresRegexp(RuntimeError, '^Gotcha', engine.run)


class EngineLinearFlowTest(utils.EngineTestBase):

    def test_run_empty_flow(self):
        flow = lf.Flow('flow-1')
        engine = self._make_engine(flow)
        self.assertRaises(exc.Empty, engine.run)

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

    def test_sequential_flow_two_tasks_iter(self):
        flow = lf.Flow('flow-2').add(
            utils.SaveOrderTask(name='task1'),
            utils.SaveOrderTask(name='task2')
        )
        e = self._make_engine(flow)
        gathered_states = list(e.run_iter())
        self.assertTrue(len(gathered_states) > 0)
        self.assertEqual(self.values, ['task1', 'task2'])
        self.assertEqual(len(flow), 2)

    def test_sequential_flow_iter_suspend_resume(self):
        flow = lf.Flow('flow-2').add(
            utils.SaveOrderTask(name='task1'),
            utils.SaveOrderTask(name='task2')
        )
        _lb, fd = p_utils.temporary_flow_detail(self.backend)
        e = self._make_engine(flow, flow_detail=fd)
        it = e.run_iter()
        gathered_states = []
        suspend_it = None
        while True:
            try:
                s = it.send(suspend_it)
                gathered_states.append(s)
                if s == states.WAITING:
                    # Stop it before task2 runs/starts.
                    suspend_it = True
            except StopIteration:
                break
        self.assertTrue(len(gathered_states) > 0)
        self.assertEqual(self.values, ['task1'])
        self.assertEqual(states.SUSPENDED, e.storage.get_flow_state())

        # Attempt to resume it and see what runs now...
        #
        # NOTE(harlowja): Clear all the values, but don't reset the reference.
        while len(self.values):
            self.values.pop()
        gathered_states = list(e.run_iter())
        self.assertTrue(len(gathered_states) > 0)
        self.assertEqual(self.values, ['task2'])
        self.assertEqual(states.SUCCESS, e.storage.get_flow_state())

    def test_revert_removes_data(self):
        flow = lf.Flow('revert-removes').add(
            utils.TaskOneReturn(provides='one'),
            utils.TaskMultiReturn(provides=('a', 'b', 'c')),
            utils.FailingTask(name='fail')
        )
        engine = self._make_engine(flow)
        self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
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
        self.assertFailuresRegexp(RuntimeError, '^Gotcha', engine.run)

    def test_revert_not_run_task_is_not_reverted(self):
        flow = lf.Flow('revert-not-run').add(
            utils.FailingTask('fail'),
            utils.NeverRunningTask(),
        )
        engine = self._make_engine(flow)
        self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
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
        self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(
            self.values,
            ['task1', 'task2',
             'fail reverted(Failure: RuntimeError: Woot!)',
             'task2 reverted(5)', 'task1 reverted(5)'])


class EngineParallelFlowTest(utils.EngineTestBase):

    def test_run_empty_flow(self):
        flow = uf.Flow('p-1')
        engine = self._make_engine(flow)
        self.assertRaises(exc.Empty, engine.run)

    def test_parallel_flow_one_task(self):
        flow = uf.Flow('p-1').add(
            utils.SaveOrderTask(name='task1', provides='a')
        )
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(self.values, ['task1'])
        self.assertEqual(engine.storage.fetch_all(), {'a': 5})

    def test_parallel_flow_two_tasks(self):
        flow = uf.Flow('p-2').add(
            utils.SaveOrderTask(name='task1'),
            utils.SaveOrderTask(name='task2')
        )
        self._make_engine(flow).run()

        result = set(self.values)
        self.assertEqual(result, set(['task1', 'task2']))

    def test_parallel_revert(self):
        flow = uf.Flow('p-r-3').add(
            utils.TaskNoRequiresNoReturns(name='task1'),
            utils.FailingTask(name='fail'),
            utils.TaskNoRequiresNoReturns(name='task2')
        )
        engine = self._make_engine(flow)
        self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
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
        self.assertFailuresRegexp(RuntimeError, '^Gotcha', engine.run)

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
            td.update(conn.update_atom_details(td))

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
        self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)

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
        self.assertFailuresRegexp(RuntimeError, '^Gotcha', engine.run)

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
        self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
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
        self.assertFailuresRegexp(RuntimeError, '^Gotcha', engine.run)
        self.assertNotIn('task2 reverted(5)', self.values)


class EngineGraphFlowTest(utils.EngineTestBase):

    def test_run_empty_flow(self):
        flow = gf.Flow('g-1')
        engine = self._make_engine(flow)
        self.assertRaises(exc.Empty, engine.run)

    def test_run_nested_empty_flows(self):
        flow = gf.Flow('g-1').add(lf.Flow('l-1'),
                                  gf.Flow('g-2'))
        engine = self._make_engine(flow)
        self.assertRaises(exc.Empty, engine.run)

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
        self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
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
        self.assertFailuresRegexp(RuntimeError, '^Gotcha', engine.run)
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
        engine.compile()
        graph = engine.compilation.execution_graph
        self.assertIsInstance(graph, gr.DiGraph)

    def test_task_graph_property_for_one_task(self):
        flow = utils.TaskNoRequiresNoReturns(name='task1')

        engine = self._make_engine(flow)
        engine.compile()
        graph = engine.compilation.execution_graph
        self.assertIsInstance(graph, gr.DiGraph)


class EngineCheckingTaskTest(utils.EngineTestBase):

    def test_flow_failures_are_passed_to_revert(self):
        class CheckingTask(task.Task):
            def execute(m_self):
                return 'RESULT'

            def revert(m_self, result, flow_failures):
                self.assertEqual(result, 'RESULT')
                self.assertEqual(list(flow_failures.keys()), ['fail1'])
                fail = flow_failures['fail1']
                self.assertIsInstance(fail, failure.Failure)
                self.assertEqual(str(fail), 'Failure: RuntimeError: Woot!')

        flow = lf.Flow('test').add(
            CheckingTask(),
            utils.FailingTask('fail1')
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegexp(RuntimeError, '^Woot', engine.run)


class SingleThreadedEngineTest(EngineTaskTest,
                               EngineLinearFlowTest,
                               EngineParallelFlowTest,
                               EngineLinearAndUnorderedExceptionsTest,
                               EngineGraphFlowTest,
                               EngineCheckingTaskTest,
                               test.TestCase):
    def _make_engine(self, flow, flow_detail=None):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine='serial',
                                     backend=self.backend)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.SerialActionEngine)

    def test_singlethreaded_is_the_default(self):
        engine = taskflow.engines.load(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.SerialActionEngine)


class MultiThreadedEngineTest(EngineTaskTest,
                              EngineLinearFlowTest,
                              EngineParallelFlowTest,
                              EngineLinearAndUnorderedExceptionsTest,
                              EngineGraphFlowTest,
                              EngineCheckingTaskTest,
                              test.TestCase):
    def _make_engine(self, flow, flow_detail=None, executor=None):
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     backend=self.backend,
                                     executor=executor,
                                     engine='parallel')

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.ParallelActionEngine)

    def test_using_common_executor(self):
        flow = utils.TaskNoRequiresNoReturns(name='task1')
        executor = futures.ThreadPoolExecutor(2)
        try:
            e1 = self._make_engine(flow, executor=executor)
            e2 = self._make_engine(flow, executor=executor)
            self.assertIs(e1.options['executor'], e2.options['executor'])
        finally:
            executor.shutdown(wait=True)


@testtools.skipIf(not au.EVENTLET_AVAILABLE, 'eventlet is not available')
class ParallelEngineWithEventletTest(EngineTaskTest,
                                     EngineLinearFlowTest,
                                     EngineParallelFlowTest,
                                     EngineLinearAndUnorderedExceptionsTest,
                                     EngineGraphFlowTest,
                                     EngineCheckingTaskTest,
                                     test.TestCase):

    def _make_engine(self, flow, flow_detail=None, executor=None):
        if executor is None:
            executor = futures.GreenThreadPoolExecutor()
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     backend=self.backend, engine='parallel',
                                     executor=executor)


class WorkerBasedEngineTest(EngineTaskTest,
                            EngineLinearFlowTest,
                            EngineParallelFlowTest,
                            EngineLinearAndUnorderedExceptionsTest,
                            EngineGraphFlowTest,
                            test.TestCase):
    def setUp(self):
        super(WorkerBasedEngineTest, self).setUp()
        shared_conf = {
            'exchange': 'test',
            'transport': 'memory',
            'transport_options': {
                # NOTE(imelnikov): I run tests several times for different
                # intervals. Reducing polling interval below 0.01 did not give
                # considerable win in tests run time; reducing polling interval
                # too much (AFAIR below 0.0005) affected stability -- I was
                # seeing timeouts. So, 0.01 looks like the most balanced for
                # local transports (for now).
                'polling_interval': 0.01,
            },
        }
        worker_conf = shared_conf.copy()
        worker_conf.update({
            'topic': 'my-topic',
            'tasks': [
                # This makes it possible for the worker to run/find any atoms
                # that are defined in the test.utils module (which are all
                # the task/atom types that this test uses)...
                utils.__name__,
            ],
        })
        self.engine_conf = shared_conf.copy()
        self.engine_conf.update({
            'engine': 'worker-based',
            'topics': tuple([worker_conf['topic']]),
        })
        self.worker = wkr.Worker(**worker_conf)
        self.worker_thread = tu.daemon_thread(target=self.worker.run)
        self.worker_thread.start()

        # Make sure the worker is started before we can continue...
        self.worker.wait()

    def tearDown(self):
        self.worker.stop()
        self.worker_thread.join()
        super(WorkerBasedEngineTest, self).tearDown()

    def _make_engine(self, flow, flow_detail=None):
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     backend=self.backend, **self.engine_conf)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, w_eng.WorkerBasedActionEngine)
