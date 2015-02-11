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
from taskflow.utils import eventlet_utils as eu
from taskflow.utils import persistence_utils as p_utils
from taskflow.utils import threading_utils as tu


class EngineTaskTest(object):

    def test_run_task_as_flow(self):
        flow = utils.ProgressingTask(name='task1')
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)

    def test_run_task_with_notifications(self):
        flow = utils.ProgressingTask(name='task1')
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        expected = ['task1.f RUNNING', 'task1.t RUNNING',
                    'task1.t SUCCESS(5)', 'task1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_failing_task_with_notifications(self):
        values = []
        flow = utils.FailingTask('fail')
        engine = self._make_engine(flow)
        expected = ['fail.f RUNNING', 'fail.t RUNNING',
                    'fail.t FAILURE(Failure: RuntimeError: Woot!)',
                    'fail.t REVERTING', 'fail.t REVERTED',
                    'fail.f REVERTED']
        with utils.CaptureListener(engine, values=values) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(expected, capturer.values)
        self.assertEqual(engine.storage.get_flow_state(), states.REVERTED)
        with utils.CaptureListener(engine, values=values) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        now_expected = list(expected)
        now_expected.extend(['fail.t PENDING', 'fail.f PENDING'])
        now_expected.extend(expected)
        self.assertEqual(now_expected, values)
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


class EngineOptionalRequirementsTest(utils.EngineTestBase):
    def test_expected_optional_multiplers(self):
        flow_no_inject = lf.Flow("flow")
        flow_no_inject.add(utils.OptionalTask(provides='result'))

        flow_inject_a = lf.Flow("flow")
        flow_inject_a.add(utils.OptionalTask(provides='result',
                                             inject={'a': 10}))

        flow_inject_b = lf.Flow("flow")
        flow_inject_b.add(utils.OptionalTask(provides='result',
                                             inject={'b': 1000}))

        engine = self._make_engine(flow_no_inject, store={'a': 3})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual(result, {'a': 3, 'result': 15})

        engine = self._make_engine(flow_no_inject,
                                   store={'a': 3, 'b': 7})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual(result, {'a': 3, 'b': 7, 'result': 21})

        engine = self._make_engine(flow_inject_a, store={'a': 3})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual(result, {'a': 3, 'result': 50})

        engine = self._make_engine(flow_inject_a, store={'a': 3, 'b': 7})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual(result, {'a': 3, 'b': 7, 'result': 70})

        engine = self._make_engine(flow_inject_b, store={'a': 3})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual(result, {'a': 3, 'result': 3000})

        engine = self._make_engine(flow_inject_b, store={'a': 3, 'b': 7})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual(result, {'a': 3, 'b': 7, 'result': 3000})


class EngineLinearFlowTest(utils.EngineTestBase):

    def test_run_empty_flow(self):
        flow = lf.Flow('flow-1')
        engine = self._make_engine(flow)
        self.assertRaises(exc.Empty, engine.run)

    def test_sequential_flow_one_task(self):
        flow = lf.Flow('flow-1').add(
            utils.ProgressingTask(name='task1')
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)

    def test_sequential_flow_two_tasks(self):
        flow = lf.Flow('flow-2').add(
            utils.ProgressingTask(name='task1'),
            utils.ProgressingTask(name='task2')
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)',
                    'task2.t RUNNING', 'task2.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)
        self.assertEqual(len(flow), 2)

    def test_sequential_flow_two_tasks_iter(self):
        flow = lf.Flow('flow-2').add(
            utils.ProgressingTask(name='task1'),
            utils.ProgressingTask(name='task2')
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            gathered_states = list(engine.run_iter())
        self.assertTrue(len(gathered_states) > 0)
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)',
                    'task2.t RUNNING', 'task2.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)
        self.assertEqual(len(flow), 2)

    def test_sequential_flow_iter_suspend_resume(self):
        flow = lf.Flow('flow-2').add(
            utils.ProgressingTask(name='task1'),
            utils.ProgressingTask(name='task2')
        )
        lb, fd = p_utils.temporary_flow_detail(self.backend)

        engine = self._make_engine(flow, flow_detail=fd)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            it = engine.run_iter()
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
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)
        self.assertEqual(states.SUSPENDED, engine.storage.get_flow_state())

        # Attempt to resume it and see what runs now...
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            gathered_states = list(engine.run_iter())
        self.assertTrue(len(gathered_states) > 0)
        expected = ['task2.t RUNNING', 'task2.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)
        self.assertEqual(states.SUCCESS, engine.storage.get_flow_state())

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
            utils.ProgressingTask('task1'),
            lf.Flow('inner-1').add(
                utils.ProgressingTask('task2')
            )
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)',
                    'task2.t RUNNING', 'task2.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)

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
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        expected = ['fail.t RUNNING',
                    'fail.t FAILURE(Failure: RuntimeError: Woot!)',
                    'fail.t REVERTING', 'fail.t REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_correctly_reverts_children(self):
        flow = lf.Flow('root-1').add(
            utils.ProgressingTask('task1'),
            lf.Flow('child-1').add(
                utils.ProgressingTask('task2'),
                utils.FailingTask('fail')
            )
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)',
                    'task2.t RUNNING', 'task2.t SUCCESS(5)',
                    'fail.t RUNNING',
                    'fail.t FAILURE(Failure: RuntimeError: Woot!)',
                    'fail.t REVERTING', 'fail.t REVERTED',
                    'task2.t REVERTING', 'task2.t REVERTED',
                    'task1.t REVERTING', 'task1.t REVERTED']
        self.assertEqual(expected, capturer.values)


class EngineParallelFlowTest(utils.EngineTestBase):

    def test_run_empty_flow(self):
        flow = uf.Flow('p-1')
        engine = self._make_engine(flow)
        self.assertRaises(exc.Empty, engine.run)

    def test_parallel_flow_one_task(self):
        flow = uf.Flow('p-1').add(
            utils.ProgressingTask(name='task1', provides='a')
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)
        self.assertEqual(engine.storage.fetch_all(), {'a': 5})

    def test_parallel_flow_two_tasks(self):
        flow = uf.Flow('p-2').add(
            utils.ProgressingTask(name='task1'),
            utils.ProgressingTask(name='task2')
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = set(['task2.t SUCCESS(5)', 'task2.t RUNNING',
                        'task1.t RUNNING', 'task1.t SUCCESS(5)'])
        self.assertEqual(expected, set(capturer.values))

    def test_parallel_revert(self):
        flow = uf.Flow('p-r-3').add(
            utils.TaskNoRequiresNoReturns(name='task1'),
            utils.FailingTask(name='fail'),
            utils.TaskNoRequiresNoReturns(name='task2')
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        self.assertIn('fail.t FAILURE(Failure: RuntimeError: Woot!)',
                      capturer.values)

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
            utils.ProgressingTask(name='task1', provides='x1'),
            utils.ProgressingTask(name='task2', provides='x2')
        )

        # Create FlowDetail as if we already run task1
        lb, fd = p_utils.temporary_flow_detail(self.backend)
        td = logbook.TaskDetail(name='task1', uuid='42')
        td.state = states.SUCCESS
        td.results = 17
        fd.add(td)

        with contextlib.closing(self.backend.get_connection()) as conn:
            fd.update(conn.update_flow_details(fd))
            td.update(conn.update_atom_details(td))

        engine = self._make_engine(flow, fd)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['task2.t RUNNING', 'task2.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)
        self.assertEqual(engine.storage.fetch_all(),
                         {'x1': 17, 'x2': 5})


class EngineLinearAndUnorderedExceptionsTest(utils.EngineTestBase):

    def test_revert_ok_for_unordered_in_linear(self):
        flow = lf.Flow('p-root').add(
            utils.ProgressingTask(name='task1'),
            utils.ProgressingTask(name='task2'),
            uf.Flow('p-inner').add(
                utils.ProgressingTask(name='task3'),
                utils.FailingTask('fail')
            )
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)

        # NOTE(imelnikov): we don't know if task 3 was run, but if it was,
        # it should have been reverted in correct order.
        possible_values_no_task3 = [
            'task1.t RUNNING', 'task2.t RUNNING',
            'fail.t FAILURE(Failure: RuntimeError: Woot!)',
            'task2.t REVERTED', 'task1.t REVERTED'
        ]
        self.assertIsSuperAndSubsequence(capturer.values,
                                         possible_values_no_task3)
        if 'task3' in capturer.values:
            possible_values_task3 = [
                'task1.t RUNNING', 'task2.t RUNNING', 'task3.t RUNNING',
                'task3.t REVERTED', 'task2.t REVERTED', 'task1.t REVERTED'
            ]
            self.assertIsSuperAndSubsequence(capturer.values,
                                             possible_values_task3)

    def test_revert_raises_for_unordered_in_linear(self):
        flow = lf.Flow('p-root').add(
            utils.ProgressingTask(name='task1'),
            utils.ProgressingTask(name='task2'),
            uf.Flow('p-inner').add(
                utils.ProgressingTask(name='task3'),
                utils.NastyFailingTask(name='nasty')
            )
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine,
                                   capture_flow=False,
                                   skip_tasks=['nasty']) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Gotcha', engine.run)

        # NOTE(imelnikov): we don't know if task 3 was run, but if it was,
        # it should have been reverted in correct order.
        possible_values = ['task1.t RUNNING', 'task1.t SUCCESS(5)',
                           'task2.t RUNNING', 'task2.t SUCCESS(5)',
                           'task3.t RUNNING', 'task3.t SUCCESS(5)',
                           'task3.t REVERTING',
                           'task3.t REVERTED']
        self.assertIsSuperAndSubsequence(possible_values, capturer.values)
        possible_values_no_task3 = ['task1.t RUNNING', 'task2.t RUNNING']
        self.assertIsSuperAndSubsequence(capturer.values,
                                         possible_values_no_task3)

    def test_revert_ok_for_linear_in_unordered(self):
        flow = uf.Flow('p-root').add(
            utils.ProgressingTask(name='task1'),
            lf.Flow('p-inner').add(
                utils.ProgressingTask(name='task2'),
                utils.FailingTask('fail')
            )
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        self.assertIn('fail.t FAILURE(Failure: RuntimeError: Woot!)',
                      capturer.values)

        # NOTE(imelnikov): if task1 was run, it should have been reverted.
        if 'task1' in capturer.values:
            task1_story = ['task1.t RUNNING', 'task1.t SUCCESS(5)',
                           'task1.t REVERTED']
            self.assertIsSuperAndSubsequence(capturer.values, task1_story)

        # NOTE(imelnikov): task2 should have been run and reverted
        task2_story = ['task2.t RUNNING', 'task2.t SUCCESS(5)',
                       'task2.t REVERTED']
        self.assertIsSuperAndSubsequence(capturer.values, task2_story)

    def test_revert_raises_for_linear_in_unordered(self):
        flow = uf.Flow('p-root').add(
            utils.ProgressingTask(name='task1'),
            lf.Flow('p-inner').add(
                utils.ProgressingTask(name='task2'),
                utils.NastyFailingTask()
            )
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Gotcha', engine.run)
        self.assertNotIn('task2.t REVERTED', capturer.values)


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
            utils.ProgressingTask(name='task1')
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)

    def test_graph_flow_two_independent_tasks(self):
        flow = gf.Flow('g-2').add(
            utils.ProgressingTask(name='task1'),
            utils.ProgressingTask(name='task2')
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = set(['task2.t SUCCESS(5)', 'task2.t RUNNING',
                        'task1.t RUNNING', 'task1.t SUCCESS(5)'])
        self.assertEqual(expected, set(capturer.values))
        self.assertEqual(len(flow), 2)

    def test_graph_flow_two_tasks(self):
        flow = gf.Flow('g-1-1').add(
            utils.ProgressingTask(name='task2', requires=['a']),
            utils.ProgressingTask(name='task1', provides='a')
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)',
                    'task2.t RUNNING', 'task2.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)

    def test_graph_flow_four_tasks_added_separately(self):
        flow = (gf.Flow('g-4')
                .add(utils.ProgressingTask(name='task4',
                                           provides='d', requires=['c']))
                .add(utils.ProgressingTask(name='task2',
                                           provides='b', requires=['a']))
                .add(utils.ProgressingTask(name='task3',
                                           provides='c', requires=['b']))
                .add(utils.ProgressingTask(name='task1',
                                           provides='a'))
                )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)',
                    'task2.t RUNNING', 'task2.t SUCCESS(5)',
                    'task3.t RUNNING', 'task3.t SUCCESS(5)',
                    'task4.t RUNNING', 'task4.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)

    def test_graph_flow_four_tasks_revert(self):
        flow = gf.Flow('g-4-failing').add(
            utils.ProgressingTask(name='task4',
                                  provides='d', requires=['c']),
            utils.ProgressingTask(name='task2',
                                  provides='b', requires=['a']),
            utils.FailingTask(name='task3',
                              provides='c', requires=['b']),
            utils.ProgressingTask(name='task1', provides='a'))

        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)',
                    'task2.t RUNNING', 'task2.t SUCCESS(5)',
                    'task3.t RUNNING',
                    'task3.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task3.t REVERTING',
                    'task3.t REVERTED',
                    'task2.t REVERTING',
                    'task2.t REVERTED',
                    'task1.t REVERTING',
                    'task1.t REVERTED']
        self.assertEqual(expected, capturer.values)
        self.assertEqual(engine.storage.get_flow_state(), states.REVERTED)

    def test_graph_flow_four_tasks_revert_failure(self):
        flow = gf.Flow('g-3-nasty').add(
            utils.NastyTask(name='task2', provides='b', requires=['a']),
            utils.FailingTask(name='task3', requires=['b']),
            utils.ProgressingTask(name='task1', provides='a'))

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
    # FIXME: this test uses a inner class that workers/process engines can't
    # get to, so we need to do something better to make this test useful for
    # those engines...

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


class SerialEngineTest(EngineTaskTest,
                       EngineLinearFlowTest,
                       EngineParallelFlowTest,
                       EngineLinearAndUnorderedExceptionsTest,
                       EngineOptionalRequirementsTest,
                       EngineGraphFlowTest,
                       EngineCheckingTaskTest,
                       test.TestCase):
    def _make_engine(self, flow,
                     flow_detail=None, store=None):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine='serial',
                                     backend=self.backend,
                                     store=store)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.SerialActionEngine)

    def test_singlethreaded_is_the_default(self):
        engine = taskflow.engines.load(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.SerialActionEngine)


class ParallelEngineWithThreadsTest(EngineTaskTest,
                                    EngineLinearFlowTest,
                                    EngineParallelFlowTest,
                                    EngineLinearAndUnorderedExceptionsTest,
                                    EngineOptionalRequirementsTest,
                                    EngineGraphFlowTest,
                                    EngineCheckingTaskTest,
                                    test.TestCase):
    _EXECUTOR_WORKERS = 2

    def _make_engine(self, flow,
                     flow_detail=None, executor=None, store=None):
        if executor is None:
            executor = 'threads'
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     backend=self.backend,
                                     executor=executor,
                                     engine='parallel',
                                     store=store,
                                     max_workers=self._EXECUTOR_WORKERS)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.ParallelActionEngine)

    def test_using_common_executor(self):
        flow = utils.TaskNoRequiresNoReturns(name='task1')
        executor = futures.ThreadPoolExecutor(self._EXECUTOR_WORKERS)
        try:
            e1 = self._make_engine(flow, executor=executor)
            e2 = self._make_engine(flow, executor=executor)
            self.assertIs(e1.options['executor'], e2.options['executor'])
        finally:
            executor.shutdown(wait=True)


@testtools.skipIf(not eu.EVENTLET_AVAILABLE, 'eventlet is not available')
class ParallelEngineWithEventletTest(EngineTaskTest,
                                     EngineLinearFlowTest,
                                     EngineParallelFlowTest,
                                     EngineLinearAndUnorderedExceptionsTest,
                                     EngineOptionalRequirementsTest,
                                     EngineGraphFlowTest,
                                     EngineCheckingTaskTest,
                                     test.TestCase):

    def _make_engine(self, flow,
                     flow_detail=None, executor=None, store=None):
        if executor is None:
            executor = futures.GreenThreadPoolExecutor()
            self.addCleanup(executor.shutdown)
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     backend=self.backend, engine='parallel',
                                     executor=executor,
                                     store=store)


class ParallelEngineWithProcessTest(EngineTaskTest,
                                    EngineLinearFlowTest,
                                    EngineParallelFlowTest,
                                    EngineLinearAndUnorderedExceptionsTest,
                                    EngineOptionalRequirementsTest,
                                    EngineGraphFlowTest,
                                    test.TestCase):
    _EXECUTOR_WORKERS = 2

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.ParallelActionEngine)

    def _make_engine(self, flow,
                     flow_detail=None, executor=None, store=None):
        if executor is None:
            executor = 'processes'
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     backend=self.backend,
                                     engine='parallel',
                                     executor=executor,
                                     store=store,
                                     max_workers=self._EXECUTOR_WORKERS)


class WorkerBasedEngineTest(EngineTaskTest,
                            EngineLinearFlowTest,
                            EngineParallelFlowTest,
                            EngineLinearAndUnorderedExceptionsTest,
                            EngineOptionalRequirementsTest,
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
        self.worker_thread = tu.daemon_thread(self.worker.run)
        self.worker_thread.start()

        # Make sure the worker is started before we can continue...
        self.worker.wait()

    def tearDown(self):
        self.worker.stop()
        self.worker_thread.join()
        super(WorkerBasedEngineTest, self).tearDown()

    def _make_engine(self, flow,
                     flow_detail=None, store=None):
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     backend=self.backend,
                                     store=store, **self.engine_conf)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, w_eng.WorkerBasedActionEngine)
