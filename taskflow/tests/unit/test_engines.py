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

import collections
import contextlib
import functools
import threading

import futurist
import six
import testtools

import taskflow.engines
from taskflow.engines.action_engine import engine as eng
from taskflow.engines.worker_based import engine as w_eng
from taskflow.engines.worker_based import worker as wkr
from taskflow import exceptions as exc
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow.persistence import models
from taskflow import states
from taskflow import task
from taskflow import test
from taskflow.tests import utils
from taskflow.types import failure
from taskflow.types import graph as gr
from taskflow.utils import eventlet_utils as eu
from taskflow.utils import persistence_utils as p_utils
from taskflow.utils import threading_utils as tu


# Expected engine transitions when empty workflows are ran...
_EMPTY_TRANSITIONS = [
    states.RESUMING, states.SCHEDULING, states.WAITING,
    states.ANALYZING, states.SUCCESS,
]


class EngineTaskNotificationsTest(object):
    def test_run_capture_task_notifications(self):
        captured = collections.defaultdict(list)

        def do_capture(bound_name, event_type, details):
            progress_capture = captured[bound_name]
            progress_capture.append(details)

        flow = lf.Flow("flow")
        work_1 = utils.MultiProgressingTask('work-1')
        work_1.notifier.register(task.EVENT_UPDATE_PROGRESS,
                                 functools.partial(do_capture, 'work-1'))
        work_2 = utils.MultiProgressingTask('work-2')
        work_2.notifier.register(task.EVENT_UPDATE_PROGRESS,
                                 functools.partial(do_capture, 'work-2'))
        flow.add(work_1, work_2)

        # NOTE(harlowja): These were selected so that float comparison will
        # work vs not work...
        progress_chunks = tuple([0.2, 0.5, 0.8])
        engine = self._make_engine(
            flow, store={'progress_chunks': progress_chunks})
        engine.run()

        expected = [
            {'progress': 0.0},
            {'progress': 0.2},
            {'progress': 0.5},
            {'progress': 0.8},
            {'progress': 1.0},
        ]
        for name in ['work-1', 'work-2']:
            self.assertEqual(expected, captured[name])


class EngineTaskTest(object):

    def test_run_task_as_flow(self):
        flow = utils.ProgressingTask(name='task1')
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)

    def test_run_task_with_flow_notifications(self):
        flow = utils.ProgressingTask(name='task1')
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        expected = ['task1.f RUNNING', 'task1.t RUNNING',
                    'task1.t SUCCESS(5)', 'task1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_failing_task_with_flow_notifications(self):
        values = []
        flow = utils.FailingTask('fail')
        engine = self._make_engine(flow)
        expected = ['fail.f RUNNING', 'fail.t RUNNING',
                    'fail.t FAILURE(Failure: RuntimeError: Woot!)',
                    'fail.t REVERTING', 'fail.t REVERTED(None)',
                    'fail.f REVERTED']
        with utils.CaptureListener(engine, values=values) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(expected, capturer.values)
        self.assertEqual(states.REVERTED, engine.storage.get_flow_state())
        with utils.CaptureListener(engine, values=values) as capturer:
            self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        now_expected = list(expected)
        now_expected.extend(['fail.t PENDING', 'fail.f PENDING'])
        now_expected.extend(expected)
        self.assertEqual(now_expected, values)
        self.assertEqual(states.REVERTED, engine.storage.get_flow_state())

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
        self.assertEqual({'a': 3, 'result': 15}, result)

        engine = self._make_engine(flow_no_inject,
                                   store={'a': 3, 'b': 7})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual({'a': 3, 'b': 7, 'result': 21}, result)

        engine = self._make_engine(flow_inject_a, store={'a': 3})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual({'a': 3, 'result': 50}, result)

        engine = self._make_engine(flow_inject_a, store={'a': 3, 'b': 7})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual({'a': 3, 'b': 7, 'result': 70}, result)

        engine = self._make_engine(flow_inject_b, store={'a': 3})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual({'a': 3, 'result': 3000}, result)

        engine = self._make_engine(flow_inject_b, store={'a': 3, 'b': 7})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual({'a': 3, 'b': 7, 'result': 3000}, result)


class EngineMultipleResultsTest(utils.EngineTestBase):
    def test_fetch_with_a_single_result(self):
        flow = lf.Flow("flow")
        flow.add(utils.TaskOneReturn(provides='x'))

        engine = self._make_engine(flow)
        engine.run()
        result = engine.storage.fetch('x')
        self.assertEqual(1, result)

    def test_many_results_visible_to(self):
        flow = lf.Flow("flow")
        flow.add(utils.AddOneSameProvidesRequires(
            'a', rebind={'value': 'source'}))
        flow.add(utils.AddOneSameProvidesRequires('b'))
        flow.add(utils.AddOneSameProvidesRequires('c'))
        engine = self._make_engine(flow, store={'source': 0})
        engine.run()

        # Check what each task in the prior should be seeing...
        atoms = list(flow)
        a = atoms[0]
        a_kwargs = engine.storage.fetch_mapped_args(a.rebind,
                                                    atom_name='a')
        self.assertEqual({'value': 0}, a_kwargs)

        b = atoms[1]
        b_kwargs = engine.storage.fetch_mapped_args(b.rebind,
                                                    atom_name='b')
        self.assertEqual({'value': 1}, b_kwargs)

        c = atoms[2]
        c_kwargs = engine.storage.fetch_mapped_args(c.rebind,
                                                    atom_name='c')
        self.assertEqual({'value': 2}, c_kwargs)

    def test_many_results_storage_provided_visible_to(self):
        # This works as expected due to docs listed at
        #
        # http://docs.openstack.org/developer/taskflow/engines.html#scoping
        flow = lf.Flow("flow")
        flow.add(utils.AddOneSameProvidesRequires('a'))
        flow.add(utils.AddOneSameProvidesRequires('b'))
        flow.add(utils.AddOneSameProvidesRequires('c'))
        engine = self._make_engine(flow, store={'value': 0})
        engine.run()

        # Check what each task in the prior should be seeing...
        atoms = list(flow)
        a = atoms[0]
        a_kwargs = engine.storage.fetch_mapped_args(a.rebind,
                                                    atom_name='a')
        self.assertEqual({'value': 0}, a_kwargs)

        b = atoms[1]
        b_kwargs = engine.storage.fetch_mapped_args(b.rebind,
                                                    atom_name='b')
        self.assertEqual({'value': 0}, b_kwargs)

        c = atoms[2]
        c_kwargs = engine.storage.fetch_mapped_args(c.rebind,
                                                    atom_name='c')
        self.assertEqual({'value': 0}, c_kwargs)

    def test_fetch_with_two_results(self):
        flow = lf.Flow("flow")
        flow.add(utils.TaskOneReturn(provides='x'))

        engine = self._make_engine(flow, store={'x': 0})
        engine.run()
        result = engine.storage.fetch('x')
        self.assertEqual(0, result)

    def test_fetch_all_with_a_single_result(self):
        flow = lf.Flow("flow")
        flow.add(utils.TaskOneReturn(provides='x'))

        engine = self._make_engine(flow)
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual({'x': 1}, result)

    def test_fetch_all_with_two_results(self):
        flow = lf.Flow("flow")
        flow.add(utils.TaskOneReturn(provides='x'))

        engine = self._make_engine(flow, store={'x': 0})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual({'x': [0, 1]}, result)

    def test_task_can_update_value(self):
        flow = lf.Flow("flow")
        flow.add(utils.TaskOneArgOneReturn(requires='x', provides='x'))

        engine = self._make_engine(flow, store={'x': 0})
        engine.run()
        result = engine.storage.fetch_all()
        self.assertEqual({'x': [0, 1]}, result)


class EngineLinearFlowTest(utils.EngineTestBase):

    def test_run_empty_linear_flow(self):
        flow = lf.Flow('flow-1')
        engine = self._make_engine(flow)
        self.assertEqual(_EMPTY_TRANSITIONS, list(engine.run_iter()))

    def test_overlap_parent_sibling_expected_result(self):
        flow = lf.Flow('flow-1')
        flow.add(utils.ProgressingTask(provides='source'))
        flow.add(utils.TaskOneReturn(provides='source'))
        subflow = lf.Flow('flow-2')
        subflow.add(utils.AddOne())
        flow.add(subflow)
        engine = self._make_engine(flow)
        engine.run()
        results = engine.storage.fetch_all()
        self.assertEqual(2, results['result'])

    def test_overlap_parent_expected_result(self):
        flow = lf.Flow('flow-1')
        flow.add(utils.ProgressingTask(provides='source'))
        subflow = lf.Flow('flow-2')
        subflow.add(utils.TaskOneReturn(provides='source'))
        subflow.add(utils.AddOne())
        flow.add(subflow)
        engine = self._make_engine(flow)
        engine.run()
        results = engine.storage.fetch_all()
        self.assertEqual(2, results['result'])

    def test_overlap_sibling_expected_result(self):
        flow = lf.Flow('flow-1')
        flow.add(utils.ProgressingTask(provides='source'))
        flow.add(utils.TaskOneReturn(provides='source'))
        flow.add(utils.AddOne())
        engine = self._make_engine(flow)
        engine.run()
        results = engine.storage.fetch_all()
        self.assertEqual(2, results['result'])

    def test_sequential_flow_interrupted_externally(self):
        flow = lf.Flow('flow-1').add(
            utils.ProgressingTask(name='task1'),
            utils.ProgressingTask(name='task2'),
            utils.ProgressingTask(name='task3'),
        )
        engine = self._make_engine(flow)

        def _run_engine_and_raise():
            engine_states = {}
            engine_it = engine.run_iter()
            while True:
                try:
                    engine_state = six.next(engine_it)
                    if engine_state not in engine_states:
                        engine_states[engine_state] = 1
                    else:
                        engine_states[engine_state] += 1
                    if engine_states.get(states.SCHEDULING) == 2:
                        engine_state = engine_it.throw(IOError("I Broke"))
                        if engine_state not in engine_states:
                            engine_states[engine_state] = 1
                        else:
                            engine_states[engine_state] += 1
                except StopIteration:
                    break

        self.assertRaises(IOError, _run_engine_and_raise)
        self.assertEqual(states.FAILURE, engine.storage.get_flow_state())

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
        self.assertEqual(2, len(flow))

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
        self.assertEqual(2, len(flow))

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
        self.assertEqual({}, engine.storage.fetch_all())

    def test_revert_provided(self):
        flow = lf.Flow('revert').add(
            utils.GiveBackRevert('giver'),
            utils.FailingTask(name='fail')
        )
        engine = self._make_engine(flow, store={'value': 0})
        self.assertFailuresRegexp(RuntimeError, '^Woot', engine.run)
        self.assertEqual(2, engine.storage.get_revert_result('giver'))

    def test_nasty_revert(self):
        flow = lf.Flow('revert').add(
            utils.NastyTask('nasty'),
            utils.FailingTask(name='fail')
        )
        engine = self._make_engine(flow)
        self.assertFailuresRegexp(RuntimeError, '^Gotcha', engine.run)
        fail = engine.storage.get_revert_result('nasty')
        self.assertIsNotNone(fail.check(RuntimeError))
        exec_failures = engine.storage.get_execute_failures()
        self.assertIn('fail', exec_failures)
        rev_failures = engine.storage.get_revert_failures()
        self.assertIn('nasty', rev_failures)

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
                    'fail.t REVERTING', 'fail.t REVERTED(None)']
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
                    'fail.t REVERTING', 'fail.t REVERTED(None)',
                    'task2.t REVERTING', 'task2.t REVERTED(None)',
                    'task1.t REVERTING', 'task1.t REVERTED(None)']
        self.assertEqual(expected, capturer.values)


class EngineParallelFlowTest(utils.EngineTestBase):

    def test_run_empty_unordered_flow(self):
        flow = uf.Flow('p-1')
        engine = self._make_engine(flow)
        self.assertEqual(_EMPTY_TRANSITIONS, list(engine.run_iter()))

    def test_parallel_flow_with_priority(self):
        flow = uf.Flow('p-1')
        for i in range(0, 10):
            t = utils.ProgressingTask(name='task%s' % i)
            t.priority = i
            flow.add(t)
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = [
            'task9.t RUNNING',
            'task8.t RUNNING',
            'task7.t RUNNING',
            'task6.t RUNNING',
            'task5.t RUNNING',
            'task4.t RUNNING',
            'task3.t RUNNING',
            'task2.t RUNNING',
            'task1.t RUNNING',
            'task0.t RUNNING',
        ]
        # NOTE(harlowja): chop off the gathering of SUCCESS states, since we
        # don't care if thats in order...
        gotten = capturer.values[0:10]
        self.assertEqual(expected, gotten)

    def test_parallel_flow_one_task(self):
        flow = uf.Flow('p-1').add(
            utils.ProgressingTask(name='task1', provides='a')
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['task1.t RUNNING', 'task1.t SUCCESS(5)']
        self.assertEqual(expected, capturer.values)
        self.assertEqual({'a': 5}, engine.storage.fetch_all())

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
        td = models.TaskDetail(name='task1', uuid='42')
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
        self.assertEqual({'x1': 17, 'x2': 5},
                         engine.storage.fetch_all())


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
        # it should have been REVERTED(None) in correct order.
        possible_values_no_task3 = [
            'task1.t RUNNING', 'task2.t RUNNING',
            'fail.t FAILURE(Failure: RuntimeError: Woot!)',
            'task2.t REVERTED(None)', 'task1.t REVERTED(None)'
        ]
        self.assertIsSuperAndSubsequence(capturer.values,
                                         possible_values_no_task3)
        if 'task3' in capturer.values:
            possible_values_task3 = [
                'task1.t RUNNING', 'task2.t RUNNING', 'task3.t RUNNING',
                'task3.t REVERTED(None)', 'task2.t REVERTED(None)',
                'task1.t REVERTED(None)'
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
        # it should have been REVERTED(None) in correct order.
        possible_values = ['task1.t RUNNING', 'task1.t SUCCESS(5)',
                           'task2.t RUNNING', 'task2.t SUCCESS(5)',
                           'task3.t RUNNING', 'task3.t SUCCESS(5)',
                           'task3.t REVERTING',
                           'task3.t REVERTED(None)']
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
                           'task1.t REVERTED(None)']
            self.assertIsSuperAndSubsequence(capturer.values, task1_story)

        # NOTE(imelnikov): task2 should have been run and reverted
        task2_story = ['task2.t RUNNING', 'task2.t SUCCESS(5)',
                       'task2.t REVERTED(None)']
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
        self.assertNotIn('task2.t REVERTED(None)', capturer.values)


class EngineDeciderDepthTest(utils.EngineTestBase):

    def test_run_graph_flow_decider_various_depths(self):
        sub_flow_1 = gf.Flow('g_1')
        g_1_1 = utils.ProgressingTask(name='g_1-1')
        sub_flow_1.add(g_1_1)
        g_1 = utils.ProgressingTask(name='g-1')
        g_2 = utils.ProgressingTask(name='g-2')
        g_3 = utils.ProgressingTask(name='g-3')
        g_4 = utils.ProgressingTask(name='g-4')
        for a_depth, ran_how_many in [('all', 1),
                                      ('atom', 4),
                                      ('flow', 2),
                                      ('neighbors', 3)]:
            flow = gf.Flow('g')
            flow.add(g_1, g_2, sub_flow_1, g_3, g_4)
            flow.link(g_1, g_2,
                      decider=lambda history: False,
                      decider_depth=a_depth)
            flow.link(g_2, sub_flow_1)
            flow.link(g_2, g_3)
            flow.link(g_3, g_4)
            flow.link(g_1, sub_flow_1,
                      decider=lambda history: True,
                      decider_depth=a_depth)
            e = self._make_engine(flow)
            with utils.CaptureListener(e, capture_flow=False) as capturer:
                e.run()
            ran_tasks = 0
            for outcome in capturer.values:
                if outcome.endswith("RUNNING"):
                    ran_tasks += 1
            self.assertEqual(ran_how_many, ran_tasks)

    def test_run_graph_flow_decider_jump_over_atom(self):
        flow = gf.Flow('g')
        a = utils.AddOneSameProvidesRequires("a", inject={'value': 0})
        b = utils.AddOneSameProvidesRequires("b")
        c = utils.AddOneSameProvidesRequires("c")
        flow.add(a, b, c, resolve_requires=False)
        flow.link(a, b, decider=lambda history: False,
                  decider_depth='atom')
        flow.link(b, c)
        e = self._make_engine(flow)
        e.run()
        self.assertEqual(2, e.storage.get('c'))
        self.assertEqual(states.IGNORE, e.storage.get_atom_state('b'))

    def test_run_graph_flow_decider_jump_over_bad_atom(self):
        flow = gf.Flow('g')
        a = utils.NoopTask("a")
        b = utils.FailingTask("b")
        c = utils.NoopTask("c")
        flow.add(a, b, c)
        flow.link(a, b, decider=lambda history: False,
                  decider_depth='atom')
        flow.link(b, c)
        e = self._make_engine(flow)
        e.run()

    def test_run_graph_flow_decider_revert(self):
        flow = gf.Flow('g')
        a = utils.NoopTask("a")
        b = utils.NoopTask("b")
        c = utils.FailingTask("c")
        flow.add(a, b, c)
        flow.link(a, b, decider=lambda history: False,
                  decider_depth='atom')
        flow.link(b, c)
        e = self._make_engine(flow)
        with utils.CaptureListener(e, capture_flow=False) as capturer:
            # Wrapped failure here for WBE engine, make this better in
            # the future, perhaps via a custom testtools matcher??
            self.assertRaises((RuntimeError, exc.WrappedFailure), e.run)
        expected = [
            'a.t RUNNING',
            'a.t SUCCESS(None)',
            'b.t IGNORE',
            'c.t RUNNING',
            'c.t FAILURE(Failure: RuntimeError: Woot!)',
            'c.t REVERTING',
            'c.t REVERTED(None)',
            'a.t REVERTING',
            'a.t REVERTED(None)',
        ]
        self.assertEqual(expected, capturer.values)


class EngineGraphFlowTest(utils.EngineTestBase):

    def test_run_empty_graph_flow(self):
        flow = gf.Flow('g-1')
        engine = self._make_engine(flow)
        self.assertEqual(_EMPTY_TRANSITIONS, list(engine.run_iter()))

    def test_run_empty_nested_graph_flows(self):
        flow = gf.Flow('g-1').add(lf.Flow('l-1'),
                                  gf.Flow('g-2'))
        engine = self._make_engine(flow)
        self.assertEqual(_EMPTY_TRANSITIONS, list(engine.run_iter()))

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
        self.assertEqual(2, len(flow))

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
                    'task3.t REVERTED(None)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'task1.t REVERTING',
                    'task1.t REVERTED(None)']
        self.assertEqual(expected, capturer.values)
        self.assertEqual(states.REVERTED, engine.storage.get_flow_state())

    def test_graph_flow_four_tasks_revert_failure(self):
        flow = gf.Flow('g-3-nasty').add(
            utils.NastyTask(name='task2', provides='b', requires=['a']),
            utils.FailingTask(name='task3', requires=['b']),
            utils.ProgressingTask(name='task1', provides='a'))

        engine = self._make_engine(flow)
        self.assertFailuresRegexp(RuntimeError, '^Gotcha', engine.run)
        self.assertEqual(states.FAILURE, engine.storage.get_flow_state())

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
        self.assertEqual({
            'a': 1,
            'b': 3,
            'c': 5,
            'x': 30,
            'y': 38,
            'z': 42
        }, engine.storage.fetch_all())

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


class EngineMissingDepsTest(utils.EngineTestBase):
    def test_missing_deps_deep(self):
        flow = gf.Flow('missing-many').add(
            utils.TaskOneReturn(name='task1',
                                requires=['a', 'b', 'c']),
            utils.TaskMultiArgOneReturn(name='task2',
                                        rebind=['e', 'f', 'g']))
        engine = self._make_engine(flow)
        engine.compile()
        engine.prepare()
        self.assertRaises(exc.MissingDependencies, engine.validate)
        c_e = None
        try:
            engine.validate()
        except exc.MissingDependencies as e:
            c_e = e
        self.assertIsNotNone(c_e)
        self.assertIsNotNone(c_e.cause)


class EngineResetTests(utils.EngineTestBase):
    def test_completed_reset_run_again(self):
        task1 = utils.ProgressingTask(name='task1')
        task2 = utils.ProgressingTask(name='task2')
        task3 = utils.ProgressingTask(name='task3')

        flow = lf.Flow('root')
        flow.add(task1, task2, task3)

        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = [
            'task1.t RUNNING',
            'task1.t SUCCESS(5)',

            'task2.t RUNNING',
            'task2.t SUCCESS(5)',

            'task3.t RUNNING',
            'task3.t SUCCESS(5)',
        ]
        self.assertEqual(expected, capturer.values)

        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        self.assertEqual([], capturer.values)

        engine.reset()
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        self.assertEqual(expected, capturer.values)

    def test_failed_reset_run_again(self):
        task1 = utils.ProgressingTask(name='task1')
        task2 = utils.ProgressingTask(name='task2')
        task3 = utils.FailingTask(name='task3')

        flow = lf.Flow('root')
        flow.add(task1, task2, task3)
        engine = self._make_engine(flow)

        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            # Also allow a WrappedFailure exception so that when this is used
            # with the WBE engine (as it can't re-raise the original
            # exception) that we will work correctly....
            self.assertRaises((RuntimeError, exc.WrappedFailure), engine.run)

        expected = [
            'task1.t RUNNING',
            'task1.t SUCCESS(5)',
            'task2.t RUNNING',
            'task2.t SUCCESS(5)',
            'task3.t RUNNING',

            'task3.t FAILURE(Failure: RuntimeError: Woot!)',

            'task3.t REVERTING',
            'task3.t REVERTED(None)',
            'task2.t REVERTING',
            'task2.t REVERTED(None)',
            'task1.t REVERTING',
            'task1.t REVERTED(None)',
        ]
        self.assertEqual(expected, capturer.values)

        engine.reset()
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            self.assertRaises((RuntimeError, exc.WrappedFailure), engine.run)
        self.assertEqual(expected, capturer.values)

    def test_suspended_reset_run_again(self):
        task1 = utils.ProgressingTask(name='task1')
        task2 = utils.ProgressingTask(name='task2')
        task3 = utils.ProgressingTask(name='task3')

        flow = lf.Flow('root')
        flow.add(task1, task2, task3)
        engine = self._make_engine(flow)
        suspend_at = object()
        expected_states = [
            states.RESUMING,
            states.SCHEDULING,
            states.WAITING,
            states.ANALYZING,
            states.SCHEDULING,
            states.WAITING,
            # Stop/suspend here...
            suspend_at,
            states.SUSPENDED,
        ]
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            for i, st in enumerate(engine.run_iter()):
                expected = expected_states[i]
                if expected is suspend_at:
                    engine.suspend()
                else:
                    self.assertEqual(expected, st)

        expected = [
            'task1.t RUNNING',
            'task1.t SUCCESS(5)',

            'task2.t RUNNING',
            'task2.t SUCCESS(5)',
        ]
        self.assertEqual(expected, capturer.values)

        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = [
            'task3.t RUNNING',
            'task3.t SUCCESS(5)',
        ]
        self.assertEqual(expected, capturer.values)

        engine.reset()
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = [
            'task1.t RUNNING',
            'task1.t SUCCESS(5)',

            'task2.t RUNNING',
            'task2.t SUCCESS(5)',

            'task3.t RUNNING',
            'task3.t SUCCESS(5)',
        ]
        self.assertEqual(expected, capturer.values)


class EngineGraphConditionalFlowTest(utils.EngineTestBase):

    def test_graph_flow_conditional_jumps_across_2(self):
        histories = []

        def should_go(history):
            histories.append(history)
            return False

        task1 = utils.ProgressingTask(name='task1')
        task2 = utils.ProgressingTask(name='task2')
        task3 = utils.ProgressingTask(name='task3')
        task4 = utils.ProgressingTask(name='task4')

        subflow = lf.Flow("more-work")
        subsub_flow = lf.Flow("more-more-work")
        subsub_flow.add(task3, task4)
        subflow.add(subsub_flow)

        flow = gf.Flow("main-work")
        flow.add(task1, task2)
        flow.link(task1, task2)
        flow.add(subflow)
        flow.link(task2, subflow, decider=should_go)

        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()

        expected = [
            'task1.t RUNNING',
            'task1.t SUCCESS(5)',

            'task2.t RUNNING',
            'task2.t SUCCESS(5)',

            'task3.t IGNORE',
            'task4.t IGNORE',
        ]
        self.assertEqual(expected, capturer.values)
        self.assertEqual(1, len(histories))
        self.assertIn('task2', histories[0])

    def test_graph_flow_conditional_jumps_across(self):
        histories = []

        def should_go(history):
            histories.append(history)
            return False

        task1 = utils.ProgressingTask(name='task1')
        task2 = utils.ProgressingTask(name='task2')
        task3 = utils.ProgressingTask(name='task3')
        task4 = utils.ProgressingTask(name='task4')

        subflow = lf.Flow("more-work")
        subflow.add(task3, task4)
        flow = gf.Flow("main-work")
        flow.add(task1, task2)
        flow.link(task1, task2)
        flow.add(subflow)
        flow.link(task2, subflow, decider=should_go)
        flow.link(task1, subflow, decider=should_go)

        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()

        expected = [
            'task1.t RUNNING',
            'task1.t SUCCESS(5)',

            'task2.t RUNNING',
            'task2.t SUCCESS(5)',

            'task3.t IGNORE',
            'task4.t IGNORE',
        ]
        self.assertEqual(expected, capturer.values)
        self.assertEqual(2, len(histories))
        for i in range(0, 2):
            self.assertIn('task1', histories[i])
            self.assertIn('task2', histories[i])

    def test_graph_flow_conditional(self):
        flow = gf.Flow('root')

        task1 = utils.ProgressingTask(name='task1')
        task2 = utils.ProgressingTask(name='task2')
        task2_2 = utils.ProgressingTask(name='task2_2')
        task3 = utils.ProgressingTask(name='task3')

        flow.add(task1, task2, task2_2, task3)
        flow.link(task1, task2, decider=lambda history: False)
        flow.link(task2, task2_2)
        flow.link(task1, task3, decider=lambda history: True)

        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()

        expected = set([
            'task1.t RUNNING',
            'task1.t SUCCESS(5)',

            'task2.t IGNORE',
            'task2_2.t IGNORE',

            'task3.t RUNNING',
            'task3.t SUCCESS(5)',
        ])
        self.assertEqual(expected, set(capturer.values))

    def test_graph_flow_conditional_ignore_reset(self):
        allow_execute = threading.Event()
        flow = gf.Flow('root')

        task1 = utils.ProgressingTask(name='task1')
        task2 = utils.ProgressingTask(name='task2')
        task3 = utils.ProgressingTask(name='task3')

        flow.add(task1, task2, task3)
        flow.link(task1, task2)
        flow.link(task2, task3, decider=lambda history: allow_execute.is_set())

        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()

        expected = set([
            'task1.t RUNNING',
            'task1.t SUCCESS(5)',

            'task2.t RUNNING',
            'task2.t SUCCESS(5)',

            'task3.t IGNORE',
        ])
        self.assertEqual(expected, set(capturer.values))
        self.assertEqual(states.IGNORE,
                         engine.storage.get_atom_state('task3'))
        self.assertEqual(states.IGNORE,
                         engine.storage.get_atom_intention('task3'))

        engine.reset()
        allow_execute.set()
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()

        expected = set([
            'task1.t RUNNING',
            'task1.t SUCCESS(5)',

            'task2.t RUNNING',
            'task2.t SUCCESS(5)',

            'task3.t RUNNING',
            'task3.t SUCCESS(5)',
        ])
        self.assertEqual(expected, set(capturer.values))

    def test_graph_flow_diamond_ignored(self):
        flow = gf.Flow('root')

        task1 = utils.ProgressingTask(name='task1')
        task2 = utils.ProgressingTask(name='task2')
        task3 = utils.ProgressingTask(name='task3')
        task4 = utils.ProgressingTask(name='task4')

        flow.add(task1, task2, task3, task4)
        flow.link(task1, task2)
        flow.link(task2, task4, decider=lambda history: False)
        flow.link(task1, task3)
        flow.link(task3, task4, decider=lambda history: True)

        engine = self._make_engine(flow)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()

        expected = set([
            'task1.t RUNNING',
            'task1.t SUCCESS(5)',

            'task2.t RUNNING',
            'task2.t SUCCESS(5)',

            'task3.t RUNNING',
            'task3.t SUCCESS(5)',

            'task4.t IGNORE',
        ])
        self.assertEqual(expected, set(capturer.values))
        self.assertEqual(states.IGNORE,
                         engine.storage.get_atom_state('task4'))
        self.assertEqual(states.IGNORE,
                         engine.storage.get_atom_intention('task4'))

    def test_graph_flow_conditional_history(self):

        def even_odd_decider(history, allowed):
            total = sum(six.itervalues(history))
            if total == allowed:
                return True
            return False

        flow = gf.Flow('root')

        task1 = utils.TaskMultiArgOneReturn(name='task1')
        task2 = utils.ProgressingTask(name='task2')
        task2_2 = utils.ProgressingTask(name='task2_2')
        task3 = utils.ProgressingTask(name='task3')
        task3_3 = utils.ProgressingTask(name='task3_3')

        flow.add(task1, task2, task2_2, task3, task3_3)
        flow.link(task1, task2,
                  decider=functools.partial(even_odd_decider, allowed=2))
        flow.link(task2, task2_2)

        flow.link(task1, task3,
                  decider=functools.partial(even_odd_decider, allowed=1))
        flow.link(task3, task3_3)

        engine = self._make_engine(flow)
        engine.storage.inject({'x': 0, 'y': 1, 'z': 1})

        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()

        expected = set([
            'task1.t RUNNING', 'task1.t SUCCESS(2)',
            'task3.t IGNORE', 'task3_3.t IGNORE',
            'task2.t RUNNING', 'task2.t SUCCESS(5)',
            'task2_2.t RUNNING', 'task2_2.t SUCCESS(5)',
        ])
        self.assertEqual(expected, set(capturer.values))

        engine = self._make_engine(flow)
        engine.storage.inject({'x': 0, 'y': 0, 'z': 1})
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()

        expected = set([
            'task1.t RUNNING', 'task1.t SUCCESS(1)',
            'task2.t IGNORE', 'task2_2.t IGNORE',
            'task3.t RUNNING', 'task3.t SUCCESS(5)',
            'task3_3.t RUNNING', 'task3_3.t SUCCESS(5)',
        ])
        self.assertEqual(expected, set(capturer.values))


class EngineCheckingTaskTest(utils.EngineTestBase):
    # FIXME: this test uses a inner class that workers/process engines can't
    # get to, so we need to do something better to make this test useful for
    # those engines...

    def test_flow_failures_are_passed_to_revert(self):
        class CheckingTask(task.Task):
            def execute(m_self):
                return 'RESULT'

            def revert(m_self, result, flow_failures):
                self.assertEqual('RESULT', result)
                self.assertEqual(['fail1'], list(flow_failures.keys()))
                fail = flow_failures['fail1']
                self.assertIsInstance(fail, failure.Failure)
                self.assertEqual('Failure: RuntimeError: Woot!', str(fail))

        flow = lf.Flow('test').add(
            CheckingTask(),
            utils.FailingTask('fail1')
        )
        engine = self._make_engine(flow)
        self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)


class SerialEngineTest(EngineTaskTest,
                       EngineMultipleResultsTest,
                       EngineLinearFlowTest,
                       EngineParallelFlowTest,
                       EngineLinearAndUnorderedExceptionsTest,
                       EngineOptionalRequirementsTest,
                       EngineGraphFlowTest,
                       EngineMissingDepsTest,
                       EngineResetTests,
                       EngineGraphConditionalFlowTest,
                       EngineCheckingTaskTest,
                       EngineDeciderDepthTest,
                       EngineTaskNotificationsTest,
                       test.TestCase):
    def _make_engine(self, flow,
                     flow_detail=None, store=None, **kwargs):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine='serial',
                                     backend=self.backend,
                                     store=store, **kwargs)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.SerialActionEngine)

    def test_singlethreaded_is_the_default(self):
        engine = taskflow.engines.load(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.SerialActionEngine)


class ParallelEngineWithThreadsTest(EngineTaskTest,
                                    EngineMultipleResultsTest,
                                    EngineLinearFlowTest,
                                    EngineParallelFlowTest,
                                    EngineLinearAndUnorderedExceptionsTest,
                                    EngineOptionalRequirementsTest,
                                    EngineGraphFlowTest,
                                    EngineResetTests,
                                    EngineMissingDepsTest,
                                    EngineGraphConditionalFlowTest,
                                    EngineCheckingTaskTest,
                                    EngineDeciderDepthTest,
                                    EngineTaskNotificationsTest,
                                    test.TestCase):
    _EXECUTOR_WORKERS = 2

    def _make_engine(self, flow,
                     flow_detail=None, executor=None, store=None,
                     **kwargs):
        if executor is None:
            executor = 'threads'
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     backend=self.backend,
                                     executor=executor,
                                     engine='parallel',
                                     store=store,
                                     max_workers=self._EXECUTOR_WORKERS,
                                     **kwargs)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.ParallelActionEngine)

    def test_using_common_executor(self):
        flow = utils.TaskNoRequiresNoReturns(name='task1')
        executor = futurist.ThreadPoolExecutor(self._EXECUTOR_WORKERS)
        try:
            e1 = self._make_engine(flow, executor=executor)
            e2 = self._make_engine(flow, executor=executor)
            self.assertIs(e1.options['executor'], e2.options['executor'])
        finally:
            executor.shutdown(wait=True)


@testtools.skipIf(not eu.EVENTLET_AVAILABLE, 'eventlet is not available')
class ParallelEngineWithEventletTest(EngineTaskTest,
                                     EngineMultipleResultsTest,
                                     EngineLinearFlowTest,
                                     EngineParallelFlowTest,
                                     EngineLinearAndUnorderedExceptionsTest,
                                     EngineOptionalRequirementsTest,
                                     EngineGraphFlowTest,
                                     EngineResetTests,
                                     EngineMissingDepsTest,
                                     EngineGraphConditionalFlowTest,
                                     EngineCheckingTaskTest,
                                     EngineDeciderDepthTest,
                                     EngineTaskNotificationsTest,
                                     test.TestCase):

    def _make_engine(self, flow,
                     flow_detail=None, executor=None, store=None,
                     **kwargs):
        if executor is None:
            executor = 'greenthreads'
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     backend=self.backend, engine='parallel',
                                     executor=executor,
                                     store=store, **kwargs)


class ParallelEngineWithProcessTest(EngineTaskTest,
                                    EngineMultipleResultsTest,
                                    EngineLinearFlowTest,
                                    EngineParallelFlowTest,
                                    EngineLinearAndUnorderedExceptionsTest,
                                    EngineOptionalRequirementsTest,
                                    EngineGraphFlowTest,
                                    EngineResetTests,
                                    EngineMissingDepsTest,
                                    EngineGraphConditionalFlowTest,
                                    EngineDeciderDepthTest,
                                    EngineTaskNotificationsTest,
                                    test.TestCase):
    _EXECUTOR_WORKERS = 2

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, eng.ParallelActionEngine)

    def _make_engine(self, flow,
                     flow_detail=None, executor=None, store=None,
                     **kwargs):
        if executor is None:
            executor = 'processes'
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     backend=self.backend,
                                     engine='parallel',
                                     executor=executor,
                                     store=store,
                                     max_workers=self._EXECUTOR_WORKERS,
                                     **kwargs)


class WorkerBasedEngineTest(EngineTaskTest,
                            EngineMultipleResultsTest,
                            EngineLinearFlowTest,
                            EngineParallelFlowTest,
                            EngineLinearAndUnorderedExceptionsTest,
                            EngineOptionalRequirementsTest,
                            EngineGraphFlowTest,
                            EngineResetTests,
                            EngineMissingDepsTest,
                            EngineGraphConditionalFlowTest,
                            EngineDeciderDepthTest,
                            EngineTaskNotificationsTest,
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

        # Ensure worker and thread is stopped when test is done; these are
        # called in reverse order, so make sure we signal the stop before
        # performing the join (because the reverse won't work).
        self.addCleanup(self.worker_thread.join)
        self.addCleanup(self.worker.stop)

        # Make sure the worker is started before we can continue...
        self.worker.wait()

    def _make_engine(self, flow,
                     flow_detail=None, store=None, **kwargs):
        kwargs.update(self.engine_conf)
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     backend=self.backend,
                                     store=store, **kwargs)

    def test_correct_load(self):
        engine = self._make_engine(utils.TaskNoRequiresNoReturns)
        self.assertIsInstance(engine, w_eng.WorkerBasedActionEngine)
