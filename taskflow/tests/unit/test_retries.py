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
import time

import taskflow.engines
from taskflow.engines.action_engine import executor
from taskflow import exceptions as exc
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow import retry
from taskflow import states as st
from taskflow import test
from taskflow.tests import utils
from taskflow.types import failure
from taskflow.utils import eventlet_utils as eu

try:
    from taskflow.engines.action_engine import process_executor as pe
except ImportError:
    pe = None


class FailingRetry(retry.Retry):

    def execute(self, **kwargs):
        raise ValueError('OMG I FAILED')

    def revert(self, history, **kwargs):
        self.history = history

    def on_failure(self, **kwargs):
        return retry.REVERT


class NastyFailingRetry(FailingRetry):
    def revert(self, history, **kwargs):
        raise ValueError('WOOT!')


class RetryTest(utils.EngineTestBase):

    def test_run_empty_linear_flow(self):
        flow = lf.Flow('flow-1', utils.OneReturnRetry(provides='x'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual({'x': 1}, engine.storage.fetch_all())

    def test_run_empty_unordered_flow(self):
        flow = uf.Flow('flow-1', utils.OneReturnRetry(provides='x'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual({'x': 1}, engine.storage.fetch_all())

    def test_run_empty_graph_flow(self):
        flow = gf.Flow('flow-1', utils.OneReturnRetry(provides='x'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual({'x': 1}, engine.storage.fetch_all())

    def test_states_retry_success_linear_flow(self):
        flow = lf.Flow('flow-1', retry.Times(4, 'r1', provides='x')).add(
            utils.ProgressingTask("task1"),
            utils.ConditionalTask("task2")
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        self.assertEqual({'y': 2, 'x': 2}, engine.storage.fetch_all())
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING', 'r1.r SUCCESS(1)',
                    'task1.t RUNNING', 'task1.t SUCCESS(5)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task2.t REVERTING', 'task2.t REVERTED(None)',
                    'task1.t REVERTING', 'task1.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task1.t PENDING',
                    'task2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'task2.t RUNNING',
                    'task2.t SUCCESS(None)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_states_retry_reverted_linear_flow(self):
        flow = lf.Flow('flow-1', retry.Times(2, 'r1', provides='x')).add(
            utils.ProgressingTask("task1"),
            utils.ConditionalTask("task2")
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 4})
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        self.assertEqual({'y': 4}, engine.storage.fetch_all())
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task1.t PENDING',
                    'task2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_states_retry_failure_linear_flow(self):
        flow = lf.Flow('flow-1', retry.Times(2, 'r1', provides='x')).add(
            utils.NastyTask("task1"),
            utils.ConditionalTask("task2")
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 4})
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Gotcha', engine.run)
        self.assertEqual({'y': 4, 'x': 1}, engine.storage.fetch_all())
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(None)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'task1.t REVERTING',
                    'task1.t REVERT_FAILURE(Failure: RuntimeError: Gotcha!)',
                    'flow-1.f FAILURE']
        self.assertEqual(expected, capturer.values)

    def test_states_retry_failure_nested_flow_fails(self):
        flow = lf.Flow('flow-1', utils.retry.AlwaysRevert('r1')).add(
            utils.TaskNoRequiresNoReturns("task1"),
            lf.Flow('flow-2', retry.Times(3, 'r2', provides='x')).add(
                utils.TaskNoRequiresNoReturns("task2"),
                utils.ConditionalTask("task3")
            ),
            utils.TaskNoRequiresNoReturns("task4")
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        self.assertEqual({'y': 2, 'x': 2}, engine.storage.fetch_all())
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(None)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(None)',
                    'r2.r RUNNING',
                    'r2.r SUCCESS(1)',
                    'task2.t RUNNING',
                    'task2.t SUCCESS(None)',
                    'task3.t RUNNING',
                    'task3.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task3.t REVERTING',
                    'task3.t REVERTED(None)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r2.r RETRYING',
                    'task2.t PENDING',
                    'task3.t PENDING',
                    'r2.r RUNNING',
                    'r2.r SUCCESS(2)',
                    'task2.t RUNNING',
                    'task2.t SUCCESS(None)',
                    'task3.t RUNNING',
                    'task3.t SUCCESS(None)',
                    'task4.t RUNNING',
                    'task4.t SUCCESS(None)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_new_revert_vs_old(self):
        flow = lf.Flow('flow-1').add(
            utils.TaskNoRequiresNoReturns("task1"),
            lf.Flow('flow-2', retry.Times(1, 'r1', provides='x')).add(
                utils.TaskNoRequiresNoReturns("task2"),
                utils.ConditionalTask("task3")
            ),
            utils.TaskNoRequiresNoReturns("task4")
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine) as capturer:
            try:
                engine.run()
            except Exception:
                pass

        expected = ['flow-1.f RUNNING',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(None)',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    'task2.t RUNNING',
                    'task2.t SUCCESS(None)',
                    'task3.t RUNNING',
                    'task3.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task3.t REVERTING',
                    'task3.t REVERTED(None)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

        engine = self._make_engine(flow, defer_reverts=True)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine) as capturer:
            try:
                engine.run()
            except Exception:
                pass

        expected = ['flow-1.f RUNNING',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(None)',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    'task2.t RUNNING',
                    'task2.t SUCCESS(None)',
                    'task3.t RUNNING',
                    'task3.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task3.t REVERTING',
                    'task3.t REVERTED(None)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_states_retry_failure_parent_flow_fails(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1', provides='x1')).add(
            utils.TaskNoRequiresNoReturns("task1"),
            lf.Flow('flow-2', retry.Times(3, 'r2', provides='x2')).add(
                utils.TaskNoRequiresNoReturns("task2"),
                utils.TaskNoRequiresNoReturns("task3")
            ),
            utils.ConditionalTask("task4", rebind={'x': 'x1'})
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        self.assertEqual({'y': 2, 'x1': 2,
                          'x2': 1},
                         engine.storage.fetch_all())
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(None)',
                    'r2.r RUNNING',
                    'r2.r SUCCESS(1)',
                    'task2.t RUNNING',
                    'task2.t SUCCESS(None)',
                    'task3.t RUNNING',
                    'task3.t SUCCESS(None)',
                    'task4.t RUNNING',
                    'task4.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task4.t REVERTING',
                    'task4.t REVERTED(None)',
                    'task3.t REVERTING',
                    'task3.t REVERTED(None)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r2.r REVERTING',
                    'r2.r REVERTED(None)',
                    'task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task1.t PENDING',
                    'r2.r PENDING',
                    'task2.t PENDING',
                    'task3.t PENDING',
                    'task4.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(None)',
                    'r2.r RUNNING',
                    'r2.r SUCCESS(1)',
                    'task2.t RUNNING',
                    'task2.t SUCCESS(None)',
                    'task3.t RUNNING',
                    'task3.t SUCCESS(None)',
                    'task4.t RUNNING',
                    'task4.t SUCCESS(None)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_unordered_flow_task_fails_parallel_tasks_should_be_reverted(self):
        flow = uf.Flow('flow-1', retry.Times(3, 'r', provides='x')).add(
            utils.ProgressingTask("task1"),
            utils.ConditionalTask("task2")
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        self.assertEqual({'y': 2, 'x': 2}, engine.storage.fetch_all())
        expected = ['flow-1.f RUNNING',
                    'r.r RUNNING',
                    'r.r SUCCESS(1)',
                    'task1.t RUNNING',
                    'task2.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'task2.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task2.t REVERTING',
                    'task1.t REVERTING',
                    'task2.t REVERTED(None)',
                    'task1.t REVERTED(None)',
                    'r.r RETRYING',
                    'task1.t PENDING',
                    'task2.t PENDING',
                    'r.r RUNNING',
                    'r.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task2.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'task2.t SUCCESS(None)',
                    'flow-1.f SUCCESS']
        self.assertCountEqual(capturer.values, expected)

    def test_nested_flow_reverts_parent_retries(self):
        retry1 = retry.Times(3, 'r1', provides='x')
        retry2 = retry.Times(0, 'r2', provides='x2')
        flow = lf.Flow('flow-1', retry1).add(
            utils.ProgressingTask("task1"),
            lf.Flow('flow-2', retry2).add(utils.ConditionalTask("task2"))
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        self.assertEqual({'y': 2, 'x': 2, 'x2': 1}, engine.storage.fetch_all())
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'r2.r RUNNING',
                    'r2.r SUCCESS(1)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r2.r REVERTING',
                    'r2.r REVERTED(None)',
                    'task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task1.t PENDING',
                    'r2.r PENDING',
                    'task2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'r2.r RUNNING',
                    'r2.r SUCCESS(1)',
                    'task2.t RUNNING',
                    'task2.t SUCCESS(None)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_nested_flow_with_retry_revert(self):
        retry1 = retry.Times(0, 'r1', provides='x2')
        flow = lf.Flow('flow-1').add(
            utils.ProgressingTask("task1"),
            lf.Flow('flow-2', retry1).add(
                utils.ConditionalTask("task2", inject={'x': 1}))
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine) as capturer:
            try:
                engine.run()
            except Exception:
                pass
        self.assertEqual({'y': 2}, engine.storage.fetch_all())
        expected = ['flow-1.f RUNNING',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_nested_flow_with_retry_revert_all(self):
        retry1 = retry.Times(0, 'r1', provides='x2', revert_all=True)
        flow = lf.Flow('flow-1').add(
            utils.ProgressingTask("task1"),
            lf.Flow('flow-2', retry1).add(
                utils.ConditionalTask("task2", inject={'x': 1}))
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine) as capturer:
            try:
                engine.run()
            except Exception:
                pass
        self.assertEqual({'y': 2}, engine.storage.fetch_all())
        expected = ['flow-1.f RUNNING',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_revert_all_retry(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1', provides='x')).add(
            utils.ProgressingTask("task1"),
            lf.Flow('flow-2', retry.AlwaysRevertAll('r2')).add(
                utils.ConditionalTask("task2"))
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        self.assertEqual({'y': 2}, engine.storage.fetch_all())
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'r2.r RUNNING',
                    'r2.r SUCCESS(None)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r2.r REVERTING',
                    'r2.r REVERTED(None)',
                    'task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_restart_reverted_flow_with_retry(self):
        flow = lf.Flow('test', retry=utils.OneReturnRetry(provides='x')).add(
            utils.FailingTask('fail'))
        engine = self._make_engine(flow)
        self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)

    def test_restart_reverted_unordered_flows_with_retries(self):
        now = time.time()

        # First flow of an unordered flow:
        subflow1 = lf.Flow('subflow1')

        # * a task that completes in 3 sec with a few retries
        subsubflow1 = lf.Flow('subflow1.subsubflow1',
                              retry=utils.RetryFiveTimes())
        subsubflow1.add(utils.SuccessAfter3Sec('subflow1.fail1',
                                               inject={'start_time': now}))
        subflow1.add(subsubflow1)

        # * a task that fails and triggers a revert after 5 retries
        subsubflow2 = lf.Flow('subflow1.subsubflow2',
                              retry=utils.RetryFiveTimes())
        subsubflow2.add(utils.FailingTask('subflow1.fail2'))
        subflow1.add(subsubflow2)

        # Second flow of the unordered flow:
        subflow2 = lf.Flow('subflow2')

        # * a task that always fails and retries
        subsubflow1 = lf.Flow('subflow2.subsubflow1',
                              retry=utils.AlwaysRetry())
        subsubflow1.add(utils.FailingTask('subflow2.fail1'))
        subflow2.add(subsubflow1)

        unordered_flow = uf.Flow('unordered_flow')
        unordered_flow.add(subflow1, subflow2)

        # Main flow, contains a simple task and an unordered flow
        flow = lf.Flow('test')
        flow.add(utils.NoopTask('task1'))
        flow.add(unordered_flow)

        engine = self._make_engine(flow)

        # This test fails when using Green threads, skipping it for now
        if isinstance(engine._task_executor,
                      executor.ParallelGreenThreadTaskExecutor):
            self.skipTest("Skipping this test when using green threads.")

        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(exc.WrappedFailure,
                                   '.*RuntimeError: Woot!',
                                   engine.run)
            # task1 should have been reverted
            self.assertIn('task1.t REVERTED(None)', capturer.values)

    def test_run_just_retry(self):
        flow = utils.OneReturnRetry(provides='x')
        engine = self._make_engine(flow)
        self.assertRaises(TypeError, engine.run)

    def test_use_retry_as_a_task(self):
        flow = lf.Flow('test').add(utils.OneReturnRetry(provides='x'))
        engine = self._make_engine(flow)
        self.assertRaises(TypeError, engine.run)

    def test_resume_flow_that_had_been_interrupted_during_retrying(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1')).add(
            utils.ProgressingTask('t1'),
            utils.ProgressingTask('t2'),
            utils.ProgressingTask('t3')
        )
        engine = self._make_engine(flow)
        engine.compile()
        engine.prepare()
        with utils.CaptureListener(engine) as capturer:
            engine.storage.set_atom_state('r1', st.RETRYING)
            engine.storage.set_atom_state('t1', st.PENDING)
            engine.storage.set_atom_state('t2', st.REVERTED)
            engine.storage.set_atom_state('t3', st.REVERTED)
            engine.run()
        expected = ['flow-1.f RUNNING',
                    't2.t PENDING',
                    't3.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    't1.t RUNNING',
                    't1.t SUCCESS(5)',
                    't2.t RUNNING',
                    't2.t SUCCESS(5)',
                    't3.t RUNNING',
                    't3.t SUCCESS(5)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_resume_flow_that_should_be_retried(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1')).add(
            utils.ProgressingTask('t1'),
            utils.ProgressingTask('t2')
        )
        engine = self._make_engine(flow)
        engine.compile()
        engine.prepare()
        with utils.CaptureListener(engine) as capturer:
            engine.storage.set_atom_intention('r1', st.RETRY)
            engine.storage.set_atom_state('r1', st.SUCCESS)
            engine.storage.set_atom_state('t1', st.REVERTED)
            engine.storage.set_atom_state('t2', st.REVERTED)
            engine.run()
        expected = ['flow-1.f RUNNING',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    't2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    't1.t RUNNING',
                    't1.t SUCCESS(5)',
                    't2.t RUNNING',
                    't2.t SUCCESS(5)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_retry_tasks_that_has_not_been_reverted(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1', provides='x')).add(
            utils.ConditionalTask('c'),
            utils.ProgressingTask('t1')
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    'c.t RUNNING',
                    'c.t FAILURE(Failure: RuntimeError: Woot!)',
                    'c.t REVERTING',
                    'c.t REVERTED(None)',
                    'r1.r RETRYING',
                    'c.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    'c.t RUNNING',
                    'c.t SUCCESS(None)',
                    't1.t RUNNING',
                    't1.t SUCCESS(5)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_default_times_retry(self):
        flow = lf.Flow('flow-1', retry.Times(3, 'r1')).add(
            utils.ProgressingTask('t1'),
            utils.FailingTask('t2'))
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(1)',
                    't1.t RUNNING',
                    't1.t SUCCESS(5)',
                    't2.t RUNNING',
                    't2.t FAILURE(Failure: RuntimeError: Woot!)',
                    't2.t REVERTING',
                    't2.t REVERTED(None)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    't2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    't1.t RUNNING',
                    't1.t SUCCESS(5)',
                    't2.t RUNNING',
                    't2.t FAILURE(Failure: RuntimeError: Woot!)',
                    't2.t REVERTING',
                    't2.t REVERTED(None)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    't2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    't1.t RUNNING',
                    't1.t SUCCESS(5)',
                    't2.t RUNNING',
                    't2.t FAILURE(Failure: RuntimeError: Woot!)',
                    't2.t REVERTING',
                    't2.t REVERTED(None)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_for_each_with_list(self):
        collection = [3, 2, 3, 5]
        retry1 = retry.ForEach(collection, 'r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.FailingTaskWithOneArg('t1'))
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 3)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 2)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 3)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(5)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 5)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_for_each_with_set(self):
        collection = set([3, 2, 5])
        retry1 = retry.ForEach(collection, 'r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.FailingTaskWithOneArg('t1'))
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 2)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 3)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(5)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 5)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertCountEqual(capturer.values, expected)

    def test_nested_for_each_revert(self):
        collection = [3, 2, 3, 5]
        retry1 = retry.ForEach(collection, 'r1', provides='x')
        flow = lf.Flow('flow-1').add(
            utils.ProgressingTask("task1"),
            lf.Flow('flow-2', retry1).add(
                utils.FailingTaskWithOneArg('task2')
            )
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        expected = ['flow-1.f RUNNING',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot with 3)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot with 2)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot with 3)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(5)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot with 5)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_nested_for_each_revert_all(self):
        collection = [3, 2, 3, 5]
        retry1 = retry.ForEach(collection, 'r1', provides='x', revert_all=True)
        flow = lf.Flow('flow-1').add(
            utils.ProgressingTask("task1"),
            lf.Flow('flow-2', retry1).add(
                utils.FailingTaskWithOneArg('task2')
            )
        )
        engine = self._make_engine(flow)
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        expected = ['flow-1.f RUNNING',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot with 3)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot with 2)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot with 3)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(5)',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot with 5)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_for_each_empty_collection(self):
        values = []
        retry1 = retry.ForEach(values, 'r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.ConditionalTask('t1'))
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 1})
        self.assertRaisesRegex(exc.NotFound, '^No elements left', engine.run)

    def test_parameterized_for_each_with_list(self):
        values = [3, 2, 5]
        retry1 = retry.ParameterizedForEach('r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.FailingTaskWithOneArg('t1'))
        engine = self._make_engine(flow)
        engine.storage.inject({'values': values, 'y': 1})
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 3)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 2)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(5)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 5)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_parameterized_for_each_with_set(self):
        values = ([3, 2, 5])
        retry1 = retry.ParameterizedForEach('r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.FailingTaskWithOneArg('t1'))
        engine = self._make_engine(flow)
        engine.storage.inject({'values': values, 'y': 1})
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        expected = ['flow-1.f RUNNING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 3)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 2)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r RETRYING',
                    't1.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(5)',
                    't1.t RUNNING',
                    't1.t FAILURE(Failure: RuntimeError: Woot with 5)',
                    't1.t REVERTING',
                    't1.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertCountEqual(capturer.values, expected)

    def test_nested_parameterized_for_each_revert(self):
        values = [3, 2, 5]
        retry1 = retry.ParameterizedForEach('r1', provides='x')
        flow = lf.Flow('flow-1').add(
            utils.ProgressingTask('task-1'),
            lf.Flow('flow-2', retry1).add(
                utils.FailingTaskWithOneArg('task-2')
            )
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'values': values, 'y': 1})
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        expected = ['flow-1.f RUNNING',
                    'task-1.t RUNNING',
                    'task-1.t SUCCESS(5)',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    'task-2.t RUNNING',
                    'task-2.t FAILURE(Failure: RuntimeError: Woot with 3)',
                    'task-2.t REVERTING',
                    'task-2.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task-2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    'task-2.t RUNNING',
                    'task-2.t FAILURE(Failure: RuntimeError: Woot with 2)',
                    'task-2.t REVERTING',
                    'task-2.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task-2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(5)',
                    'task-2.t RUNNING',
                    'task-2.t FAILURE(Failure: RuntimeError: Woot with 5)',
                    'task-2.t REVERTING',
                    'task-2.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_nested_parameterized_for_each_revert_all(self):
        values = [3, 2, 5]
        retry1 = retry.ParameterizedForEach('r1', provides='x',
                                            revert_all=True)
        flow = lf.Flow('flow-1').add(
            utils.ProgressingTask('task-1'),
            lf.Flow('flow-2', retry1).add(
                utils.FailingTaskWithOneArg('task-2')
            )
        )
        engine = self._make_engine(flow)
        engine.storage.inject({'values': values, 'y': 1})
        with utils.CaptureListener(engine) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        expected = ['flow-1.f RUNNING',
                    'task-1.t RUNNING',
                    'task-1.t SUCCESS(5)',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(3)',
                    'task-2.t RUNNING',
                    'task-2.t FAILURE(Failure: RuntimeError: Woot with 3)',
                    'task-2.t REVERTING',
                    'task-2.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task-2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(2)',
                    'task-2.t RUNNING',
                    'task-2.t FAILURE(Failure: RuntimeError: Woot with 2)',
                    'task-2.t REVERTING',
                    'task-2.t REVERTED(None)',
                    'r1.r RETRYING',
                    'task-2.t PENDING',
                    'r1.r RUNNING',
                    'r1.r SUCCESS(5)',
                    'task-2.t RUNNING',
                    'task-2.t FAILURE(Failure: RuntimeError: Woot with 5)',
                    'task-2.t REVERTING',
                    'task-2.t REVERTED(None)',
                    'r1.r REVERTING',
                    'r1.r REVERTED(None)',
                    'task-1.t REVERTING',
                    'task-1.t REVERTED(None)',
                    'flow-1.f REVERTED']
        self.assertEqual(expected, capturer.values)

    def test_parameterized_for_each_empty_collection(self):
        values = []
        retry1 = retry.ParameterizedForEach('r1', provides='x')
        flow = lf.Flow('flow-1', retry1).add(utils.ConditionalTask('t1'))
        engine = self._make_engine(flow)
        engine.storage.inject({'values': values, 'y': 1})
        self.assertRaisesRegex(exc.NotFound, '^No elements left', engine.run)

    def _pretend_to_run_a_flow_and_crash(self, when):
        flow = uf.Flow('flow-1', retry.Times(3, provides='x')).add(
            utils.ProgressingTask('task1'))
        engine = self._make_engine(flow)
        engine.compile()
        engine.prepare()
        # imagine we run engine
        engine.storage.set_flow_state(st.RUNNING)
        engine.storage.set_atom_intention('flow-1_retry', st.EXECUTE)
        engine.storage.set_atom_intention('task1', st.EXECUTE)
        # we execute retry
        engine.storage.save('flow-1_retry', 1)
        # task fails
        fail = failure.Failure.from_exception(RuntimeError('foo'))
        engine.storage.save('task1', fail, state=st.FAILURE)
        if when == 'task fails':
            return engine
        # we save it's failure to retry and ask what to do
        engine.storage.save_retry_failure('flow-1_retry', 'task1', fail)
        if when == 'retry queried':
            return engine
        # it returned 'RETRY', so we update it's intention
        engine.storage.set_atom_intention('flow-1_retry', st.RETRY)
        if when == 'retry updated':
            return engine
        # we set task1 intention to REVERT
        engine.storage.set_atom_intention('task1', st.REVERT)
        if when == 'task updated':
            return engine
        # we schedule task1 for reversion
        engine.storage.set_atom_state('task1', st.REVERTING)
        if when == 'revert scheduled':
            return engine
        raise ValueError('Invalid crash point: %s' % when)

    def test_resumption_on_crash_after_task_failure(self):
        engine = self._pretend_to_run_a_flow_and_crash('task fails')
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        expected = ['task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'flow-1_retry.r RETRYING',
                    'task1.t PENDING',
                    'flow-1_retry.r RUNNING',
                    'flow-1_retry.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_resumption_on_crash_after_retry_queried(self):
        engine = self._pretend_to_run_a_flow_and_crash('retry queried')
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        expected = ['task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'flow-1_retry.r RETRYING',
                    'task1.t PENDING',
                    'flow-1_retry.r RUNNING',
                    'flow-1_retry.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_resumption_on_crash_after_retry_updated(self):
        engine = self._pretend_to_run_a_flow_and_crash('retry updated')
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        expected = ['task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'flow-1_retry.r RETRYING',
                    'task1.t PENDING',
                    'flow-1_retry.r RUNNING',
                    'flow-1_retry.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_resumption_on_crash_after_task_updated(self):
        engine = self._pretend_to_run_a_flow_and_crash('task updated')
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        expected = ['task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'flow-1_retry.r RETRYING',
                    'task1.t PENDING',
                    'flow-1_retry.r RUNNING',
                    'flow-1_retry.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_resumption_on_crash_after_revert_scheduled(self):
        engine = self._pretend_to_run_a_flow_and_crash('revert scheduled')
        with utils.CaptureListener(engine) as capturer:
            engine.run()
        expected = ['task1.t REVERTED(None)',
                    'flow-1_retry.r RETRYING',
                    'task1.t PENDING',
                    'flow-1_retry.r RUNNING',
                    'flow-1_retry.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'flow-1.f SUCCESS']
        self.assertEqual(expected, capturer.values)

    def test_retry_fails(self):
        r = FailingRetry()
        flow = lf.Flow('testflow', r)
        engine = self._make_engine(flow)
        self.assertRaisesRegex(ValueError, '^OMG', engine.run)
        self.assertEqual(1, len(engine.storage.get_retry_histories()))
        self.assertEqual(0, len(r.history))
        self.assertEqual([], list(r.history.outcomes_iter()))
        self.assertIsNotNone(r.history.failure)
        self.assertTrue(r.history.caused_by(ValueError, include_retry=True))

    def test_retry_revert_fails(self):
        r = NastyFailingRetry()
        flow = lf.Flow('testflow', r)
        engine = self._make_engine(flow)
        self.assertRaisesRegex(ValueError, '^WOOT', engine.run)

    def test_nested_provides_graph_reverts_correctly(self):
        flow = gf.Flow("test").add(
            utils.ProgressingTask('a', requires=['x']),
            lf.Flow("test2", retry=retry.Times(2)).add(
                utils.ProgressingTask('b', provides='x'),
                utils.FailingTask('c')))
        engine = self._make_engine(flow)
        engine.compile()
        engine.prepare()
        engine.storage.save('test2_retry', 1)
        engine.storage.save('b', 11)
        engine.storage.save('a', 10)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            self.assertRaisesRegex(RuntimeError, '^Woot', engine.run)
        expected = ['c.t RUNNING',
                    'c.t FAILURE(Failure: RuntimeError: Woot!)',
                    'a.t REVERTING',
                    'c.t REVERTING',
                    'a.t REVERTED(None)',
                    'c.t REVERTED(None)',
                    'b.t REVERTING',
                    'b.t REVERTED(None)']
        self.assertCountEqual(capturer.values[:8], expected)
        # Task 'a' was or was not executed again, both cases are ok.
        self.assertIsSuperAndSubsequence(capturer.values[8:], [
            'b.t RUNNING',
            'c.t FAILURE(Failure: RuntimeError: Woot!)',
            'b.t REVERTED(None)',
        ])
        self.assertEqual(st.REVERTED, engine.storage.get_flow_state())

    def test_nested_provides_graph_retried_correctly(self):
        flow = gf.Flow("test").add(
            utils.ProgressingTask('a', requires=['x']),
            lf.Flow("test2", retry=retry.Times(2)).add(
                utils.ProgressingTask('b', provides='x'),
                utils.ProgressingTask('c')))
        engine = self._make_engine(flow)
        engine.compile()
        engine.prepare()
        engine.storage.save('test2_retry', 1)
        engine.storage.save('b', 11)
        # pretend that 'c' failed
        fail = failure.Failure.from_exception(RuntimeError('Woot!'))
        engine.storage.save('c', fail, st.FAILURE)
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        expected = ['c.t REVERTING',
                    'c.t REVERTED(None)',
                    'b.t REVERTING',
                    'b.t REVERTED(None)']
        self.assertCountEqual(capturer.values[:4], expected)
        expected = ['test2_retry.r RETRYING',
                    'b.t PENDING',
                    'c.t PENDING',
                    'test2_retry.r RUNNING',
                    'test2_retry.r SUCCESS(2)',
                    'b.t RUNNING',
                    'b.t SUCCESS(5)',
                    'a.t RUNNING',
                    'c.t RUNNING',
                    'a.t SUCCESS(5)',
                    'c.t SUCCESS(5)']
        self.assertCountEqual(expected, capturer.values[4:])
        self.assertEqual(st.SUCCESS, engine.storage.get_flow_state())


class RetryParallelExecutionTest(utils.EngineTestBase):
    # FIXME(harlowja): fix this class so that it doesn't use events or uses
    # them in a way that works with more executors...

    def test_when_subflow_fails_revert_running_tasks(self):
        waiting_task = utils.WaitForOneFromTask('task1', 'task2',
                                                [st.SUCCESS, st.FAILURE])
        flow = uf.Flow('flow-1', retry.Times(3, 'r', provides='x')).add(
            waiting_task,
            utils.ConditionalTask('task2')
        )
        engine = self._make_engine(flow)
        engine.atom_notifier.register('*', waiting_task.callback)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        self.assertEqual({'y': 2, 'x': 2}, engine.storage.fetch_all())
        expected = ['r.r RUNNING',
                    'r.r SUCCESS(1)',
                    'task1.t RUNNING',
                    'task2.t RUNNING',
                    'task2.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'task1.t SUCCESS(5)',
                    'task1.t REVERTING',
                    'task1.t REVERTED(None)',
                    'r.r RETRYING',
                    'task1.t PENDING',
                    'task2.t PENDING',
                    'r.r RUNNING',
                    'r.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task2.t RUNNING',
                    'task2.t SUCCESS(None)',
                    'task1.t SUCCESS(5)']
        self.assertCountEqual(capturer.values, expected)

    def test_when_subflow_fails_revert_success_tasks(self):
        waiting_task = utils.WaitForOneFromTask('task2', 'task1',
                                                [st.SUCCESS, st.FAILURE])
        flow = uf.Flow('flow-1', retry.Times(3, 'r', provides='x')).add(
            utils.ProgressingTask('task1'),
            lf.Flow('flow-2').add(
                waiting_task,
                utils.ConditionalTask('task3'))
        )
        engine = self._make_engine(flow)
        engine.atom_notifier.register('*', waiting_task.callback)
        engine.storage.inject({'y': 2})
        with utils.CaptureListener(engine, capture_flow=False) as capturer:
            engine.run()
        self.assertEqual({'y': 2, 'x': 2}, engine.storage.fetch_all())
        expected = ['r.r RUNNING',
                    'r.r SUCCESS(1)',
                    'task1.t RUNNING',
                    'task2.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'task2.t SUCCESS(5)',
                    'task3.t RUNNING',
                    'task3.t FAILURE(Failure: RuntimeError: Woot!)',
                    'task3.t REVERTING',
                    'task1.t REVERTING',
                    'task3.t REVERTED(None)',
                    'task1.t REVERTED(None)',
                    'task2.t REVERTING',
                    'task2.t REVERTED(None)',
                    'r.r RETRYING',
                    'task1.t PENDING',
                    'task2.t PENDING',
                    'task3.t PENDING',
                    'r.r RUNNING',
                    'r.r SUCCESS(2)',
                    'task1.t RUNNING',
                    'task2.t RUNNING',
                    'task1.t SUCCESS(5)',
                    'task2.t SUCCESS(5)',
                    'task3.t RUNNING',
                    'task3.t SUCCESS(None)']
        self.assertCountEqual(capturer.values, expected)


class SerialEngineTest(RetryTest, test.TestCase):
    def _make_engine(self, flow, defer_reverts=None, flow_detail=None):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine='serial',
                                     backend=self.backend,
                                     defer_reverts=defer_reverts)


class ParallelEngineWithThreadsTest(RetryTest,
                                    RetryParallelExecutionTest,
                                    test.TestCase):
    _EXECUTOR_WORKERS = 2

    def _make_engine(self, flow, defer_reverts=None, flow_detail=None,
                     executor=None):
        if executor is None:
            executor = 'threads'
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine='parallel',
                                     backend=self.backend,
                                     executor=executor,
                                     max_workers=self._EXECUTOR_WORKERS,
                                     defer_reverts=defer_reverts)


@testtools.skipIf(not eu.EVENTLET_AVAILABLE, 'eventlet is not available')
class ParallelEngineWithEventletTest(RetryTest, test.TestCase):

    def _make_engine(self, flow, defer_reverts=None, flow_detail=None,
                     executor=None):
        if executor is None:
            executor = 'greenthreads'
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     backend=self.backend,
                                     engine='parallel',
                                     executor=executor,
                                     defer_reverts=defer_reverts)


@testtools.skipIf(pe is None, 'process_executor is not available')
class ParallelEngineWithProcessTest(RetryTest, test.TestCase):
    _EXECUTOR_WORKERS = 2

    def _make_engine(self, flow, defer_reverts=None, flow_detail=None,
                     executor=None):
        if executor is None:
            executor = 'processes'
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine='parallel',
                                     backend=self.backend,
                                     executor=executor,
                                     max_workers=self._EXECUTOR_WORKERS,
                                     defer_reverts=defer_reverts)
