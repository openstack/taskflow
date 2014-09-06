# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

import six

from taskflow.engines.action_engine import compiler
from taskflow.engines.action_engine import executor
from taskflow.engines.action_engine import runtime
from taskflow.patterns import linear_flow as lf
from taskflow import states as st
from taskflow import storage
from taskflow import test
from taskflow.tests import utils as test_utils
from taskflow.utils import misc
from taskflow.utils import persistence_utils as pu


class RunnerTest(test.TestCase):
    def _make_runtime(self, flow, initial_state=None):
        compilation = compiler.PatternCompiler().compile(flow)
        flow_detail = pu.create_flow_detail(flow)
        store = storage.SingleThreadedStorage(flow_detail)
        # This ensures the tasks exist in storage...
        for task in compilation.execution_graph:
            store.ensure_task(task.name)
        if initial_state:
            store.set_flow_state(initial_state)
        task_notifier = misc.Notifier()
        task_executor = executor.SerialTaskExecutor()
        task_executor.start()
        self.addCleanup(task_executor.stop)
        return runtime.Runtime(compiler.PatternCompiler().compile(flow),
                               store, task_notifier, task_executor)

    def test_running(self):
        flow = lf.Flow("root")
        flow.add(*test_utils.make_many(1))

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        self.assertTrue(rt.runner.is_running())

        rt = self._make_runtime(flow, initial_state=st.SUSPENDED)
        self.assertFalse(rt.runner.is_running())

    def test_run_iterations(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            1, task_cls=test_utils.TaskNoRequiresNoReturns)
        flow.add(*tasks)

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        self.assertTrue(rt.runner.is_running())

        it = rt.runner.run_iter()
        state, failures = six.next(it)
        self.assertEqual(st.RESUMING, state)
        self.assertEqual(0, len(failures))

        state, failures = six.next(it)
        self.assertEqual(st.SCHEDULING, state)
        self.assertEqual(0, len(failures))

        state, failures = six.next(it)
        self.assertEqual(st.WAITING, state)
        self.assertEqual(0, len(failures))

        state, failures = six.next(it)
        self.assertEqual(st.ANALYZING, state)
        self.assertEqual(0, len(failures))

        state, failures = six.next(it)
        self.assertEqual(st.SUCCESS, state)
        self.assertEqual(0, len(failures))

        self.assertRaises(StopIteration, six.next, it)

    def test_run_iterations_reverted(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            1, task_cls=test_utils.TaskWithFailure)
        flow.add(*tasks)

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        self.assertTrue(rt.runner.is_running())

        transitions = list(rt.runner.run_iter())
        state, failures = transitions[-1]
        self.assertEqual(st.REVERTED, state)
        self.assertEqual([], failures)

        self.assertEqual(st.REVERTED, rt.storage.get_atom_state(tasks[0].name))

    def test_run_iterations_failure(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            1, task_cls=test_utils.NastyFailingTask)
        flow.add(*tasks)

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        self.assertTrue(rt.runner.is_running())

        transitions = list(rt.runner.run_iter())
        state, failures = transitions[-1]
        self.assertEqual(st.FAILURE, state)
        self.assertEqual(1, len(failures))
        failure = failures[0]
        self.assertTrue(failure.check(RuntimeError))

        self.assertEqual(st.FAILURE, rt.storage.get_atom_state(tasks[0].name))

    def test_run_iterations_suspended(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            2, task_cls=test_utils.TaskNoRequiresNoReturns)
        flow.add(*tasks)

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        self.assertTrue(rt.runner.is_running())

        transitions = []
        for state, failures in rt.runner.run_iter():
            transitions.append((state, failures))
            if state == st.ANALYZING:
                rt.storage.set_flow_state(st.SUSPENDED)
        state, failures = transitions[-1]
        self.assertEqual(st.SUSPENDED, state)
        self.assertEqual([], failures)

        self.assertEqual(st.SUCCESS, rt.storage.get_atom_state(tasks[0].name))
        self.assertEqual(st.PENDING, rt.storage.get_atom_state(tasks[1].name))

    def test_run_iterations_suspended_failure(self):
        flow = lf.Flow("root")
        sad_tasks = test_utils.make_many(
            1, task_cls=test_utils.NastyFailingTask)
        flow.add(*sad_tasks)
        happy_tasks = test_utils.make_many(
            1, task_cls=test_utils.TaskNoRequiresNoReturns, offset=1)
        flow.add(*happy_tasks)

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        self.assertTrue(rt.runner.is_running())

        transitions = []
        for state, failures in rt.runner.run_iter():
            transitions.append((state, failures))
            if state == st.ANALYZING:
                rt.storage.set_flow_state(st.SUSPENDED)
        state, failures = transitions[-1]
        self.assertEqual(st.SUSPENDED, state)
        self.assertEqual([], failures)

        self.assertEqual(st.PENDING,
                         rt.storage.get_atom_state(happy_tasks[0].name))
        self.assertEqual(st.FAILURE,
                         rt.storage.get_atom_state(sad_tasks[0].name))
