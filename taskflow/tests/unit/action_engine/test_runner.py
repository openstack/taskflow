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
from taskflow.engines.action_engine import runner
from taskflow.engines.action_engine import runtime
from taskflow import exceptions as excp
from taskflow.patterns import linear_flow as lf
from taskflow import states as st
from taskflow import storage
from taskflow import test
from taskflow.tests import utils as test_utils
from taskflow.types import fsm
from taskflow.types import notifier
from taskflow.utils import persistence_utils as pu


class _RunnerTestMixin(object):
    def _make_runtime(self, flow, initial_state=None):
        compilation = compiler.PatternCompiler(flow).compile()
        flow_detail = pu.create_flow_detail(flow)
        store = storage.Storage(flow_detail)
        # This ensures the tasks exist in storage...
        for task in compilation.execution_graph:
            store.ensure_atom(task)
        if initial_state:
            store.set_flow_state(initial_state)
        task_notifier = notifier.Notifier()
        task_executor = executor.SerialTaskExecutor()
        task_executor.start()
        self.addCleanup(task_executor.stop)
        return runtime.Runtime(compilation, store,
                               task_notifier, task_executor)


class RunnerTest(test.TestCase, _RunnerTestMixin):
    def test_running(self):
        flow = lf.Flow("root")
        flow.add(*test_utils.make_many(1))

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        self.assertTrue(rt.runner.runnable())

        rt = self._make_runtime(flow, initial_state=st.SUSPENDED)
        self.assertFalse(rt.runner.runnable())

    def test_run_iterations(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            1, task_cls=test_utils.TaskNoRequiresNoReturns)
        flow.add(*tasks)

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        self.assertTrue(rt.runner.runnable())

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
        self.assertTrue(rt.runner.runnable())

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
        self.assertTrue(rt.runner.runnable())

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
        self.assertTrue(rt.runner.runnable())

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
        self.assertTrue(rt.runner.runnable())

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


class RunnerBuilderTest(test.TestCase, _RunnerTestMixin):
    def test_builder_manual_process(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            1, task_cls=test_utils.TaskNoRequiresNoReturns)
        flow.add(*tasks)

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        machine, memory = rt.runner.builder.build()
        self.assertTrue(rt.runner.builder.runnable())
        self.assertRaises(fsm.NotInitialized, machine.process_event, 'poke')

        # Should now be pending...
        self.assertEqual(st.PENDING, rt.storage.get_atom_state(tasks[0].name))

        machine.initialize()
        self.assertEqual(runner._UNDEFINED, machine.current_state)
        self.assertFalse(machine.terminated)
        self.assertRaises(excp.NotFound, machine.process_event, 'poke')
        last_state = machine.current_state

        reaction, terminal = machine.process_event('start')
        self.assertFalse(terminal)
        self.assertIsNotNone(reaction)
        self.assertEqual(st.RESUMING, machine.current_state)
        self.assertRaises(excp.NotFound, machine.process_event, 'poke')

        last_state = machine.current_state
        cb, args, kwargs = reaction
        next_event = cb(last_state, machine.current_state,
                        'start', *args, **kwargs)
        reaction, terminal = machine.process_event(next_event)
        self.assertFalse(terminal)
        self.assertIsNotNone(reaction)
        self.assertEqual(st.SCHEDULING, machine.current_state)
        self.assertRaises(excp.NotFound, machine.process_event, 'poke')

        last_state = machine.current_state
        cb, args, kwargs = reaction
        next_event = cb(last_state, machine.current_state,
                        next_event, *args, **kwargs)
        reaction, terminal = machine.process_event(next_event)
        self.assertFalse(terminal)
        self.assertEqual(st.WAITING, machine.current_state)
        self.assertRaises(excp.NotFound, machine.process_event, 'poke')

        # Should now be running...
        self.assertEqual(st.RUNNING, rt.storage.get_atom_state(tasks[0].name))

        last_state = machine.current_state
        cb, args, kwargs = reaction
        next_event = cb(last_state, machine.current_state,
                        next_event, *args, **kwargs)
        reaction, terminal = machine.process_event(next_event)
        self.assertFalse(terminal)
        self.assertIsNotNone(reaction)
        self.assertEqual(st.ANALYZING, machine.current_state)
        self.assertRaises(excp.NotFound, machine.process_event, 'poke')

        last_state = machine.current_state
        cb, args, kwargs = reaction
        next_event = cb(last_state, machine.current_state,
                        next_event, *args, **kwargs)
        reaction, terminal = machine.process_event(next_event)
        self.assertFalse(terminal)
        self.assertEqual(runner._GAME_OVER, machine.current_state)

        # Should now be done...
        self.assertEqual(st.SUCCESS, rt.storage.get_atom_state(tasks[0].name))

    def test_builder_automatic_process(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            1, task_cls=test_utils.TaskNoRequiresNoReturns)
        flow.add(*tasks)

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        machine, memory = rt.runner.builder.build()
        self.assertTrue(rt.runner.builder.runnable())

        transitions = list(machine.run_iter('start'))
        self.assertEqual((runner._UNDEFINED, st.RESUMING), transitions[0])
        self.assertEqual((runner._GAME_OVER, st.SUCCESS), transitions[-1])
        self.assertEqual(st.SUCCESS, rt.storage.get_atom_state(tasks[0].name))

    def test_builder_automatic_process_failure(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(1, task_cls=test_utils.NastyFailingTask)
        flow.add(*tasks)

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        machine, memory = rt.runner.builder.build()
        self.assertTrue(rt.runner.builder.runnable())

        transitions = list(machine.run_iter('start'))
        self.assertEqual((runner._GAME_OVER, st.FAILURE), transitions[-1])
        self.assertEqual(1, len(memory.failures))

    def test_builder_automatic_process_reverted(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(1, task_cls=test_utils.TaskWithFailure)
        flow.add(*tasks)

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        machine, memory = rt.runner.builder.build()
        self.assertTrue(rt.runner.builder.runnable())

        transitions = list(machine.run_iter('start'))
        self.assertEqual((runner._GAME_OVER, st.REVERTED), transitions[-1])
        self.assertEqual(st.REVERTED, rt.storage.get_atom_state(tasks[0].name))

    def test_builder_expected_transition_occurrences(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            10, task_cls=test_utils.TaskNoRequiresNoReturns)
        flow.add(*tasks)

        rt = self._make_runtime(flow, initial_state=st.RUNNING)
        machine, memory = rt.runner.builder.build()
        transitions = list(machine.run_iter('start'))

        occurrences = dict((t, transitions.count(t)) for t in transitions)
        self.assertEqual(10, occurrences.get((st.SCHEDULING, st.WAITING)))
        self.assertEqual(10, occurrences.get((st.WAITING, st.ANALYZING)))
        self.assertEqual(9, occurrences.get((st.ANALYZING, st.SCHEDULING)))
        self.assertEqual(1, occurrences.get((runner._GAME_OVER, st.SUCCESS)))
        self.assertEqual(1, occurrences.get((runner._UNDEFINED, st.RESUMING)))

        self.assertEqual(0, len(memory.next_nodes))
        self.assertEqual(0, len(memory.not_done))
        self.assertEqual(0, len(memory.failures))
