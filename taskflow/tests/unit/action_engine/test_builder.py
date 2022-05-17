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

from automaton import exceptions as excp
from automaton import runners

from taskflow.engines.action_engine import builder
from taskflow.engines.action_engine import compiler
from taskflow.engines.action_engine import executor
from taskflow.engines.action_engine import runtime
from taskflow.patterns import linear_flow as lf
from taskflow import states as st
from taskflow import storage
from taskflow import test
from taskflow.tests import utils as test_utils
from taskflow.types import notifier
from taskflow.utils import persistence_utils as pu


class BuildersTest(test.TestCase):

    def _make_runtime(self, flow, initial_state=None):
        compilation = compiler.PatternCompiler(flow).compile()
        flow_detail = pu.create_flow_detail(flow)
        store = storage.Storage(flow_detail)
        nodes_iter = compilation.execution_graph.nodes(data=True)
        for node, node_attrs in nodes_iter:
            if node_attrs['kind'] in ('task', 'retry'):
                store.ensure_atom(node)
        if initial_state:
            store.set_flow_state(initial_state)
        atom_notifier = notifier.Notifier()
        task_executor = executor.SerialTaskExecutor()
        retry_executor = executor.SerialRetryExecutor()
        task_executor.start()
        self.addCleanup(task_executor.stop)
        r = runtime.Runtime(compilation, store,
                            atom_notifier, task_executor,
                            retry_executor)
        r.compile()
        return r

    def _make_machine(self, flow, initial_state=None):
        runtime = self._make_runtime(flow, initial_state=initial_state)
        machine, memory = runtime.builder.build({})
        machine_runner = runners.FiniteRunner(machine)
        return (runtime, machine, memory, machine_runner)

    def test_run_iterations(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            1, task_cls=test_utils.TaskNoRequiresNoReturns)
        flow.add(*tasks)

        runtime, machine, memory, machine_runner = self._make_machine(
            flow, initial_state=st.RUNNING)

        it = machine_runner.run_iter(builder.START)
        prior_state, new_state = next(it)
        self.assertEqual(st.RESUMING, new_state)
        self.assertEqual(0, len(memory.failures))

        prior_state, new_state = next(it)
        self.assertEqual(st.SCHEDULING, new_state)
        self.assertEqual(0, len(memory.failures))

        prior_state, new_state = next(it)
        self.assertEqual(st.WAITING, new_state)
        self.assertEqual(0, len(memory.failures))

        prior_state, new_state = next(it)
        self.assertEqual(st.ANALYZING, new_state)
        self.assertEqual(0, len(memory.failures))

        prior_state, new_state = next(it)
        self.assertEqual(builder.GAME_OVER, new_state)
        self.assertEqual(0, len(memory.failures))
        prior_state, new_state = next(it)
        self.assertEqual(st.SUCCESS, new_state)
        self.assertEqual(0, len(memory.failures))

        self.assertRaises(StopIteration, next, it)

    def test_run_iterations_reverted(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            1, task_cls=test_utils.TaskWithFailure)
        flow.add(*tasks)

        runtime, machine, memory, machine_runner = self._make_machine(
            flow, initial_state=st.RUNNING)

        transitions = list(machine_runner.run_iter(builder.START))
        prior_state, new_state = transitions[-1]
        self.assertEqual(st.REVERTED, new_state)
        self.assertEqual([], memory.failures)
        self.assertEqual(st.REVERTED,
                         runtime.storage.get_atom_state(tasks[0].name))

    def test_run_iterations_failure(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            1, task_cls=test_utils.NastyFailingTask)
        flow.add(*tasks)

        runtime, machine, memory, machine_runner = self._make_machine(
            flow, initial_state=st.RUNNING)

        transitions = list(machine_runner.run_iter(builder.START))
        prior_state, new_state = transitions[-1]
        self.assertEqual(st.FAILURE, new_state)
        self.assertEqual(1, len(memory.failures))
        failure = memory.failures[0]
        self.assertTrue(failure.check(RuntimeError))
        self.assertEqual(st.REVERT_FAILURE,
                         runtime.storage.get_atom_state(tasks[0].name))

    def test_run_iterations_suspended(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            2, task_cls=test_utils.TaskNoRequiresNoReturns)
        flow.add(*tasks)

        runtime, machine, memory, machine_runner = self._make_machine(
            flow, initial_state=st.RUNNING)

        transitions = []
        for prior_state, new_state in machine_runner.run_iter(builder.START):
            transitions.append((new_state, memory.failures))
            if new_state == st.ANALYZING:
                runtime.storage.set_flow_state(st.SUSPENDED)
        state, failures = transitions[-1]
        self.assertEqual(st.SUSPENDED, state)
        self.assertEqual([], failures)

        self.assertEqual(st.SUCCESS,
                         runtime.storage.get_atom_state(tasks[0].name))
        self.assertEqual(st.PENDING,
                         runtime.storage.get_atom_state(tasks[1].name))

    def test_run_iterations_suspended_failure(self):
        flow = lf.Flow("root")
        sad_tasks = test_utils.make_many(
            1, task_cls=test_utils.NastyFailingTask)
        flow.add(*sad_tasks)
        happy_tasks = test_utils.make_many(
            1, task_cls=test_utils.TaskNoRequiresNoReturns, offset=1)
        flow.add(*happy_tasks)

        runtime, machine, memory, machine_runner = self._make_machine(
            flow, initial_state=st.RUNNING)

        transitions = []
        for prior_state, new_state in machine_runner.run_iter(builder.START):
            transitions.append((new_state, memory.failures))
            if new_state == st.ANALYZING:
                runtime.storage.set_flow_state(st.SUSPENDED)
        state, failures = transitions[-1]
        self.assertEqual(st.SUSPENDED, state)
        self.assertEqual([], failures)

        self.assertEqual(st.PENDING,
                         runtime.storage.get_atom_state(happy_tasks[0].name))
        self.assertEqual(st.FAILURE,
                         runtime.storage.get_atom_state(sad_tasks[0].name))

    def test_builder_manual_process(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            1, task_cls=test_utils.TaskNoRequiresNoReturns)
        flow.add(*tasks)

        runtime, machine, memory, machine_runner = self._make_machine(
            flow, initial_state=st.RUNNING)
        self.assertRaises(excp.NotInitialized, machine.process_event, 'poke')

        # Should now be pending...
        self.assertEqual(st.PENDING,
                         runtime.storage.get_atom_state(tasks[0].name))

        machine.initialize()
        self.assertEqual(builder.UNDEFINED, machine.current_state)
        self.assertFalse(machine.terminated)
        self.assertRaises(excp.NotFound, machine.process_event, 'poke')
        last_state = machine.current_state

        reaction, terminal = machine.process_event(builder.START)
        self.assertFalse(terminal)
        self.assertIsNotNone(reaction)
        self.assertEqual(st.RESUMING, machine.current_state)
        self.assertRaises(excp.NotFound, machine.process_event, 'poke')

        last_state = machine.current_state
        cb, args, kwargs = reaction
        next_event = cb(last_state, machine.current_state,
                        builder.START, *args, **kwargs)
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
        self.assertEqual(st.RUNNING,
                         runtime.storage.get_atom_state(tasks[0].name))

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
        self.assertEqual(builder.GAME_OVER, machine.current_state)

        # Should now be done...
        self.assertEqual(st.SUCCESS,
                         runtime.storage.get_atom_state(tasks[0].name))

    def test_builder_automatic_process(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            1, task_cls=test_utils.TaskNoRequiresNoReturns)
        flow.add(*tasks)

        runtime, machine, memory, machine_runner = self._make_machine(
            flow, initial_state=st.RUNNING)

        transitions = list(machine_runner.run_iter(builder.START))
        self.assertEqual((builder.UNDEFINED, st.RESUMING), transitions[0])
        self.assertEqual((builder.GAME_OVER, st.SUCCESS), transitions[-1])
        self.assertEqual(st.SUCCESS,
                         runtime.storage.get_atom_state(tasks[0].name))

    def test_builder_automatic_process_failure(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(1, task_cls=test_utils.NastyFailingTask)
        flow.add(*tasks)

        runtime, machine, memory, machine_runner = self._make_machine(
            flow, initial_state=st.RUNNING)

        transitions = list(machine_runner.run_iter(builder.START))
        self.assertEqual((builder.GAME_OVER, st.FAILURE), transitions[-1])
        self.assertEqual(1, len(memory.failures))

    def test_builder_automatic_process_reverted(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(1, task_cls=test_utils.TaskWithFailure)
        flow.add(*tasks)

        runtime, machine, memory, machine_runner = self._make_machine(
            flow, initial_state=st.RUNNING)

        transitions = list(machine_runner.run_iter(builder.START))
        self.assertEqual((builder.GAME_OVER, st.REVERTED), transitions[-1])
        self.assertEqual(st.REVERTED,
                         runtime.storage.get_atom_state(tasks[0].name))

    def test_builder_expected_transition_occurrences(self):
        flow = lf.Flow("root")
        tasks = test_utils.make_many(
            10, task_cls=test_utils.TaskNoRequiresNoReturns)
        flow.add(*tasks)

        runtime, machine, memory, machine_runner = self._make_machine(
            flow, initial_state=st.RUNNING)
        transitions = list(machine_runner.run_iter(builder.START))

        occurrences = dict((t, transitions.count(t)) for t in transitions)
        self.assertEqual(10, occurrences.get((st.SCHEDULING, st.WAITING)))
        self.assertEqual(10, occurrences.get((st.WAITING, st.ANALYZING)))
        self.assertEqual(9, occurrences.get((st.ANALYZING, st.SCHEDULING)))
        self.assertEqual(1, occurrences.get((builder.GAME_OVER, st.SUCCESS)))
        self.assertEqual(1, occurrences.get((builder.UNDEFINED, st.RESUMING)))

        self.assertEqual(0, len(memory.next_up))
        self.assertEqual(0, len(memory.not_done))
        self.assertEqual(0, len(memory.failures))
