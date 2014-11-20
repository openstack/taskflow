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
import threading

from oslo.utils import excutils

from taskflow.engines.action_engine import compiler
from taskflow.engines.action_engine import executor
from taskflow.engines.action_engine import runtime
from taskflow.engines import base
from taskflow import exceptions as exc
from taskflow import states
from taskflow import storage as atom_storage
from taskflow.types import failure
from taskflow.utils import lock_utils
from taskflow.utils import misc
from taskflow.utils import reflection


@contextlib.contextmanager
def _start_stop(executor):
    # A teenie helper context manager to safely start/stop a executor...
    executor.start()
    try:
        yield executor
    finally:
        executor.stop()


class ActionEngine(base.EngineBase):
    """Generic action-based engine.

    This engine compiles the flow (and any subflows) into a compilation unit
    which contains the full runtime definition to be executed and then uses
    this compilation unit in combination with the executor, runtime, runner
    and storage classes to attempt to run your flow (and any subflows &
    contained atoms) to completion.

    NOTE(harlowja): during this process it is permissible and valid to have a
    task or multiple tasks in the execution graph fail (at the same time even),
    which will cause the process of reversion or retrying to commence. See the
    valid states in the states module to learn more about what other states
    the tasks and flow being ran can go through.
    """
    _compiler_factory = compiler.PatternCompiler
    _task_executor_factory = executor.SerialTaskExecutor

    def __init__(self, flow, flow_detail, backend, options):
        super(ActionEngine, self).__init__(flow, flow_detail, backend, options)
        self._runtime = None
        self._compiled = False
        self._compilation = None
        self._lock = threading.RLock()
        self._state_lock = threading.RLock()
        self._storage_ensured = False

    def __str__(self):
        return "%s: %s" % (reflection.get_class_name(self), id(self))

    def suspend(self):
        if not self._compiled:
            raise exc.InvalidState("Can not suspend an engine"
                                   " which has not been compiled")
        self._change_state(states.SUSPENDING)

    @property
    def compilation(self):
        """The compilation result.

        NOTE(harlowja): Only accessible after compilation has completed (None
        will be returned when this property is accessed before compilation has
        completed successfully).
        """
        if self._compiled:
            return self._compilation
        else:
            return None

    def run(self):
        with lock_utils.try_lock(self._lock) as was_locked:
            if not was_locked:
                raise exc.ExecutionFailure("Engine currently locked, please"
                                           " try again later")
            for _state in self.run_iter():
                pass

    def run_iter(self, timeout=None):
        """Runs the engine using iteration (or die trying).

        :param timeout: timeout to wait for any tasks to complete (this timeout
            will be used during the waiting period that occurs after the
            waiting state is yielded when unfinished tasks are being waited
            for).

        Instead of running to completion in a blocking manner, this will
        return a generator which will yield back the various states that the
        engine is going through (and can be used to run multiple engines at
        once using a generator per engine). the iterator returned also
        responds to the send() method from pep-0342 and will attempt to suspend
        itself if a truthy value is sent in (the suspend may be delayed until
        all active tasks have finished).

        NOTE(harlowja): using the run_iter method will **not** retain the
        engine lock while executing so the user should ensure that there is
        only one entity using a returned engine iterator (one per engine) at a
        given time.
        """
        self.compile()
        self.prepare()
        runner = self._runtime.runner
        last_state = None
        with _start_stop(self._task_executor):
            self._change_state(states.RUNNING)
            try:
                closed = False
                for (last_state, failures) in runner.run_iter(timeout=timeout):
                    if failures:
                        failure.Failure.reraise_if_any(failures)
                    if closed:
                        continue
                    try:
                        try_suspend = yield last_state
                    except GeneratorExit:
                        # The generator was closed, attempt to suspend and
                        # continue looping until we have cleanly closed up
                        # shop...
                        closed = True
                        self.suspend()
                    else:
                        if try_suspend:
                            self.suspend()
            except Exception:
                with excutils.save_and_reraise_exception():
                    self._change_state(states.FAILURE)
            else:
                ignorable_states = getattr(runner, 'ignorable_states', [])
                if last_state and last_state not in ignorable_states:
                    self._change_state(last_state)
                    if last_state not in [states.SUSPENDED, states.SUCCESS]:
                        failures = self.storage.get_failures()
                        failure.Failure.reraise_if_any(failures.values())

    def _change_state(self, state):
        with self._state_lock:
            old_state = self.storage.get_flow_state()
            if not states.check_flow_transition(old_state, state):
                return
            self.storage.set_flow_state(state)
        details = {
            'engine': self,
            'flow_name': self.storage.flow_name,
            'flow_uuid': self.storage.flow_uuid,
            'old_state': old_state,
        }
        self.notifier.notify(state, details)

    def _ensure_storage(self):
        """Ensure all contained atoms exist in the storage unit."""
        for node in self._compilation.execution_graph.nodes_iter():
            self.storage.ensure_atom(node)
            if node.inject:
                self.storage.inject_atom_args(node.name, node.inject)

    @lock_utils.locked
    def prepare(self):
        if not self._compiled:
            raise exc.InvalidState("Can not prepare an engine"
                                   " which has not been compiled")
        if not self._storage_ensured:
            # Set our own state to resuming -> (ensure atoms exist
            # in storage) -> suspended in the storage unit and notify any
            # attached listeners of these changes.
            self._change_state(states.RESUMING)
            self._ensure_storage()
            self._change_state(states.SUSPENDED)
            self._storage_ensured = True
        # At this point we can check to ensure all dependencies are either
        # flow/task provided or storage provided, if there are still missing
        # dependencies then this flow will fail at runtime (which we can avoid
        # by failing at preparation time).
        external_provides = set(self.storage.fetch_all().keys())
        missing = self._flow.requires - external_provides
        if missing:
            raise exc.MissingDependencies(self._flow, sorted(missing))
        # Reset everything back to pending (if we were previously reverted).
        if self.storage.get_flow_state() == states.REVERTED:
            self._runtime.reset_all()
            self._change_state(states.PENDING)

    @misc.cachedproperty
    def _task_executor(self):
        return self._task_executor_factory()

    @misc.cachedproperty
    def _compiler(self):
        return self._compiler_factory(self._flow)

    @lock_utils.locked
    def compile(self):
        if self._compiled:
            return
        self._compilation = self._compiler.compile()
        self._runtime = runtime.Runtime(self._compilation,
                                        self.storage,
                                        self.atom_notifier,
                                        self._task_executor)
        self._compiled = True


class SerialActionEngine(ActionEngine):
    """Engine that runs tasks in serial manner."""
    _storage_factory = atom_storage.SingleThreadedStorage


class ParallelActionEngine(ActionEngine):
    """Engine that runs tasks in parallel manner."""
    _storage_factory = atom_storage.MultiThreadedStorage

    def _task_executor_factory(self):
        return executor.ParallelTaskExecutor(
            executor=self._options.get('executor'),
            max_workers=self._options.get('max_workers'))
