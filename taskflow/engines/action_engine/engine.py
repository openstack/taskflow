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
import threading

from concurrent import futures
from oslo_utils import excutils
import six

from taskflow.engines.action_engine import compiler
from taskflow.engines.action_engine import executor
from taskflow.engines.action_engine import runtime
from taskflow.engines import base
from taskflow import exceptions as exc
from taskflow import states
from taskflow.types import failure
from taskflow.utils import lock_utils
from taskflow.utils import misc


@contextlib.contextmanager
def _start_stop(executor):
    # A teenie helper context manager to safely start/stop a executor...
    executor.start()
    try:
        yield executor
    finally:
        executor.stop()


class ActionEngine(base.Engine):
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

    def __init__(self, flow, flow_detail, backend, options):
        super(ActionEngine, self).__init__(flow, flow_detail, backend, options)
        self._runtime = None
        self._compiled = False
        self._compilation = None
        self._lock = threading.RLock()
        self._state_lock = threading.RLock()
        self._storage_ensured = False

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

    def __init__(self, flow, flow_detail, backend, options):
        super(SerialActionEngine, self).__init__(flow, flow_detail,
                                                 backend, options)
        self._task_executor = executor.SerialTaskExecutor()


class _ExecutorTypeMatch(collections.namedtuple('_ExecutorTypeMatch',
                                                ['types', 'executor_cls'])):
    def matches(self, executor):
        return isinstance(executor, self.types)


class _ExecutorTextMatch(collections.namedtuple('_ExecutorTextMatch',
                                                ['strings', 'executor_cls'])):
    def matches(self, text):
        return text.lower() in self.strings


class ParallelActionEngine(ActionEngine):
    """Engine that runs tasks in parallel manner.

    Supported keyword arguments:

    * ``executor``: a object that implements a :pep:`3148` compatible executor
      interface; it will be used for scheduling tasks. The following
      type are applicable (other unknown types passed will cause a type
      error to be raised).

=========================  ===============================================
Type provided              Executor used
=========================  ===============================================
|cft|.ThreadPoolExecutor   :class:`~.executor.ParallelThreadTaskExecutor`
|cfp|.ProcessPoolExecutor  :class:`~.executor.ParallelProcessTaskExecutor`
|cf|._base.Executor        :class:`~.executor.ParallelThreadTaskExecutor`
=========================  ===============================================

    * ``executor``: a string that will be used to select a :pep:`3148`
      compatible executor; it will be used for scheduling tasks. The following
      string are applicable (other unknown strings passed will cause a value
      error to be raised).

===========================  ===============================================
String (case insensitive)    Executor used
===========================  ===============================================
``process``                  :class:`~.executor.ParallelProcessTaskExecutor`
``processes``                :class:`~.executor.ParallelProcessTaskExecutor`
``thread``                   :class:`~.executor.ParallelThreadTaskExecutor`
``threaded``                 :class:`~.executor.ParallelThreadTaskExecutor`
``threads``                  :class:`~.executor.ParallelThreadTaskExecutor`
===========================  ===============================================

    .. |cfp| replace:: concurrent.futures.process
    .. |cft| replace:: concurrent.futures.thread
    .. |cf| replace:: concurrent.futures
    """

    # One of these types should match when a object (non-string) is provided
    # for the 'executor' option.
    #
    # NOTE(harlowja): the reason we use the library/built-in futures is to
    # allow for instances of that to be detected and handled correctly, instead
    # of forcing everyone to use our derivatives...
    _executor_cls_matchers = [
        _ExecutorTypeMatch((futures.ThreadPoolExecutor,),
                           executor.ParallelThreadTaskExecutor),
        _ExecutorTypeMatch((futures.ProcessPoolExecutor,),
                           executor.ParallelProcessTaskExecutor),
        _ExecutorTypeMatch((futures.Executor,),
                           executor.ParallelThreadTaskExecutor),
    ]

    # One of these should match when a string/text is provided for the
    # 'executor' option (a mixed case equivalent is allowed since the match
    # will be lower-cased before checking).
    _executor_str_matchers = [
        _ExecutorTextMatch(frozenset(['processes', 'process']),
                           executor.ParallelProcessTaskExecutor),
        _ExecutorTextMatch(frozenset(['thread', 'threads', 'threaded']),
                           executor.ParallelThreadTaskExecutor),
    ]

    # Used when no executor is provided (either a string or object)...
    _default_executor_cls = executor.ParallelThreadTaskExecutor

    def __init__(self, flow, flow_detail, backend, options):
        super(ParallelActionEngine, self).__init__(flow, flow_detail,
                                                   backend, options)
        # This ensures that any provided executor will be validated before
        # we get to far in the compilation/execution pipeline...
        self._task_executor = self._fetch_task_executor(self._options)

    @classmethod
    def _fetch_task_executor(cls, options):
        kwargs = {}
        executor_cls = cls._default_executor_cls
        # Match the desired executor to a class that will work with it...
        desired_executor = options.get('executor')
        if isinstance(desired_executor, six.string_types):
            matched_executor_cls = None
            for m in cls._executor_str_matchers:
                if m.matches(desired_executor):
                    matched_executor_cls = m.executor_cls
                    break
            if matched_executor_cls is None:
                expected = set()
                for m in cls._executor_str_matchers:
                    expected.update(m.strings)
                raise ValueError("Unknown executor string '%s' expected"
                                 " one of %s (or mixed case equivalent)"
                                 % (desired_executor, list(expected)))
            else:
                executor_cls = matched_executor_cls
        elif desired_executor is not None:
            matched_executor_cls = None
            for m in cls._executor_cls_matchers:
                if m.matches(desired_executor):
                    matched_executor_cls = m.executor_cls
                    break
            if matched_executor_cls is None:
                expected = set()
                for m in cls._executor_cls_matchers:
                    expected.update(m.types)
                raise TypeError("Unknown executor '%s' (%s) expected an"
                                " instance of %s" % (desired_executor,
                                                     type(desired_executor),
                                                     list(expected)))
            else:
                executor_cls = matched_executor_cls
                kwargs['executor'] = desired_executor
        for k in getattr(executor_cls, 'OPTIONS', []):
            if k == 'executor':
                continue
            try:
                kwargs[k] = options[k]
            except KeyError:
                pass
        return executor_cls(**kwargs)
