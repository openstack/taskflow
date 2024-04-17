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
import itertools
import threading

from automaton import runners
from concurrent import futures
import fasteners
import functools
import networkx as nx
from oslo_utils import excutils
from oslo_utils import strutils
from oslo_utils import timeutils

from taskflow.engines.action_engine import builder
from taskflow.engines.action_engine import compiler
from taskflow.engines.action_engine import executor
from taskflow.engines.action_engine import runtime
from taskflow.engines import base
from taskflow import exceptions as exc
from taskflow import logging
from taskflow import states
from taskflow import storage
from taskflow.types import failure
from taskflow.utils import misc

try:
    from taskflow.engines.action_engine import process_executor
except ImportError:
    process_executor = None

LOG = logging.getLogger(__name__)


@contextlib.contextmanager
def _start_stop(task_executor, retry_executor):
    # A teenie helper context manager to safely start/stop engine executors...
    task_executor.start()
    try:
        retry_executor.start()
        try:
            yield (task_executor, retry_executor)
        finally:
            retry_executor.stop()
    finally:
        task_executor.stop()


def _pre_check(check_compiled=True, check_storage_ensured=True,
               check_validated=True):
    """Engine state precondition checking decorator."""

    def decorator(meth):
        do_what = meth.__name__

        @functools.wraps(meth)
        def wrapper(self, *args, **kwargs):
            if check_compiled and not self._compiled:
                raise exc.InvalidState("Can not %s an engine which"
                                       " has not been compiled" % do_what)
            if check_storage_ensured and not self._storage_ensured:
                raise exc.InvalidState("Can not %s an engine"
                                       " which has not had its storage"
                                       " populated" % do_what)
            if check_validated and not self._validated:
                raise exc.InvalidState("Can not %s an engine which"
                                       " has not been validated" % do_what)
            return meth(self, *args, **kwargs)

        return wrapper

    return decorator


class ActionEngine(base.Engine):
    """Generic action-based engine.

    This engine compiles the flow (and any subflows) into a compilation unit
    which contains the full runtime definition to be executed and then uses
    this compilation unit in combination with the executor, runtime, machine
    builder and storage classes to attempt to run your flow (and any
    subflows & contained atoms) to completion.

    NOTE(harlowja): during this process it is permissible and valid to have a
    task or multiple tasks in the execution graph fail (at the same time even),
    which will cause the process of reversion or retrying to commence. See the
    valid states in the states module to learn more about what other states
    the tasks and flow being ran can go through.

    **Engine options:**

    +----------------------+-----------------------+------+------------+
    | Name/key             | Description           | Type | Default    |
    +======================+=======================+======+============+
    | ``defer_reverts``    | This option lets you  | bool | ``False``  |
    |                      | safely nest flows     |      |            |
    |                      | with retries inside   |      |            |
    |                      | flows without retries |      |            |
    |                      | and it still behaves  |      |            |
    |                      | as a user would       |      |            |
    |                      | expect (for example   |      |            |
    |                      | if the retry gets     |      |            |
    |                      | exhausted it reverts  |      |            |
    |                      | the outer flow unless |      |            |
    |                      | the outer flow has a  |      |            |
    |                      | has a separate retry  |      |            |
    |                      | behavior).            |      |            |
    +----------------------+-----------------------+------+------------+
    | ``never_resolve``    | When true, instead    | bool | ``False``  |
    |                      | of reverting          |      |            |
    |                      | and trying to resolve |      |            |
    |                      | a atom failure the    |      |            |
    |                      | engine will skip      |      |            |
    |                      | reverting and abort   |      |            |
    |                      | instead of reverting  |      |            |
    |                      | and/or retrying.      |      |            |
    +----------------------+-----------------------+------+------------+
    | ``inject_transient`` | When true, values     | bool | ``True``   |
    |                      | that are local to     |      |            |
    |                      | each atoms scope      |      |            |
    |                      | are injected into     |      |            |
    |                      | storage into a        |      |            |
    |                      | transient location    |      |            |
    |                      | (typically a local    |      |            |
    |                      | dictionary), when     |      |            |
    |                      | false those values    |      |            |
    |                      | are instead persisted |      |            |
    |                      | into atom details     |      |            |
    |                      | (and saved in a non-  |      |            |
    |                      | transient manner).    |      |            |
    +----------------------+-----------------------+------+------------+
    """

    NO_RERAISING_STATES = frozenset([states.SUSPENDED, states.SUCCESS])
    """
    States that if the engine stops in will **not** cause any potential
    failures to be reraised. States **not** in this list will cause any
    failure/s that were captured (if any) to get reraised.
    """

    IGNORABLE_STATES = frozenset(
        itertools.chain([states.SCHEDULING, states.WAITING, states.RESUMING,
                         states.ANALYZING], builder.META_STATES))
    """
    Informational states this engines internal machine yields back while
    running, not useful to have the engine record but useful to provide to
    end-users when doing execution iterations via :py:meth:`.run_iter`.
    """

    MAX_MACHINE_STATES_RETAINED = 10
    """
    During :py:meth:`~.run_iter` the last X state machine transitions will
    be recorded (typically only useful on failure).
    """

    def __init__(self, flow, flow_detail, backend, options):
        super(ActionEngine, self).__init__(flow, flow_detail, backend, options)
        self._runtime = None
        self._compiled = False
        self._compilation = None
        self._compiler = compiler.PatternCompiler(flow)
        self._lock = threading.RLock()
        self._storage_ensured = False
        self._validated = False
        # Retries are not *currently* executed out of the engines process
        # or thread (this could change in the future if we desire it to).
        self._retry_executor = executor.SerialRetryExecutor()
        self._inject_transient = strutils.bool_from_string(
            self._options.get('inject_transient', True))
        self._gather_statistics = strutils.bool_from_string(
            self._options.get('gather_statistics', True))
        self._statistics = {}

    @_pre_check(check_compiled=True,
                # NOTE(harlowja): We can alter the state of the
                # flow without ensuring its storage is setup for
                # its atoms (since this state change does not affect
                # those units).
                check_storage_ensured=False,
                check_validated=False)
    def suspend(self):
        self._change_state(states.SUSPENDING)

    @property
    def statistics(self):
        return self._statistics

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

    @misc.cachedproperty
    def storage(self):
        """The storage unit for this engine.

        NOTE(harlowja): the atom argument lookup strategy will change for
        this storage unit after
        :py:func:`~taskflow.engines.base.Engine.compile` has
        completed (since **only** after compilation is the actual structure
        known). Before :py:func:`~taskflow.engines.base.Engine.compile`
        has completed the atom argument lookup strategy lookup will be
        restricted to injected arguments **only** (this will **not** reflect
        the actual runtime lookup strategy, which typically will be, but is
        not always different).
        """
        def _scope_fetcher(atom_name):
            if self._compiled:
                return self._runtime.fetch_scopes_for(atom_name)
            else:
                return None
        return storage.Storage(self._flow_detail,
                               backend=self._backend,
                               scope_fetcher=_scope_fetcher)

    def run(self, timeout=None):
        """Runs the engine (or die trying).

        :param timeout: timeout to wait for any atoms to complete (this timeout
            will be used during the waiting period that occurs when
            unfinished atoms are being waited on).
        """
        with fasteners.try_lock(self._lock) as was_locked:
            if not was_locked:
                raise exc.ExecutionFailure("Engine currently locked, please"
                                           " try again later")
            for _state in self.run_iter(timeout=timeout):
                pass

    def run_iter(self, timeout=None):
        """Runs the engine using iteration (or die trying).

        :param timeout: timeout to wait for any atoms to complete (this timeout
            will be used during the waiting period that occurs after the
            waiting state is yielded when unfinished atoms are being waited
            on).

        Instead of running to completion in a blocking manner, this will
        return a generator which will yield back the various states that the
        engine is going through (and can be used to run multiple engines at
        once using a generator per engine). The iterator returned also
        responds to the ``send()`` method from :pep:`0342` and will attempt to
        suspend itself if a truthy value is sent in (the suspend may be
        delayed until all active atoms have finished).

        NOTE(harlowja): using the ``run_iter`` method will **not** retain the
        engine lock while executing so the user should ensure that there is
        only one entity using a returned engine iterator (one per engine) at a
        given time.
        """
        self.compile()
        self.prepare()
        self.validate()
        # Keep track of the last X state changes, which if a failure happens
        # are quite useful to log (and the performance of tracking this
        # should be negligible).
        last_transitions = collections.deque(
            maxlen=max(1, self.MAX_MACHINE_STATES_RETAINED))
        with _start_stop(self._task_executor, self._retry_executor):
            self._change_state(states.RUNNING)
            if self._gather_statistics:
                self._statistics.clear()
                w = timeutils.StopWatch()
                w.start()
            else:
                w = None
            try:
                closed = False
                machine, memory = self._runtime.builder.build(
                    self._statistics, timeout=timeout,
                    gather_statistics=self._gather_statistics)
                r = runners.FiniteRunner(machine)
                for transition in r.run_iter(builder.START):
                    last_transitions.append(transition)
                    _prior_state, new_state = transition
                    # NOTE(harlowja): skip over meta-states
                    if new_state in builder.META_STATES:
                        continue
                    if new_state == states.FAILURE:
                        failure.Failure.reraise_if_any(memory.failures)
                    if closed:
                        continue
                    try:
                        try_suspend = yield new_state
                    except GeneratorExit:
                        # The generator was closed, attempt to suspend and
                        # continue looping until we have cleanly closed up
                        # shop...
                        closed = True
                        self.suspend()
                    except Exception:
                        # Capture the failure, and ensure that the
                        # machine will notice that something externally
                        # has sent an exception in and that it should
                        # finish up and reraise.
                        memory.failures.append(failure.Failure())
                        closed = True
                    else:
                        if try_suspend:
                            self.suspend()
            except Exception:
                with excutils.save_and_reraise_exception():
                    LOG.exception("Engine execution has failed, something"
                                  " bad must have happened (last"
                                  " %s machine transitions were %s)",
                                  last_transitions.maxlen,
                                  list(last_transitions))
                    self._change_state(states.FAILURE)
            else:
                if last_transitions:
                    _prior_state, new_state = last_transitions[-1]
                    if new_state not in self.IGNORABLE_STATES:
                        self._change_state(new_state)
                        if new_state not in self.NO_RERAISING_STATES:
                            e_failures = self.storage.get_execute_failures()
                            r_failures = self.storage.get_revert_failures()
                            er_failures = itertools.chain(
                                e_failures.values(),
                                r_failures.values())
                            failure.Failure.reraise_if_any(er_failures)
            finally:
                if w is not None:
                    w.stop()
                    self._statistics['active_for'] = w.elapsed()

    @staticmethod
    def _check_compilation(compilation):
        """Performs post compilation validation/checks."""
        seen = set()
        dups = set()
        execution_graph = compilation.execution_graph
        for node, node_attrs in execution_graph.nodes(data=True):
            if node_attrs['kind'] in compiler.ATOMS:
                atom_name = node.name
                if atom_name in seen:
                    dups.add(atom_name)
                else:
                    seen.add(atom_name)
        if dups:
            raise exc.Duplicate(
                "Atoms with duplicate names found: %s" % (sorted(dups)))
        return compilation

    def _change_state(self, state):
        moved, old_state = self.storage.change_flow_state(state)
        if moved:
            details = {
                'engine': self,
                'flow_name': self.storage.flow_name,
                'flow_uuid': self.storage.flow_uuid,
                'old_state': old_state,
            }
            self.notifier.notify(state, details)

    def _ensure_storage(self):
        """Ensure all contained atoms exist in the storage unit."""
        self.storage.ensure_atoms(
            self._runtime.iterate_nodes(compiler.ATOMS))
        for atom in self._runtime.iterate_nodes(compiler.ATOMS):
            if atom.inject:
                self.storage.inject_atom_args(atom.name, atom.inject,
                                              transient=self._inject_transient)

    @fasteners.locked
    @_pre_check(check_validated=False)
    def validate(self):
        # At this point we can check to ensure all dependencies are either
        # flow/task provided or storage provided, if there are still missing
        # dependencies then this flow will fail at runtime (which we can avoid
        # by failing at validation time).
        if LOG.isEnabledFor(logging.TRACE):
            execution_graph = self._compilation.execution_graph
            LOG.trace("Validating scoping and argument visibility for"
                      " execution graph with %s nodes and %s edges with"
                      " density %0.3f", execution_graph.number_of_nodes(),
                      execution_graph.number_of_edges(),
                      nx.density(execution_graph))
        missing = set()
        # Attempt to retain a chain of what was missing (so that the final
        # raised exception for the flow has the nodes that had missing
        # dependencies).
        last_cause = None
        last_node = None
        missing_nodes = 0
        for atom in self._runtime.iterate_nodes(compiler.ATOMS):
            exec_missing = self.storage.fetch_unsatisfied_args(
                atom.name, atom.rebind, optional_args=atom.optional)
            revert_missing = self.storage.fetch_unsatisfied_args(
                atom.name, atom.revert_rebind,
                optional_args=atom.revert_optional)
            atom_missing = (('execute', exec_missing),
                            ('revert', revert_missing))
            for method, method_missing in atom_missing:
                if method_missing:
                    cause = exc.MissingDependencies(atom,
                                                    sorted(method_missing),
                                                    cause=last_cause,
                                                    method=method)
                    last_cause = cause
                    last_node = atom
                    missing_nodes += 1
                    missing.update(method_missing)
        if missing:
            # For when a task is provided (instead of a flow) and that
            # task is the only item in the graph and its missing deps, avoid
            # re-wrapping it in yet another exception...
            if missing_nodes == 1 and last_node is self._flow:
                raise last_cause
            else:
                raise exc.MissingDependencies(self._flow,
                                              sorted(missing),
                                              cause=last_cause)
        self._validated = True

    @fasteners.locked
    @_pre_check(check_storage_ensured=False, check_validated=False)
    def prepare(self):
        if not self._storage_ensured:
            # Set our own state to resuming -> (ensure atoms exist
            # in storage) -> suspended in the storage unit and notify any
            # attached listeners of these changes.
            self._change_state(states.RESUMING)
            self._ensure_storage()
            self._change_state(states.SUSPENDED)
            self._storage_ensured = True
        # Reset everything back to pending (if we were previously reverted).
        if self.storage.get_flow_state() == states.REVERTED:
            self.reset()

    @fasteners.locked
    @_pre_check(check_validated=False)
    def reset(self):
        # This transitions *all* contained atoms back into the PENDING state
        # with an intention to EXECUTE (or dies trying to do that) and then
        # changes the state of the flow to PENDING so that it can then run...
        self._runtime.reset_all()
        self._change_state(states.PENDING)

    @fasteners.locked
    def compile(self):
        if self._compiled:
            return
        self._compilation = self._check_compilation(self._compiler.compile())
        self._runtime = runtime.Runtime(self._compilation,
                                        self.storage,
                                        self.atom_notifier,
                                        self._task_executor,
                                        self._retry_executor,
                                        options=self._options)
        self._runtime.compile()
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

    **Additional engine options:**

    * ``executor``: a object that implements a :pep:`3148` compatible executor
      interface; it will be used for scheduling tasks. The following
      type are applicable (other unknown types passed will cause a type
      error to be raised).

=========================  ===============================================
Type provided              Executor used
=========================  ===============================================
|cft|.ThreadPoolExecutor   :class:`~.executor.ParallelThreadTaskExecutor`
|cfp|.ProcessPoolExecutor  :class:`~.|pe|.ParallelProcessTaskExecutor`
|cf|._base.Executor        :class:`~.executor.ParallelThreadTaskExecutor`
=========================  ===============================================

    * ``executor``: a string that will be used to select a :pep:`3148`
      compatible executor; it will be used for scheduling tasks. The following
      string are applicable (other unknown strings passed will cause a value
      error to be raised).

===========================  ===============================================
String (case insensitive)    Executor used
===========================  ===============================================
``process``                  :class:`~.|pe|.ParallelProcessTaskExecutor`
``processes``                :class:`~.|pe|.ParallelProcessTaskExecutor`
``thread``                   :class:`~.executor.ParallelThreadTaskExecutor`
``threaded``                 :class:`~.executor.ParallelThreadTaskExecutor`
``threads``                  :class:`~.executor.ParallelThreadTaskExecutor`
``greenthread``              :class:`~.executor.ParallelThreadTaskExecutor`
                              (greened version)
``greedthreaded``            :class:`~.executor.ParallelThreadTaskExecutor`
                              (greened version)
``greenthreads``             :class:`~.executor.ParallelThreadTaskExecutor`
                              (greened version)
===========================  ===============================================

    * ``max_workers``: a integer that will affect the number of parallel
      workers that are used to dispatch tasks into (this number is bounded
      by the maximum parallelization your workflow can support).

    * ``wait_timeout``: a float (in seconds) that will affect the
      parallel process task executor (and therefore is **only** applicable when
      the executor provided above is of the process variant). This number
      affects how much time the process task executor waits for messages from
      child processes (typically indicating they have finished or failed). A
      lower number will have high granularity but *currently* involves more
      polling while a higher number will involve less polling but a slower time
      for an engine to notice a task has completed.

    .. |pe|  replace:: process_executor
    .. |cfp| replace:: concurrent.futures.process
    .. |cft| replace:: concurrent.futures.thread
    .. |cf| replace:: concurrent.futures
    """

    # One of these types should match when a object (non-string) is provided
    # for the 'executor' option.
    #
    # NOTE(harlowja): the reason we use the library/built-in futures is to
    # allow for instances of that to be detected and handled correctly, instead
    # of forcing everyone to use our derivatives (futurist or other)...
    _executor_cls_matchers = [
        _ExecutorTypeMatch((futures.ThreadPoolExecutor,),
                           executor.ParallelThreadTaskExecutor),
    ]
    if process_executor is not None:
        _executor_cls_matchers.append(
            _ExecutorTypeMatch((futures.ProcessPoolExecutor,),
                               process_executor.ParallelProcessTaskExecutor)
        )
    _executor_cls_matchers.append(
        _ExecutorTypeMatch((futures.Executor,),
                           executor.ParallelThreadTaskExecutor),
    )

    # One of these should match when a string/text is provided for the
    # 'executor' option (a mixed case equivalent is allowed since the match
    # will be lower-cased before checking).
    _executor_str_matchers = [
        _ExecutorTextMatch(frozenset(['thread', 'threads', 'threaded']),
                           executor.ParallelThreadTaskExecutor),
        _ExecutorTextMatch(frozenset(['greenthread', 'greenthreads',
                                      'greenthreaded']),
                           executor.ParallelGreenThreadTaskExecutor),
    ]
    if process_executor is not None:
        _executor_str_matchers.append(
            _ExecutorTextMatch(frozenset(['processes', 'process']),
                               process_executor.ParallelProcessTaskExecutor)
        )

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
        if isinstance(desired_executor, str):
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
        try:
            for (k, value_converter) in executor_cls.constructor_options:
                try:
                    kwargs[k] = value_converter(options[k])
                except KeyError:
                    pass
        except AttributeError:
            pass
        return executor_cls(**kwargs)
