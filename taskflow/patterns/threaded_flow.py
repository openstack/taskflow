# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

from taskflow import exceptions as exc
from taskflow import flow
from taskflow import graph_utils
from taskflow import states
from taskflow import utils

import collections
import functools
import logging
import sys
import threading
import weakref

from networkx.algorithms import cycles
from networkx.classes import digraph

LOG = logging.getLogger(__name__)


class DependencyTimeout(exc.InvalidStateException):
    """When running in parallel a task has the ability to timeout waiting for
    its dependent tasks to finish, this will be raised when that occurs.
    """
    pass


class Flow(flow.Flow):
    """This flow pattern establishes tasks into a graph where each task is a
    node in the graph and dependencies between tasks are edges in the graph.
    When running (in parallel) each task will only be activated when its
    dependencies have been satisified. When a graph is split into two or more
    segments, both of those segments will be ran in parallel.

    For example lets take this small little *somewhat complicated* graph:

    X--Y--C--D
          |  |
    A--B--    --G--
          |  |     |--Z(end)
    E--F--    --H--

    In this flow the following will be ran in parallel at start:
     1. X--Y
     2. A--B
     3. E--F
    Note the C--D nodes will not be able to run until [Y,B,F] has completed.
    After C--D completes the following will be ran in parallel:
     1. G
     2. H
    Then finally Z will run (after [G,H] complete) and the flow will then have
    finished executing.
    """
    MUTABLE_STATES = set([states.PENDING, states.FAILURE, states.SUCCESS])
    REVERTABLE_STATES = set([states.FAILURE, states.INCOMPLETE])
    CANCELLABLE_STATES = set([states.PENDING, states.RUNNING])

    def __init__(self, name):
        super(Flow, self).__init__(name)
        self._graph = digraph.DiGraph(name=name)
        self._run_lock = threading.RLock()
        self._cancel_lock = threading.RLock()
        self._mutate_lock = threading.RLock()
        # NOTE(harlowja) The locking order in this list actually matters since
        # we need to make sure that users of this list do not get deadlocked
        # by out of order lock access.
        self._core_locks = [
            self._run_lock,
            self._mutate_lock,
            self._cancel_lock,
        ]
        self._run_locks = [
            self._run_lock,
            self._mutate_lock,
        ]
        self._cancel_locks = [
            self._cancel_lock,
        ]
        self.results = {}
        self.resumer = None

    def __str__(self):
        lines = ["ParallelFlow: %s" % (self.name)]
        lines.append("%s" % (self._graph.number_of_nodes()))
        lines.append("%s" % (self.state))
        return "; ".join(lines)

    def soft_reset(self):
        # The way this flow works does not allow (at the current moment) for
        # you to suspend the threads and then resume them at a later time,
        # instead it only supports interruption (which will cancel the threads)
        # and then a full reset.
        raise NotImplementedError("Threaded flow does not currently support"
                                  " soft resetting, please try using"
                                  " reset() instead")

    def interrupt(self):
        """Currently we can not pause threads and then resume them later, not
        really thinking that we should likely ever do this.
        """
        raise NotImplementedError("Threaded flow does not currently support"
                                  " interruption, please try using"
                                  " cancel() instead")

    def reset(self):
        # All locks are used so that resets can not happen while running or
        # cancelling or modifying.
        with utils.MultiLock(self._core_locks):
            super(Flow, self).reset()
            self.results = {}
            self.resumer = None

    def cancel(self):

        def check():
            if self.state not in self.CANCELLABLE_STATES:
                raise exc.InvalidStateException("Can not attempt cancellation"
                                                " when in state %s" %
                                                self.state)

        check()
        cancelled = 0
        was_empty = False

        # We don't lock the other locks so that the flow can be cancelled while
        # running. Further state management logic is then used while running
        # to verify that the flow should still be running when it has been
        # cancelled.
        with utils.MultiLock(self._cancel_locks):
            check()
            if len(self._graph) == 0:
                was_empty = True
            else:
                for r in self._graph.nodes_iter():
                    try:
                        if r.cancel(blocking=False):
                            cancelled += 1
                    except exc.InvalidStateException:
                        pass
            if cancelled or was_empty:
                self._change_state(None, states.CANCELLED)

        return cancelled

    def _find_uuid(self, uuid):
        # Finds the runner for the given uuid (or returns none)
        for r in self._graph.nodes_iter():
            if r.uuid == uuid:
                return r
        return None

    def add(self, task, timeout=None, infer=True):
        """Adds a task to the given flow using the given timeout which will be
        used a the timeout to wait for dependencies (if any) to be
        fulfilled.
        """
        def check():
            if self.state not in self.MUTABLE_STATES:
                raise exc.InvalidStateException("Flow is currently in a"
                                                " non-mutable %s state" %
                                                (self.state))

        # Ensure that we do a quick check to see if we can even perform this
        # addition before we go about actually acquiring the lock to perform
        # the actual addition.
        check()

        # All locks must be acquired so that modifications can not be made
        # while running, cancelling or performing a simultaneous mutation.
        with utils.MultiLock(self._core_locks):
            check()
            runner = ThreadRunner(task, self, timeout)
            self._graph.add_node(runner, infer=infer)
            return runner.uuid

    def _connect(self):
        """Infers and connects the edges of the given tasks by examining the
        associated tasks provides and requires attributes and connecting tasks
        that require items to tasks that produce said items.
        """

        # Disconnect all edges not manually created before we attempt to infer
        # them so that we don't retain edges that are invalid.
        def disconnect_non_user(u, v, e_data):
            if e_data and e_data.get('reason') != 'manual':
                return True
            return False

        # Link providers to requirers.
        graph_utils.connect(self._graph,
                            discard_func=disconnect_non_user)

        # Connect the successors & predecessors and related siblings
        for r in self._graph.nodes_iter():
            r._predecessors = []
            r._successors = []
            for (r2, _me) in self._graph.in_edges_iter([r]):
                r._predecessors.append(r2)
            for (_me, r2) in self._graph.out_edges_iter([r]):
                r._successors.append(r2)
            r.siblings = []
            for r2 in self._graph.nodes_iter():
                if r2 is r or r2 in r._predecessors or r2 in r._successors:
                    continue
                r._siblings.append(r2)

    def add_many(self, tasks):
        """Adds a list of tasks to the flow."""

        def check():
            if self.state not in self.MUTABLE_STATES:
                raise exc.InvalidStateException("Flow is currently in a"
                                                " non-mutable state %s"
                                                % (self.state))

        # Ensure that we do a quick check to see if we can even perform this
        # addition before we go about actually acquiring the lock.
        check()

        # All locks must be acquired so that modifications can not be made
        # while running, cancelling or performing a simultaneous mutation.
        with utils.MultiLock(self._core_locks):
            check()
            added = []
            for t in tasks:
                added.append(self.add(t))
            return added

    def add_dependency(self, provider_uuid, consumer_uuid):
        """Manually adds a dependency between a provider and a consumer."""

        def check_and_fetch():
            if self.state not in self.MUTABLE_STATES:
                raise exc.InvalidStateException("Flow is currently in a"
                                                " non-mutable state %s"
                                                % (self.state))
            provider = self._find_uuid(provider_uuid)
            if not provider or not self._graph.has_node(provider):
                raise exc.InvalidStateException("Can not add a dependency "
                                                "from unknown uuid %s" %
                                                (provider_uuid))
            consumer = self._find_uuid(consumer_uuid)
            if not consumer or not self._graph.has_node(consumer):
                raise exc.InvalidStateException("Can not add a dependency "
                                                "to unknown uuid %s"
                                                % (consumer_uuid))
            if provider is consumer:
                raise exc.InvalidStateException("Can not add a dependency "
                                                "to loop via uuid %s"
                                                % (consumer_uuid))
            return (provider, consumer)

        check_and_fetch()

        # All locks must be acquired so that modifications can not be made
        # while running, cancelling or performing a simultaneous mutation.
        with utils.MultiLock(self._core_locks):
            (provider, consumer) = check_and_fetch()
            self._graph.add_edge(provider, consumer, reason='manual')
            LOG.debug("Connecting %s as a manual provider for %s",
                      provider, consumer)

    def run(self, context, *args, **kwargs):
        """Executes the given flow using the given context and args/kwargs."""

        def abort_if(current_state, ok_states):
            if current_state in (states.CANCELLED,):
                return False
            if current_state not in ok_states:
                return False
            return True

        def check():
            if self.state not in self.RUNNABLE_STATES:
                raise exc.InvalidStateException("Flow is currently unable "
                                                "to be ran in state %s"
                                                % (self.state))

        def connect_and_verify():
            """Do basic sanity tests on the graph structure."""
            if len(self._graph) == 0:
                return
            self._connect()
            degrees = [g[1] for g in self._graph.in_degree_iter()]
            zero_degrees = [d for d in degrees if d == 0]
            if not zero_degrees:
                # If every task depends on something else to produce its input
                # then we will be in a deadlock situation.
                raise exc.InvalidStateException("No task has an in-degree"
                                                " of zero")
            self_loops = self._graph.nodes_with_selfloops()
            if self_loops:
                # A task that has a dependency on itself will never be able
                # to run.
                raise exc.InvalidStateException("%s tasks have been detected"
                                                " with dependencies on"
                                                " themselves" %
                                                len(self_loops))
            simple_cycles = len(cycles.recursive_simple_cycles(self._graph))
            if simple_cycles:
                # A task loop will never be able to run, unless it somehow
                # breaks that loop.
                raise exc.InvalidStateException("%s tasks have been detected"
                                                " with dependency loops" %
                                                simple_cycles)

        def run_it(result_cb, args, kwargs):
            check_runnable = functools.partial(abort_if,
                                               ok_states=self.RUNNABLE_STATES)
            if self._change_state(context, states.RUNNING,
                                  check_func=check_runnable):
                self.results = {}
                if len(self._graph) == 0:
                    return
                for r in self._graph.nodes_iter():
                    r.reset()
                    r._result_cb = result_cb
                executor = utils.ThreadGroupExecutor()
                for r in self._graph.nodes_iter():
                    executor.submit(r, *args, **kwargs)
                executor.await_termination()

        def trigger_rollback(failures):
            if not failures:
                return
            causes = []
            for r in failures:
                causes.append(utils.FlowFailure(r, self,
                                                r.exc, r.exc_info))
            try:
                self.rollback(context, causes)
            except exc.InvalidStateException:
                pass
            finally:
                # TODO(harlowja): re-raise a combined exception when
                # there are more than one failures??
                for f in failures:
                    if all(f.exc_info):
                        raise f.exc_info[0], f.exc_info[1], f.exc_info[2]

        def handle_results():
            # Isolate each runner state into groups so that we can easily tell
            # which ones failed, cancelled, completed...
            groups = collections.defaultdict(list)
            for r in self._graph.nodes_iter():
                groups[r.state].append(r)
            for r in self._graph.nodes_iter():
                if r not in groups.get(states.FAILURE, []) and r.has_ran():
                    self.results[r.uuid] = r.result
            if groups[states.FAILURE]:
                self._change_state(context, states.FAILURE)
                trigger_rollback(groups[states.FAILURE])
            elif (groups[states.CANCELLED] or groups[states.PENDING]
                  or groups[states.TIMED_OUT] or groups[states.STARTED]):
                self._change_state(context, states.INCOMPLETE)
            else:
                check_ran = functools.partial(abort_if,
                                              ok_states=[states.RUNNING])
                self._change_state(context, states.SUCCESS,
                                   check_func=check_ran)

        def get_resumer_cb():
            if not self.resumer:
                return None
            (ran, _others) = self.resumer(self, self._graph.nodes_iter())

            def fetch_results(runner):
                for (r, metadata) in ran:
                    if r is runner:
                        return (True, metadata.get('result'))
                return (False, None)

            result_cb = fetch_results
            return result_cb

        args = [context] + list(args)
        check()

        # Only acquire the run lock (but use further state checking) and the
        # mutation lock to stop simultaneous running and simultaneous mutating
        # which are not allowed on a running flow. Allow simultaneous cancel
        # by performing repeated state checking while running.
        with utils.MultiLock(self._run_locks):
            check()
            connect_and_verify()
            try:
                run_it(get_resumer_cb(), args, kwargs)
            finally:
                handle_results()

    def rollback(self, context, cause):
        """Rolls back all tasks that are *not* still pending or cancelled."""

        def check():
            if self.state not in self.REVERTABLE_STATES:
                raise exc.InvalidStateException("Flow is currently unable "
                                                "to be rolled back in "
                                                "state %s" % (self.state))

        check()

        # All locks must be acquired so that modifications can not be made
        # while another entity is running, rolling-back, cancelling or
        # performing a mutation operation.
        with utils.MultiLock(self._core_locks):
            check()
            accum = utils.RollbackAccumulator()
            for r in self._graph.nodes_iter():
                if r.has_ran():
                    accum.add(utils.RollbackTask(context, r.task, r.result))
            try:
                self._change_state(context, states.REVERTING)
                accum.rollback(cause)
            finally:
                self._change_state(context, states.FAILURE)


class ThreadRunner(utils.Runner):
    """A helper class that will use a countdown latch to avoid calling its
    callable object until said countdown latch has emptied. After it has
    been emptied the predecessor tasks will be examined for dependent results
    and said results will then be provided to call the runners callable
    object.

    TODO(harlowja): this could be a 'future' like object in the future since it
    is starting to have the same purpose and usage (in a way). Likely switch
    this over to the task details object or a subclass of it???
    """
    RESETTABLE_STATES = set([states.PENDING, states.SUCCESS, states.FAILURE,
                             states.CANCELLED])
    RUNNABLE_STATES = set([states.PENDING])
    CANCELABLE_STATES = set([states.PENDING])
    SUCCESS_STATES = set([states.SUCCESS])
    CANCEL_SUCCESSORS_WHEN = set([states.FAILURE, states.CANCELLED,
                                  states.TIMED_OUT])
    NO_RAN_STATES = set([states.CANCELLED, states.PENDING, states.TIMED_OUT,
                         states.RUNNING])

    def __init__(self, task, flow, timeout):
        super(ThreadRunner, self).__init__(task)
        # Use weak references to give the GC a break.
        self._flow = weakref.proxy(flow)
        self._notifier = flow.task_notifier
        self._timeout = timeout
        self._state = states.PENDING
        self._run_lock = threading.RLock()
        # Use the flows state lock so that state notifications are not sent
        # simultaneously for a given flow.
        self._state_lock = flow._state_lock
        self._cancel_lock = threading.RLock()
        self._latch = utils.CountDownLatch()
        # Any related family.
        self._predecessors = []
        self._successors = []
        self._siblings = []
        # Ensure we capture any exceptions that may have been triggered.
        self.exc = None
        self.exc_info = (None, None, None)
        # This callback will be called before the underlying task is actually
        # returned and it should either return a tuple of (has_result, result)
        self._result_cb = None

    @property
    def state(self):
        return self._state

    def has_ran(self):
        if self.state in self.NO_RAN_STATES:
            return False
        return True

    def _change_state(self, context, new_state):
        old_state = None
        changed = False
        with self._state_lock:
            if self.state != new_state:
                old_state = self.state
                self._state = new_state
                changed = True
        # Don't notify while holding the lock so that the reciever of said
        # notifications can actually perform operations on the given runner
        # without getting into deadlock.
        if changed and self._notifier:
            self._notifier.notify(self.state, details={
                'context': context,
                'flow': self._flow,
                'old_state': old_state,
                'runner': self,
            })

    def cancel(self, blocking=True):

        def check():
            if self.state not in self.CANCELABLE_STATES:
                raise exc.InvalidStateException("Runner not in a cancelable"
                                                " state: %s" % (self.state))

        # Check before as a quick way out of attempting to acquire the more
        # heavy-weight lock. Then acquire the lock (which should not be
        # possible if we are currently running) and set the state (if still
        # applicable).
        check()
        acquired = False
        cancelled = False
        try:
            acquired = self._cancel_lock.acquire(blocking=blocking)
            if acquired:
                check()
                cancelled = True
                self._change_state(None, states.CANCELLED)
        finally:
            if acquired:
                self._cancel_lock.release()
        return cancelled

    def reset(self):

        def check():
            if self.state not in self.RESETTABLE_STATES:
                raise exc.InvalidStateException("Runner not in a resettable"
                                                " state: %s" % (self.state))

        def do_reset():
            self._latch.count = len(self._predecessors)
            self.exc = None
            self.exc_info = (None, None, None)
            self.result = None
            self._change_state(None, states.PENDING)

        # We need to acquire both locks here so that we can not be running
        # or being cancelled at the same time we are resetting.
        check()
        with self._run_lock:
            check()
            with self._cancel_lock:
                check()
                do_reset()

    @property
    def runs_before(self):
        # NOTE(harlowja): this list may change, depending on which other
        # runners have completed (or are currently actively running), so
        # this is why this is a property instead of a semi-static defined list
        # like in the AOT class. The list should only get bigger and not
        # smaller so it should be fine to filter on runners that have completed
        # successfully.
        finished_ok = []
        for r in self._siblings:
            if r.has_ran() and r.state in self.SUCCESS_STATES:
                finished_ok.append(r)
        return finished_ok

    def __call__(self, context, *args, **kwargs):

        def is_runnable():
            if self.state not in self.RUNNABLE_STATES:
                return False
            return True

        def run(*args, **kwargs):
            try:
                self._change_state(context, states.RUNNING)
                has_result = False
                if self._result_cb:
                    has_result, self.result = self._result_cb(self)
                if not has_result:
                    super(ThreadRunner, self).__call__(*args, **kwargs)
                self._change_state(context, states.SUCCESS)
            except Exception as e:
                self._change_state(context, states.FAILURE)
                self.exc = e
                self.exc_info = sys.exc_info()

        def signal():
            if not self._successors:
                return
            if self.state in self.CANCEL_SUCCESSORS_WHEN:
                for r in self._successors:
                    try:
                        r.cancel(blocking=False)
                    except exc.InvalidStateException:
                        pass
            for r in self._successors:
                try:
                    r._latch.countDown()
                except Exception:
                    LOG.exception("Failed decrementing %s latch", r)

        # We check before to avoid attempting to acquire the lock when we are
        # known to be in a non-runnable state.
        if not is_runnable():
            return
        args = [context] + list(args)
        with self._run_lock:
            # We check after we now own the run lock since a previous thread
            # could have exited and released that lock and set the state to
            # not runnable.
            if not is_runnable():
                return
            may_proceed = self._latch.await(self._timeout)
            # We now acquire the cancel lock so that we can be assured that
            # we have not been cancelled by another entity.
            with self._cancel_lock:
                try:
                    # If we have been cancelled after awaiting and timing out
                    # ensure that we alter the state to show timed out (but
                    # not if we have been cancelled, since our state should
                    # be cancelled instead). This is done after acquiring the
                    # cancel lock so that we will not try to overwrite another
                    # entity trying to set the runner to the cancel state.
                    if not may_proceed and self.state != states.CANCELLED:
                        self._change_state(context, states.TIMED_OUT)
                    # We at this point should only have been able to time out
                    # or be cancelled, no other state transitions should have
                    # been possible.
                    if self.state not in (states.CANCELLED, states.TIMED_OUT):
                        run(*args, **kwargs)
                finally:
                    signal()
