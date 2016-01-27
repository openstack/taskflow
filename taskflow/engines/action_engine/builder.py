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

from concurrent import futures
import weakref

from automaton import machines
from oslo_utils import timeutils

from taskflow import logging
from taskflow import states as st
from taskflow.types import failure
from taskflow.utils import iter_utils

# Default waiting state timeout (in seconds).
WAITING_TIMEOUT = 60

# Meta states the state machine uses.
UNDEFINED = 'UNDEFINED'
GAME_OVER = 'GAME_OVER'
META_STATES = (GAME_OVER, UNDEFINED)

# Event name constants the state machine uses.
SCHEDULE = 'schedule_next'
WAIT = 'wait_finished'
ANALYZE = 'examine_finished'
FINISH = 'completed'
FAILED = 'failed'
SUSPENDED = 'suspended'
SUCCESS = 'success'
REVERTED = 'reverted'
START = 'start'

# Internal enums used to denote how/if a atom was completed."""
FAILED_COMPLETING = 'failed_completing'
WAS_CANCELLED = 'was_cancelled'
SUCCESSFULLY_COMPLETED = 'successfully_completed'


# For these states we will gather how long (in seconds) the
# state was in-progress (cumulatively if the state is entered multiple
# times)
TIMED_STATES = (st.ANALYZING, st.RESUMING, st.SCHEDULING, st.WAITING)

LOG = logging.getLogger(__name__)


class MachineMemory(object):
    """State machine memory."""

    def __init__(self):
        self.next_up = set()
        self.not_done = set()
        self.failures = []
        self.done = set()

    def cancel_futures(self):
        """Attempts to cancel any not done futures."""
        for fut in self.not_done:
            fut.cancel()


class MachineBuilder(object):
    """State machine *builder* that powers the engine components.

    NOTE(harlowja): the machine (states and events that will trigger
    transitions) that this builds is represented by the following
    table::

        +--------------+------------------+------------+----------+---------+
        |    Start     |      Event       |    End     | On Enter | On Exit |
        +--------------+------------------+------------+----------+---------+
        |  ANALYZING   |    completed     | GAME_OVER  |    .     |    .    |
        |  ANALYZING   |  schedule_next   | SCHEDULING |    .     |    .    |
        |  ANALYZING   |  wait_finished   |  WAITING   |    .     |    .    |
        |  FAILURE[$]  |        .         |     .      |    .     |    .    |
        |  GAME_OVER   |      failed      |  FAILURE   |    .     |    .    |
        |  GAME_OVER   |     reverted     |  REVERTED  |    .     |    .    |
        |  GAME_OVER   |     success      |  SUCCESS   |    .     |    .    |
        |  GAME_OVER   |    suspended     | SUSPENDED  |    .     |    .    |
        |   RESUMING   |  schedule_next   | SCHEDULING |    .     |    .    |
        | REVERTED[$]  |        .         |     .      |    .     |    .    |
        |  SCHEDULING  |  wait_finished   |  WAITING   |    .     |    .    |
        |  SUCCESS[$]  |        .         |     .      |    .     |    .    |
        | SUSPENDED[$] |        .         |     .      |    .     |    .    |
        | UNDEFINED[^] |      start       |  RESUMING  |    .     |    .    |
        |   WAITING    | examine_finished | ANALYZING  |    .     |    .    |
        +--------------+------------------+------------+----------+---------+

    Between any of these yielded states (minus ``GAME_OVER`` and ``UNDEFINED``)
    if the engine has been suspended or the engine has failed (due to a
    non-resolveable task failure or scheduling failure) the machine will stop
    executing new tasks (currently running tasks will be allowed to complete)
    and this machines run loop will be broken.

    NOTE(harlowja): If the runtimes scheduler component is able to schedule
    tasks in parallel, this enables parallel running and/or reversion.
    """

    def __init__(self, runtime, waiter):
        self._runtime = weakref.proxy(runtime)
        self._analyzer = runtime.analyzer
        self._completer = runtime.completer
        self._scheduler = runtime.scheduler
        self._storage = runtime.storage
        self._waiter = waiter

    def build(self, statistics, timeout=None, gather_statistics=True):
        """Builds a state-machine (that is used during running)."""
        if gather_statistics:
            watches = {}
            state_statistics = {}
            statistics['seconds_per_state'] = state_statistics
            watches = {}
            for timed_state in TIMED_STATES:
                state_statistics[timed_state.lower()] = 0.0
                watches[timed_state] = timeutils.StopWatch()
            statistics['discarded_failures'] = 0
            statistics['awaiting'] = 0
            statistics['completed'] = 0
            statistics['incomplete'] = 0

        memory = MachineMemory()
        if timeout is None:
            timeout = WAITING_TIMEOUT

        # Cache some local functions/methods...
        do_complete = self._completer.complete
        do_complete_failure = self._completer.complete_failure
        get_atom_intention = self._storage.get_atom_intention

        def do_schedule(next_nodes):
            return self._scheduler.schedule(
                sorted(next_nodes,
                       key=lambda node: getattr(node, 'priority', 0),
                       reverse=True))

        def iter_next_atoms(atom=None, apply_deciders=True):
            # Yields and filters and tweaks the next atoms to run...
            maybe_atoms_it = self._analyzer.iter_next_atoms(atom=atom)
            for atom, late_decider in maybe_atoms_it:
                if apply_deciders:
                    proceed = late_decider.check_and_affect(self._runtime)
                    if proceed:
                        yield atom
                else:
                    yield atom

        def resume(old_state, new_state, event):
            # This reaction function just updates the state machines memory
            # to include any nodes that need to be executed (from a previous
            # attempt, which may be empty if never ran before) and any nodes
            # that are now ready to be ran.
            memory.next_up.update(
                iter_utils.unique_seen((self._completer.resume(),
                                        iter_next_atoms())))
            return SCHEDULE

        def game_over(old_state, new_state, event):
            # This reaction function is mainly a intermediary delegation
            # function that analyzes the current memory and transitions to
            # the appropriate handler that will deal with the memory values,
            # it is *always* called before the final state is entered.
            if memory.failures:
                return FAILED
            leftover_atoms = iter_utils.count(
                # Avoid activating the deciders, since at this point
                # the engine is finishing and there will be no more further
                # work done anyway...
                iter_next_atoms(apply_deciders=False))
            if leftover_atoms:
                # Ok we didn't finish (either reverting or executing...) so
                # that means we must of been stopped at some point...
                LOG.trace("Suspension determined to have been reacted to"
                          " since (at least) %s atoms have been left in an"
                          " unfinished state", leftover_atoms)
                return SUSPENDED
            elif self._analyzer.is_success():
                return SUCCESS
            else:
                return REVERTED

        def schedule(old_state, new_state, event):
            # This reaction function starts to schedule the memory's next
            # nodes (iff the engine is still runnable, which it may not be
            # if the user of this engine has requested the engine/storage
            # that holds this information to stop or suspend); handles failures
            # that occur during this process safely...
            current_flow_state = self._storage.get_flow_state()
            if current_flow_state == st.RUNNING and memory.next_up:
                not_done, failures = do_schedule(memory.next_up)
                if not_done:
                    memory.not_done.update(not_done)
                if failures:
                    memory.failures.extend(failures)
                memory.next_up.intersection_update(not_done)
            elif current_flow_state == st.SUSPENDING and memory.not_done:
                # Try to force anything not cancelled to now be cancelled
                # so that the executor that gets it does not continue to
                # try to work on it (if the future execution is still in
                # its backlog, if it's already being executed, this will
                # do nothing).
                memory.cancel_futures()
            return WAIT

        def complete_an_atom(fut):
            # This completes a single atom saving its result in
            # storage and preparing whatever predecessors or successors will
            # now be ready to execute (or revert or retry...); it also
            # handles failures that occur during this process safely...
            atom = fut.atom
            try:
                outcome, result = fut.result()
                do_complete(atom, outcome, result)
                if isinstance(result, failure.Failure):
                    retain = do_complete_failure(atom, outcome, result)
                    if retain:
                        memory.failures.append(result)
                    else:
                        # NOTE(harlowja): avoid making any intention request
                        # to storage unless we are sure we are in DEBUG
                        # enabled logging (otherwise we will call this all
                        # the time even when DEBUG is not enabled, which
                        # would suck...)
                        if LOG.isEnabledFor(logging.DEBUG):
                            intention = get_atom_intention(atom.name)
                            LOG.debug("Discarding failure '%s' (in response"
                                      " to outcome '%s') under completion"
                                      " units request during completion of"
                                      " atom '%s' (intention is to %s)",
                                      result, outcome, atom, intention)
                        if gather_statistics:
                            statistics['discarded_failures'] += 1
                if gather_statistics:
                    statistics['completed'] += 1
            except futures.CancelledError:
                # Well it got cancelled, skip doing anything
                # and move on; at a further time it will be resumed
                # and something should be done with it to get it
                # going again.
                return WAS_CANCELLED
            except Exception:
                memory.failures.append(failure.Failure())
                LOG.exception("Engine '%s' atom post-completion"
                              " failed", atom)
                return FAILED_COMPLETING
            else:
                return SUCCESSFULLY_COMPLETED

        def wait(old_state, new_state, event):
            # TODO(harlowja): maybe we should start doing 'yield from' this
            # call sometime in the future, or equivalent that will work in
            # py2 and py3.
            if memory.not_done:
                done, not_done = self._waiter(memory.not_done, timeout=timeout)
                memory.done.update(done)
                memory.not_done = not_done
            return ANALYZE

        def analyze(old_state, new_state, event):
            # This reaction function is responsible for analyzing all nodes
            # that have finished executing/reverting and figuring
            # out what nodes are now ready to be ran (and then triggering those
            # nodes to be scheduled in the future); handles failures that
            # occur during this process safely...
            next_up = set()
            while memory.done:
                fut = memory.done.pop()
                # Force it to be completed so that we can ensure that
                # before we iterate over any successors or predecessors
                # that we know it has been completed and saved and so on...
                completion_status = complete_an_atom(fut)
                if (not memory.failures
                        and completion_status != WAS_CANCELLED):
                    atom = fut.atom
                    try:
                        more_work = set(iter_next_atoms(atom=atom))
                    except Exception:
                        memory.failures.append(failure.Failure())
                        LOG.exception("Engine '%s' atom post-completion"
                                      " next atom searching failed", atom)
                    else:
                        next_up.update(more_work)
            current_flow_state = self._storage.get_flow_state()
            if (current_flow_state == st.RUNNING
                    and next_up and not memory.failures):
                memory.next_up.update(next_up)
                return SCHEDULE
            elif memory.not_done:
                if current_flow_state == st.SUSPENDING:
                    memory.cancel_futures()
                return WAIT
            else:
                return FINISH

        def on_exit(old_state, event):
            LOG.trace("Exiting old state '%s' in response to event '%s'",
                      old_state, event)
            if gather_statistics:
                if old_state in watches:
                    w = watches[old_state]
                    w.stop()
                    state_statistics[old_state.lower()] += w.elapsed()
                if old_state in (st.SCHEDULING, st.WAITING):
                    statistics['incomplete'] = len(memory.not_done)
                if old_state in (st.ANALYZING, st.SCHEDULING):
                    statistics['awaiting'] = len(memory.next_up)

        def on_enter(new_state, event):
            LOG.trace("Entering new state '%s' in response to event '%s'",
                      new_state, event)
            if gather_statistics and new_state in watches:
                watches[new_state].restart()

        state_kwargs = {
            'on_exit': on_exit,
            'on_enter': on_enter,
        }
        m = machines.FiniteMachine()
        m.add_state(GAME_OVER, **state_kwargs)
        m.add_state(UNDEFINED, **state_kwargs)
        m.add_state(st.ANALYZING, **state_kwargs)
        m.add_state(st.RESUMING, **state_kwargs)
        m.add_state(st.REVERTED, terminal=True, **state_kwargs)
        m.add_state(st.SCHEDULING, **state_kwargs)
        m.add_state(st.SUCCESS, terminal=True, **state_kwargs)
        m.add_state(st.SUSPENDED, terminal=True, **state_kwargs)
        m.add_state(st.WAITING, **state_kwargs)
        m.add_state(st.FAILURE, terminal=True, **state_kwargs)
        m.default_start_state = UNDEFINED

        m.add_transition(GAME_OVER, st.REVERTED, REVERTED)
        m.add_transition(GAME_OVER, st.SUCCESS, SUCCESS)
        m.add_transition(GAME_OVER, st.SUSPENDED, SUSPENDED)
        m.add_transition(GAME_OVER, st.FAILURE, FAILED)
        m.add_transition(UNDEFINED, st.RESUMING, START)
        m.add_transition(st.ANALYZING, GAME_OVER, FINISH)
        m.add_transition(st.ANALYZING, st.SCHEDULING, SCHEDULE)
        m.add_transition(st.ANALYZING, st.WAITING, WAIT)
        m.add_transition(st.RESUMING, st.SCHEDULING, SCHEDULE)
        m.add_transition(st.SCHEDULING, st.WAITING, WAIT)
        m.add_transition(st.WAITING, st.ANALYZING, ANALYZE)

        m.add_reaction(GAME_OVER, FINISH, game_over)
        m.add_reaction(st.ANALYZING, ANALYZE, analyze)
        m.add_reaction(st.RESUMING, START, resume)
        m.add_reaction(st.SCHEDULING, SCHEDULE, schedule)
        m.add_reaction(st.WAITING, WAIT, wait)

        m.freeze()
        return (m, memory)
