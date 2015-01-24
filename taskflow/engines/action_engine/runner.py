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

from taskflow import logging
from taskflow import states as st
from taskflow.types import failure
from taskflow.types import fsm

# Waiting state timeout (in seconds).
_WAITING_TIMEOUT = 60

# Meta states the state machine uses.
_UNDEFINED = 'UNDEFINED'
_GAME_OVER = 'GAME_OVER'
_META_STATES = (_GAME_OVER, _UNDEFINED)

# Event name constants the state machine uses.
_SCHEDULE = 'schedule_next'
_WAIT = 'wait_finished'
_ANALYZE = 'examine_finished'
_FINISH = 'completed'
_FAILED = 'failed'
_SUSPENDED = 'suspended'
_SUCCESS = 'success'
_REVERTED = 'reverted'
_START = 'start'

LOG = logging.getLogger(__name__)


class _MachineMemory(object):
    """State machine memory."""

    def __init__(self):
        self.next_nodes = set()
        self.not_done = set()
        self.failures = []
        self.done = set()


class _MachineBuilder(object):
    """State machine *builder* that the runner uses.

    NOTE(harlowja): the machine states that this build will for are::

    +--------------+------------------+------------+----------+---------+
         Start     |      Event       |    End     | On Enter | On Exit
    +--------------+------------------+------------+----------+---------+
       ANALYZING   |    completed     | GAME_OVER  |          |
       ANALYZING   |  schedule_next   | SCHEDULING |          |
       ANALYZING   |  wait_finished   |  WAITING   |          |
       FAILURE[$]  |                  |            |          |
       GAME_OVER   |      failed      |  FAILURE   |          |
       GAME_OVER   |     reverted     |  REVERTED  |          |
       GAME_OVER   |     success      |  SUCCESS   |          |
       GAME_OVER   |    suspended     | SUSPENDED  |          |
        RESUMING   |  schedule_next   | SCHEDULING |          |
      REVERTED[$]  |                  |            |          |
       SCHEDULING  |  wait_finished   |  WAITING   |          |
       SUCCESS[$]  |                  |            |          |
      SUSPENDED[$] |                  |            |          |
      UNDEFINED[^] |      start       |  RESUMING  |          |
        WAITING    | examine_finished | ANALYZING  |          |
    +--------------+------------------+------------+----------+---------+

    Between any of these yielded states (minus ``GAME_OVER`` and ``UNDEFINED``)
    if the engine has been suspended or the engine has failed (due to a
    non-resolveable task failure or scheduling failure) the machine will stop
    executing new tasks (currently running tasks will be allowed to complete)
    and this machines run loop will be broken.
    """

    def __init__(self, runtime, waiter):
        self._analyzer = runtime.analyzer
        self._completer = runtime.completer
        self._scheduler = runtime.scheduler
        self._storage = runtime.storage
        self._waiter = waiter

    def runnable(self):
        return self._storage.get_flow_state() == st.RUNNING

    def build(self, timeout=None):
        memory = _MachineMemory()
        if timeout is None:
            timeout = _WAITING_TIMEOUT

        def resume(old_state, new_state, event):
            # This reaction function just updates the state machines memory
            # to include any nodes that need to be executed (from a previous
            # attempt, which may be empty if never ran before) and any nodes
            # that are now ready to be ran.
            memory.next_nodes.update(self._completer.resume())
            memory.next_nodes.update(self._analyzer.get_next_nodes())
            return _SCHEDULE

        def game_over(old_state, new_state, event):
            # This reaction function is mainly a intermediary delegation
            # function that analyzes the current memory and transitions to
            # the appropriate handler that will deal with the memory values,
            # it is *always* called before the final state is entered.
            if memory.failures:
                return _FAILED
            if self._analyzer.get_next_nodes():
                return _SUSPENDED
            elif self._analyzer.is_success():
                return _SUCCESS
            else:
                return _REVERTED

        def schedule(old_state, new_state, event):
            # This reaction function starts to schedule the memory's next
            # nodes (iff the engine is still runnable, which it may not be
            # if the user of this engine has requested the engine/storage
            # that holds this information to stop or suspend); handles failures
            # that occur during this process safely...
            if self.runnable() and memory.next_nodes:
                not_done, failures = self._scheduler.schedule(
                    memory.next_nodes)
                if not_done:
                    memory.not_done.update(not_done)
                if failures:
                    memory.failures.extend(failures)
                memory.next_nodes.clear()
            return _WAIT

        def wait(old_state, new_state, event):
            # TODO(harlowja): maybe we should start doing 'yield from' this
            # call sometime in the future, or equivalent that will work in
            # py2 and py3.
            if memory.not_done:
                done, not_done = self._waiter.wait_for_any(memory.not_done,
                                                           timeout)
                memory.done.update(done)
                memory.not_done = not_done
            return _ANALYZE

        def analyze(old_state, new_state, event):
            # This reaction function is responsible for analyzing all nodes
            # that have finished executing and completing them and figuring
            # out what nodes are now ready to be ran (and then triggering those
            # nodes to be scheduled in the future); handles failures that
            # occur during this process safely...
            next_nodes = set()
            while memory.done:
                fut = memory.done.pop()
                node = fut.atom
                try:
                    event, result = fut.result()
                    retain = self._completer.complete(node, event, result)
                    if isinstance(result, failure.Failure):
                        if retain:
                            memory.failures.append(result)
                        else:
                            # NOTE(harlowja): avoid making any
                            # intention request to storage unless we are
                            # sure we are in DEBUG enabled logging (otherwise
                            # we will call this all the time even when DEBUG
                            # is not enabled, which would suck...)
                            if LOG.isEnabledFor(logging.DEBUG):
                                intention = self._storage.get_atom_intention(
                                    node.name)
                                LOG.debug("Discarding failure '%s' (in"
                                          " response to event '%s') under"
                                          " completion units request during"
                                          " completion of node '%s' (intention"
                                          " is to %s)", result, event,
                                          node, intention)
                except Exception:
                    memory.failures.append(failure.Failure())
                else:
                    try:
                        more_nodes = self._analyzer.get_next_nodes(node)
                    except Exception:
                        memory.failures.append(failure.Failure())
                    else:
                        next_nodes.update(more_nodes)
            if self.runnable() and next_nodes and not memory.failures:
                memory.next_nodes.update(next_nodes)
                return _SCHEDULE
            elif memory.not_done:
                return _WAIT
            else:
                return _FINISH

        def on_exit(old_state, event):
            LOG.debug("Exiting old state '%s' in response to event '%s'",
                      old_state, event)

        def on_enter(new_state, event):
            LOG.debug("Entering new state '%s' in response to event '%s'",
                      new_state, event)

        # NOTE(harlowja): when ran in debugging mode it is quite useful
        # to track the various state transitions as they happen...
        watchers = {}
        if LOG.isEnabledFor(logging.DEBUG):
            watchers['on_exit'] = on_exit
            watchers['on_enter'] = on_enter

        m = fsm.FSM(_UNDEFINED)
        m.add_state(_GAME_OVER, **watchers)
        m.add_state(_UNDEFINED, **watchers)
        m.add_state(st.ANALYZING, **watchers)
        m.add_state(st.RESUMING, **watchers)
        m.add_state(st.REVERTED, terminal=True, **watchers)
        m.add_state(st.SCHEDULING, **watchers)
        m.add_state(st.SUCCESS, terminal=True, **watchers)
        m.add_state(st.SUSPENDED, terminal=True, **watchers)
        m.add_state(st.WAITING, **watchers)
        m.add_state(st.FAILURE, terminal=True, **watchers)

        m.add_transition(_GAME_OVER, st.REVERTED, _REVERTED)
        m.add_transition(_GAME_OVER, st.SUCCESS, _SUCCESS)
        m.add_transition(_GAME_OVER, st.SUSPENDED, _SUSPENDED)
        m.add_transition(_GAME_OVER, st.FAILURE, _FAILED)
        m.add_transition(_UNDEFINED, st.RESUMING, _START)
        m.add_transition(st.ANALYZING, _GAME_OVER, _FINISH)
        m.add_transition(st.ANALYZING, st.SCHEDULING, _SCHEDULE)
        m.add_transition(st.ANALYZING, st.WAITING, _WAIT)
        m.add_transition(st.RESUMING, st.SCHEDULING, _SCHEDULE)
        m.add_transition(st.SCHEDULING, st.WAITING, _WAIT)
        m.add_transition(st.WAITING, st.ANALYZING, _ANALYZE)

        m.add_reaction(_GAME_OVER, _FINISH, game_over)
        m.add_reaction(st.ANALYZING, _ANALYZE, analyze)
        m.add_reaction(st.RESUMING, _START, resume)
        m.add_reaction(st.SCHEDULING, _SCHEDULE, schedule)
        m.add_reaction(st.WAITING, _WAIT, wait)

        m.freeze()
        return (m, memory)


class Runner(object):
    """Runner that iterates while executing nodes using the given runtime.

    This runner acts as the action engine run loop/state-machine, it resumes
    the workflow, schedules all task it can for execution using the runtimes
    scheduler and analyzer components, and than waits on returned futures and
    then activates the runtimes completion component to finish up those tasks
    and so on...

    NOTE(harlowja): If the runtimes scheduler component is able to schedule
    tasks in parallel, this enables parallel running and/or reversion.
    """

    # Informational states this action yields while running, not useful to
    # have the engine record but useful to provide to end-users when doing
    # execution iterations.
    ignorable_states = (st.SCHEDULING, st.WAITING, st.RESUMING, st.ANALYZING)

    def __init__(self, runtime, waiter):
        self._builder = _MachineBuilder(runtime, waiter)

    @property
    def builder(self):
        return self._builder

    def runnable(self):
        return self._builder.runnable()

    def run_iter(self, timeout=None):
        """Runs the nodes using a built state machine."""
        machine, memory = self.builder.build(timeout=timeout)
        for (_prior_state, new_state) in machine.run_iter(_START):
            # NOTE(harlowja): skip over meta-states.
            if new_state not in _META_STATES:
                if new_state == st.FAILURE:
                    yield (new_state, memory.failures)
                else:
                    yield (new_state, [])
