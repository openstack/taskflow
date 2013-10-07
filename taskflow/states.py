# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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

# Job states.
CLAIMED = 'CLAIMED'
FAILURE = 'FAILURE'
PENDING = 'PENDING'
RUNNING = 'RUNNING'
SUCCESS = 'SUCCESS'
UNCLAIMED = 'UNCLAIMED'

# Flow states.
FAILURE = FAILURE
PENDING = 'PENDING'
REVERTING = 'REVERTING'
REVERTED = 'REVERTED'
RUNNING = RUNNING
SUCCESS = SUCCESS
SUSPENDING = 'SUSPENDING'
SUSPENDED = 'SUSPENDED'
RESUMING = 'RESUMING'

# Task states.
FAILURE = FAILURE
SUCCESS = SUCCESS
REVERTED = REVERTED
REVERTING = REVERTING

# TODO(harlowja): use when we can timeout tasks??
TIMED_OUT = 'TIMED_OUT'


## Flow state transitions
# https://wiki.openstack.org/wiki/TaskFlow/States_of_Task_and_Flow#Flow_States

_ALLOWED_FLOW_TRANSITIONS = frozenset((
    (PENDING, RUNNING),       # run it!

    (RUNNING, SUCCESS),       # all tasks finished successfully
    (RUNNING, FAILURE),       # some of task failed
    (RUNNING, SUSPENDING),    # engine.suspend was called

    (SUCCESS, RUNNING),       # see note below

    (FAILURE, RUNNING),       # see note below
    (FAILURE, REVERTING),     # flow failed, do cleanup now

    (REVERTING, REVERTED),    # revert done
    (REVERTING, FAILURE),     # revert failed
    (REVERTING, SUSPENDING),  # engine.suspend was called

    (REVERTED, RUNNING),      # try again

    (SUSPENDING, SUSPENDED),  # suspend finished
    (SUSPENDING, SUCCESS),    # all tasks finished while we were waiting
    (SUSPENDING, FAILURE),    # some tasks failed while we were waiting
    (SUSPENDING, REVERTED),   # all tasks were reverted while we were waiting

    (SUSPENDED, RUNNING),     # restart from suspended
    (SUSPENDED, REVERTING),   # revert from suspended

    (RESUMING, SUSPENDED),    # after flow resumed, it is suspended
))


# NOTE(imelnikov) SUCCESS->RUNNING and FAILURE->RUNNING transitions are
# useful when flow or flowdetails baciking it were altered after the flow
# was finished; then, client code may want to run through flow again
# to ensure all tasks from updated flow had a chance to run.


# NOTE(imelnikov): Engine cannot transition flow from SUSPENDING to
# SUSPENDED while some tasks from the flow are running and some results
# from them are not retrieved and saved properly, so while flow is
# in SUSPENDING state it may wait for some of the tasks to stop. Then,
# flow can go to SUSPENDED, SUCCESS, FAILURE or REVERTED state depending
# of actual state of the tasks -- e.g. if all tasks were finished
# successfully while we were waiting, flow can be transitioned from
# SUSPENDING to SUCCESS state.


_IGNORED_FLOW_TRANSITIONS = frozenset(
    (a, b)
    for a in (PENDING, FAILURE, SUCCESS, SUSPENDED, REVERTED)
    for b in (SUSPENDING, SUSPENDED, RESUMING)
    if a != b
)


def check_flow_transition(old_state, new_state):
    """Check that flow can transition from old_state to new_state.

    If transition can be performed, it returns True. If transition
    should be ignored, it returns False. If transition is not
    valid, it raises InvalidStateException.
    """
    if old_state == new_state:
        return False
    pair = (old_state, new_state)
    if pair in _ALLOWED_FLOW_TRANSITIONS:
        return True
    if pair in _IGNORED_FLOW_TRANSITIONS:
        return False
    if new_state == RESUMING:
        return True
    raise exc.InvalidStateException(
        "Flow transition from %s to %s is not allowed" % pair)
