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

import abc
import threading

from taskflow import decorators
from taskflow import exceptions as exc
from taskflow import states


class Flow(object):
    """The base abstract class of all flow implementations."""
    __metaclass__ = abc.ABCMeta

    RESETTABLE_STATES = set([
        states.INTERRUPTED,
        states.SUCCESS,
        states.PENDING,
        states.FAILURE,
    ])
    SOFT_RESETTABLE_STATES = set([
        states.INTERRUPTED,
    ])
    UNINTERRUPTIBLE_STATES = set([
        states.FAILURE,
        states.SUCCESS,
        states.PENDING,
    ])
    RUNNABLE_STATES = set([
        states.PENDING,
    ])

    def __init__(self, name, parents=None):
        self.name = name
        # The state of this flow.
        self._state = states.PENDING
        # If this flow has a parent flow/s which need to be reverted if
        # this flow fails then please include them here to allow this child
        # to call the parents...
        self.parents = parents
        # Any objects that want to listen when a wf/task starts/stops/completes
        # or errors should be registered here. This can be used to monitor
        # progress and record tasks finishing and other advanced features that
        # can be implemented when you can track a flows progress.
        self.task_listeners = []
        self.listeners = []
        # Ensure that modifications and/or multiple runs aren't happening
        # at the same time in the same flow at the same time.
        self._lock = threading.RLock()

    @property
    def state(self):
        """Provides a read-only view of the flow state."""
        return self._state

    def _change_state(self, context, new_state):
        was_changed = False
        old_state = self.state
        with self._lock:
            if self.state != new_state:
                old_state = self.state
                self._state = new_state
                was_changed = True
        if was_changed:
            # Don't notify while holding the lock.
            self._on_flow_state_change(context, old_state)

    def __str__(self):
        lines = ["Flow: %s" % (self.name)]
        lines.append("  State: %s" % (self.state))
        return "\n".join(lines)

    def _on_flow_state_change(self, context, old_state):
        # Notify any listeners that the internal state has changed.
        for f in self.listeners:
            f(context, self, old_state)

    def _on_task_error(self, context, task, cause):
        # Notify any listeners that the task has errored.
        for f in self.task_listeners:
            f(context, states.FAILURE, self, task, result=cause)

    def _on_task_start(self, context, task):
        # Notify any listeners that we are about to start the given task.
        for f in self.task_listeners:
            f(context, states.STARTED, self, task)

    def _on_task_finish(self, context, task, result):
        # Notify any listeners that we are finishing the given task.
        for f in self.task_listeners:
            f(context, states.SUCCESS, self, task, result=result)

    @abc.abstractmethod
    def add(self, task):
        """Adds a given task to this flow.

        Returns the uuid that is associated with the task for later operations
        before and after it is ran."""
        raise NotImplementedError()

    @abc.abstractmethod
    def add_many(self, tasks):
        """Adds many tasks to this flow.

        Returns a list of uuids (one for each task added).
        """
        raise NotImplementedError()

    def interrupt(self):
        """Attempts to interrupt the current flow and any tasks that are
        currently not running in the flow.

        Returns how many tasks were interrupted (if any).
        """
        if self.state in self.UNINTERRUPTIBLE_STATES:
            raise exc.InvalidStateException(("Can not interrupt when"
                                             " in state %s") % (self.state))
        # Note(harlowja): Do *not* acquire the lock here so that the flow may
        # be interrupted while running. This does mean the the above check may
        # not be valid but we can worry about that if it becomes an issue.
        old_state = self.state
        if old_state != states.INTERRUPTED:
            self._state = states.INTERRUPTED
            self._on_flow_state_change(None, old_state)

    @decorators.locked
    def reset(self):
        """Fully resets the internal state of this flow, allowing for the flow
        to be ran again. *Listeners are also reset*"""
        if self.state not in self.RESETTABLE_STATES:
            raise exc.InvalidStateException(("Can not reset when"
                                             " in state %s") % (self.state))
        self.task_listeners = []
        self.listeners = []
        self._change_state(None, states.PENDING)

    @decorators.locked
    def soft_reset(self):
        """Partially resets the internal state of this flow, allowing for the
        flow to be ran again from an interrupted state *only*"""
        if self.state not in self.SOFT_RESETTABLE_STATES:
            raise exc.InvalidStateException(("Can not soft reset when"
                                             " in state %s") % (self.state))
        self._change_state(None, states.PENDING)

    @decorators.locked
    def run(self, context, *args, **kwargs):
        """Executes the workflow."""
        if self.state not in self.RUNNABLE_STATES:
            raise exc.InvalidStateException("Unable to run flow when "
                                            "in state %s" % (self.state))

    @decorators.locked
    def rollback(self, context, cause):
        """Performs rollback of this workflow and any attached parent workflows
        if present."""
        pass
