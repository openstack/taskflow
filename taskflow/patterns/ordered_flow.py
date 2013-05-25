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
import copy
import functools
import logging

from taskflow.openstack.common import excutils
from taskflow import exceptions as exc
from taskflow import states
from taskflow import utils

LOG = logging.getLogger(__name__)


class FlowFailure(object):
    """When a task failure occurs the following object will be given to revert
       and can be used to interrogate what caused the failure."""

    def __init__(self, task, flow, exception=None):
        self.task = task
        self.flow = flow
        self.exception = exception


class RollbackTask(object):
    def __init__(self, context, task, result):
        self.task = task
        self.result = result
        self.context = context

    def __str__(self):
        return str(self.task)

    def __call__(self, cause):
        self.task.revert(self.context, self.result, cause)


class Flow(object):
    """A set tasks that can be applied as one unit or rolled back as one
    unit using an ordered arrangements of said tasks where reversion is by
    default handled by reversing through the tasks applied."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, name, parents=None):
        # The tasks which have been applied will be collected here so that they
        # can be reverted in the correct order on failure.
        self._accumulator = utils.RollbackAccumulator()
        self.name = name
        # If this flow has a parent flow/s which need to be reverted if
        # this flow fails then please include them here to allow this child
        # to call the parents...
        self.parents = parents
        # This should be a functor that returns whether a given task has
        # already ran by returning a pair of (has_result, result).
        #
        # NOTE(harlowja): This allows for resumption by skipping tasks which
        # have already occurred. The previous return value is needed due to
        # the contract we have with tasks that they will be given the value
        # they returned if reversion is triggered.
        self.result_fetcher = None
        # Any objects that want to listen when a wf/task starts/stops/completes
        # or errors should be registered here. This can be used to monitor
        # progress and record tasks finishing (so that it becomes possible to
        # store the result of a task in some persistent or semi-persistent
        # storage backend).
        self.task_listeners = []
        self.listeners = []
        # The state of this flow.
        self._state = states.PENDING
        # Tasks results are stored here...
        self.results = []

    @property
    def state(self):
        return self._state

    @abc.abstractmethod
    def add(self, task):
        """Adds a given task to this flow."""
        raise NotImplementedError()

    def __str__(self):
        return "Flow: %s" % (self.name)

    @abc.abstractmethod
    def order(self):
        """Returns the order in which the tasks should be ran
        as a iterable list."""
        raise NotImplementedError()

    def _fetch_task_inputs(self, _task):
        """Retrieves and additional kwargs inputs to provide to the task when
        said task is being applied."""
        return None

    def run(self, context, *args, **kwargs):
        if self.state != states.PENDING:
            raise exc.InvalidStateException("Unable to run flow when "
                                            "in state %s" % (self.state))

        if self.result_fetcher:
            result_fetcher = functools.partial(self.result_fetcher, context)
        else:
            result_fetcher = None

        self._change_state(context, states.STARTED)
        try:
            task_order = self.order()
        except Exception:
            with excutils.save_and_reraise_exception():
                try:
                    self._change_state(context, states.FAILURE)
                except Exception:
                    LOG.exception("Dropping exception catched when"
                                  " notifying about ordering failure.")

        def run_task(task, result=None, simulate_run=False):
            try:
                self._on_task_start(context, task)
                if not simulate_run:
                    inputs = self._fetch_task_inputs(task)
                    if not inputs:
                        inputs = {}
                    inputs.update(kwargs)
                    result = task.apply(context, *args, **inputs)
                # Keep a pristine copy of the result
                # so that if said result is altered by other further
                # states the one here will not be. This ensures that
                # if rollback occurs that the task gets exactly the
                # result it returned and not a modified one.
                self.results.append((task, result))
                # Add the task result to the accumulator before
                # notifying others that the task has finished to
                # avoid the case where a listener might throw an
                # exception.
                self._accumulator.add(RollbackTask(context, task,
                                                   copy.deepcopy(result)))
                self._on_task_finish(context, task, result)
            except Exception as e:
                cause = FlowFailure(task, self, e)
                with excutils.save_and_reraise_exception():
                    self.rollback(context, cause)

        last_task = 0
        was_interrupted = False
        if result_fetcher:
            self._change_state(context, states.RESUMING)
            for (i, task) in enumerate(task_order):
                if self.state == states.INTERRUPTED:
                    was_interrupted = True
                    break
                (has_result, result) = result_fetcher(self, task)
                if not has_result:
                    break
                # Fake running the task so that we trigger the same
                # notifications and state changes (and rollback that
                # would have happened in a normal flow).
                last_task = i + 1
                run_task(task, result=result, simulate_run=True)

        if was_interrupted:
            return

        self._change_state(context, states.RUNNING)
        for task in task_order[last_task:]:
            if self.state == states.INTERRUPTED:
                was_interrupted = True
                break
            run_task(task)

        if not was_interrupted:
            # Only gets here if everything went successfully.
            self._change_state(context, states.SUCCESS)

    def reset(self):
        self._state = states.PENDING
        self.results = []
        self._accumulator.reset()

    def interrupt(self):
        self._change_state(None, states.INTERRUPTED)

    def _change_state(self, context, new_state):
        if self.state != new_state:
            old_state = self.state
            self._state = new_state
            self._on_flow_state_change(context, old_state)

    def _on_flow_state_change(self, context, old_state):
        # Notify any listeners that the internal state has changed.
        for f in self.listeners:
            f(context, self, old_state)

    def _on_task_error(self, context, task):
        # Notify any listeners that the task has errored.
        for f in self.task_listeners:
            f(context, states.FAILURE, self, task)

    def _on_task_start(self, context, task):
        # Notify any listeners that we are about to start the given task.
        for f in self.task_listeners:
            f(context, states.STARTED, self, task)

    def _on_task_finish(self, context, task, result):
        # Notify any listeners that we are finishing the given task.
        for f in self.task_listeners:
            f(context, states.SUCCESS, self, task, result=result)

    def rollback(self, context, cause):
        # Performs basic task by task rollback by going through the reverse
        # order that tasks have finished and asking said task to undo whatever
        # it has done. If this flow has any parent flows then they will
        # also be called to rollback any tasks said parents contain.
        #
        # Note(harlowja): if a flow can more simply revert a whole set of
        # tasks via a simpler command then it can override this method to
        # accomplish that.
        #
        # For example, if each task was creating a file in a directory, then
        # it's easier to just remove the directory than to ask each task to
        # delete its file individually.
        try:
            self._change_state(context, states.REVERTING)
        except Exception:
            LOG.exception("Dropping exception catched when"
                          " changing state to reverting while performing"
                          " reconcilation on a tasks exception.")

        try:
            self._accumulator.rollback(cause)
        finally:
            try:
                self._change_state(context, states.FAILURE)
            except Exception:
                LOG.exception("Dropping exception catched when"
                              " changing state to failure while performing"
                              " reconcilation on a tasks exception.")
        if self.parents:
            # Rollback any parents flows if they exist...
            for p in self.parents:
                p.rollback(context, cause)
