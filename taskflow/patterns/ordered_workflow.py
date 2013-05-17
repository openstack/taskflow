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

LOG = logging.getLogger(__name__)


class Workflow(object):
    """A set tasks that can be applied as one unit or rolled back as one
    unit using an ordered arrangements of said tasks where reversion can be
    handled by reversing through the tasks applied."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, name, tolerant=False, parents=None):
        # The tasks which have been applied will be collected here so that they
        # can be reverted in the correct order on failure.
        self._reversions = []
        self.name = name
        # If this chain can ignore individual task reversion failure then this
        # should be set to true, instead of the default value of false.
        self.tolerant = tolerant
        # If this workflow has a parent workflow/s which need to be reverted if
        # this workflow fails then please include them here to allow this child
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
        raise NotImplementedError()

    def __str__(self):
        return "Workflow: %s" % (self.name)

    @abc.abstractmethod
    def order(self):
        raise NotImplementedError()

    def _fetch_inputs(self, task):
        return {}

    def _perform_reconcilation(self, task, excp):
        # Attempt to reconcile the given exception that occured while applying
        # the given task and either reconcile said task and its associated
        # failure, so that the workflow can continue or abort and perform
        # some type of undo of the tasks already completed.
        try:
            self._change_state(context, states.REVERTING)
        except Exception:
            LOG.exception("Dropping exception catched when"
                          " changing state to reverting while performing"
                          " reconcilation on a tasks exception.")
        cause = exc.TaskException(task, self, excp)
        with excutils.save_and_reraise_exception():
            try:
                self._on_task_error(context, task)
            except Exception:
                LOG.exception("Dropping exception catched when"
                              " notifying about existing task"
                              " exception.")
            # The default strategy will be to rollback all the contained
            # tasks by calling there reverting methods, and then calling
            # any parent workflows rollbacks (and so-on).
            try:
                self.rollback(context, cause)
            finally:
                try:
                    self._change_state(context, states.FAILURE)
                except Exception:
                    LOG.exception("Dropping exception catched when"
                                  " changing state to failure while performing"
                                  " reconcilation on a tasks exception.")

    def run(self, context, *args, **kwargs):
        if self.state != states.PENDING:
            raise exc.InvalidStateException("Unable to run workflow when "
                                            "in state %s" % (self.state))

        if self.result_fetcher:
            result_fetcher = functools.partial(self.result_fetcher, context)
        else:
            result_fetcher = None

        self._change_state(context, states.STARTED)
        task_order = self.order()
        last_task = 0
        if result_fetcher:
            self._change_state(context, states.RESUMING)
            for (i, task) in enumerate(task_order):
                (has_result, result) = result_fetcher(self, task)
                if not has_result:
                    break
                # Fake running the task so that we trigger the same
                # notifications and state changes (and rollback that would
                # have happened in a normal flow).
                last_task = i + 1
                try:
                    self._on_task_start(context, task)
                    # Keep a pristine copy of the result
                    # so that if said result is altered by other further
                    # states the one here will not be. This ensures that
                    # if rollback occurs that the task gets exactly the
                    # result it returned and not a modified one.
                    self.results.append((task, copy.deepcopy(result)))
                    self._on_task_finish(context, task, result)
                except Exception as e:
                    self._perform_reconcilation(task, e)

        self._change_state(context, states.RUNNING)
        was_interrupted = False
        for task in task_order[last_task:]:
            if self.state == states.INTERRUPTED:
                was_interrupted = True
                break
            try:
                has_result = False
                result = None
                if result_fetcher:
                    (has_result, result) = result_fetcher(self, task)
                self._on_task_start(context, task)
                if not has_result:
                    inputs = self._fetch_inputs(task)
                    inputs.update(kwargs)
                    result = task.apply(context, *args, **inputs)
                # Keep a pristine copy of the result
                # so that if said result is altered by other further states
                # the one here will not be. This ensures that if rollback
                # occurs that the task gets exactly the result it returned
                # and not a modified one.
                self.results.append((task, copy.deepcopy(result)))
                self._on_task_finish(context, task, result)
            except Exception as e:
                self._perform_reconcilation(task, e)

        if not was_interrupted:
            # Only gets here if everything went successfully.
            self._change_state(context, states.SUCCESS)

    def reset(self):
        self._state = states.PENDING
        self.results = []
        self._reversions = []

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
        self._reversions.append((task, result))
        for f in self.task_listeners:
            f(context, states.SUCCESS, self, task, result=result)

    def rollback(self, context, cause):
        # Performs basic task by task rollback by going through the reverse
        # order that tasks have finished and asking said task to undo whatever
        # it has done. If this workflow has any parent workflows then they will
        # also be called to rollback any tasks said parents contain.
        #
        # Note(harlowja): if a workflow can more simply revert a whole set of
        # tasks via a simpler command then it can override this method to
        # accomplish that.
        #
        # For example, if each task was creating a file in a directory, then
        # it's easier to just remove the directory than to ask each task to
        # delete its file individually.
        for (i, (task, result)) in enumerate(reversed(self._reversions)):
            try:
                task.revert(context, result, cause)
            except Exception:
                # Ex: WARN: Failed rolling back stage 1 (validate_request) of
                #           chain validation due to Y exception.
                msg = ("Failed rolling back stage %(index)s (%(task)s)"
                       " of workflow %(workflow)s, due to inner exception.")
                LOG.warn(msg % {'index': (i + 1), 'task': task,
                         'workflow': self})
                if not self.tolerant:
                    # NOTE(harlowja): LOG a msg AND re-raise the exception if
                    # the chain does not tolerate exceptions happening in the
                    # rollback method.
                    raise
        if self.parents:
            # Rollback any parents workflows if they exist...
            for p in self.parents:
                p.rollback(context, cause)
