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

import collections as dict_provider
import copy
import functools
import logging

from taskflow.openstack.common import excutils
from taskflow import exceptions as exc
from taskflow import states

LOG = logging.getLogger(__name__)


class Workflow(object):
    """A linear chain of independent tasks that can be applied as one unit or
       rolled back as one unit."""

    def __init__(self, name, tolerant=False, parents=None):
        # The tasks which have been applied will be collected here so that they
        # can be reverted in the correct order on failure.
        self._reversions = []
        self.name = name
        # If this chain can ignore individual task reversion failure then this
        # should be set to true, instead of the default value of false.
        self.tolerant = tolerant
        # Tasks and there results are stored here...
        self.tasks = []
        self.results = []
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
        # Any objects that want to listen when a task starts/stops/completes
        # or errors should be registered here. This can be used to monitor
        # progress and record tasks finishing (so that it becomes possible to
        # store the result of a task in some persistent or semi-persistent
        # storage backend).
        self.listeners = []
        # The state of this flow.
        self.state = states.PENDING

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, id(self))

    def run(self, context, *args, **kwargs):
        if self.state != states.PENDING:
            raise exc.InvalidStateException("Unable to run linear flow when "
                                            "in state %s" % (self.state))

        if self.result_fetcher:
            result_fetcher = functools.partial(self.result_fetcher, context)
        else:
            result_fetcher = None

        self._change_state(context, states.STARTED)

        # TODO(harlowja): we can likely add in custom reconcilation strategies
        # here or around here...
        def do_rollback_for(task, ex):
            self._change_state(context, states.REVERTING)
            with excutils.save_and_reraise_exception():
                try:
                    self._on_task_error(context, task)
                except Exception:
                    LOG.exception("Dropping exception catched when"
                                  " notifying about existing task"
                                  " exception.")
                self.rollback(context, exc.TaskException(task, self, ex))
                self._change_state(context, states.FAILURE)

        self._change_state(context, states.RESUMING)
        last_task = 0
        if result_fetcher:
            for (i, task) in enumerate(self.tasks):
                (has_result, result) = result_fetcher(context, self, task)
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
                except Exception as ex:
                    do_rollback_for(task, ex)

        self._change_state(context, states.RUNNING)
        for task in self.tasks[last_task:]:
            try:
                self._on_task_start(context, task)
                result = task.apply(context, *args, **kwargs)
                # Keep a pristine copy of the result
                # so that if said result is altered by other further states
                # the one here will not be. This ensures that if rollback
                # occurs that the task gets exactly the result it returned
                # and not a modified one.
                self.results.append((task, copy.deepcopy(result)))
                self._on_task_finish(context, task, result)
            except Exception as ex:
                do_rollback_for(task, ex)

        # Only gets here if everything went successfully.
        self._change_state(context, states.SUCCESS)

    def _change_state(self, context, new_state):
        if self.state != new_state:
            self.state = new_state
            self._on_flow_state_change(context)

    def _on_flow_state_change(self, context):
        # Notify any listeners that the internal state has changed.
        for i in self.listeners:
            i.notify(context, self)

    def _on_task_error(self, context, task):
        # Notify any listeners that the task has errored.
        for i in self.listeners:
            i.notify(context, states.FAILURE, self, task)

    def _on_task_start(self, context, task):
        # Notify any listeners that we are about to start the given task.
        for i in self.listeners:
            i.notify(context, states.STARTED, self, task)

    def _on_task_finish(self, context, task, result):
        # Notify any listeners that we are finishing the given task.
        self._reversions.append((task, result))
        for i in self.listeners:
            i.notify(context, states.SUCCESS, self, task, result=result)

    def rollback(self, context, cause):
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
