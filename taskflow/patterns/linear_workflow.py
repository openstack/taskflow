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
import logging

# OrderedDict is only in 2.7 or greater :-(
if not hasattr(dict_provider, 'OrderedDict'):
    import ordereddict as dict_provider

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
        self.reversions = []
        self.name = name
        # If this chain can ignore individual task reversion failure then this
        # should be set to true, instead of the default value of false.
        self.tolerant = tolerant
        # Ordered dicts are used so that we can nicely refer to the tasks by
        # name and easily fetch there results but also allow for the running
        # of said tasks to happen in a linear order.
        self.tasks = dict_provider.OrderedDict()
        self.results = dict_provider.OrderedDict()
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

    def __setitem__(self, name, task):
        self.tasks[name] = task

    def __getitem__(self, name):
        return self.results[name]

    def run(self, context, *args, **kwargs):
        self.state = states.STARTED
        for (name, task) in self.tasks.iteritems():
            try:
                self._on_task_start(context, task, name)
                # See if we have already ran this...
                result = None
                has_result = False
                if self.result_fetcher:
                    (has_result, result) = self.result_fetcher(context,
                                                               name, self)
                if not has_result:
                    result = task.apply(context, *args, **kwargs)
                # Keep a pristine copy of the result in the results table
                # so that if said result is altered by other further states
                # the one here will not be.
                self.results[name] = copy.deepcopy(result)
                self._on_task_finish(context, task, name, result)
                self.state = states.SUCCESS
            except Exception as ex:
                with excutils.save_and_reraise_exception():
                    self.state = states.FAILURE
                    try:
                        self._on_task_error(context, task, name)
                    except Exception:
                        LOG.exception("Dropping exception catched when"
                                      " notifying about existing task"
                                      " exception.")
                    self.state = states.REVERTING
                    self.rollback(context,
                                  exc.TaskException(task, name, self, ex))

    def _on_task_error(self, context, task, name):
        # Notify any listeners that the task has errored.
        for i in self.listeners:
            i.notify(context, states.FAILURE, self, task, name)

    def _on_task_start(self, context, task, name):
        # Notify any listeners that we are about to start the given task.
        for i in self.listeners:
            i.notify(context, states.STARTED, self, task, name)

    def _on_task_finish(self, context, task, name, result):
        # Notify any listeners that we are finishing the given task.
        self.reversions.append((name, task))
        for i in self.listeners:
            i.notify(context, states.SUCCESS, self, task, name, result=result)

    def rollback(self, context, cause):
        for (i, (name, task)) in enumerate(reversed(self.reversions)):
            try:
                task.revert(context, self.results[name], cause)
            except Exception:
                # Ex: WARN: Failed rolling back stage 1 (validate_request) of
                #           chain validation due to Y exception.
                msg = ("Failed rolling back stage %(index)s (%(name)s)"
                       " of workflow %(workflow)s, due to inner exception.")
                LOG.warn(msg % {'index': (i + 1), 'stage': name,
                         'workflow': self.name})
                if not self.tolerant:
                    # NOTE(harlowja): LOG a msg AND re-raise the exception if
                    # the chain does not tolerate exceptions happening in the
                    # rollback method.
                    raise
        if self.parents:
            # Rollback any parents workflows if they exist...
            for p in self.parents:
                p.rollback(context, cause)
