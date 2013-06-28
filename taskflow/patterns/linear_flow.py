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

import collections
import copy
import functools
import logging

from taskflow.openstack.common import excutils

from taskflow import exceptions as exc
from taskflow import states
from taskflow import utils

from taskflow.patterns import base

LOG = logging.getLogger(__name__)


class Flow(base.Flow):
    """"A linear chain of tasks that can be applied in order as one unit and
       rolled back as one unit using the reverse order that the tasks have
       been applied in.

       Note(harlowja): Each task in the chain must have requirements
       which are satisfied by the previous task/s in the chain."""

    def __init__(self, name, parents=None):
        super(Flow, self).__init__(name, parents)
        # The tasks which have been applied will be collected here so that they
        # can be reverted in the correct order on failure.
        self._accumulator = utils.RollbackAccumulator()
        # This should be a functor that returns whether a given task has
        # already ran by returning a pair of (has_result, was_error, result).
        #
        # NOTE(harlowja): This allows for resumption by skipping tasks which
        # have already occurred. The previous return value is needed due to
        # the contract we have with tasks that they will be given the value
        # they returned if reversion is triggered.
        self.result_fetcher = None
        # Tasks results are stored here...
        self.results = []
        # The last task index in the order we left off at before being
        # interrupted (or failing).
        self._left_off_at = 0
        # All tasks to run are collected here.
        self._tasks = []

    def add_many(self, tasks):
        for t in tasks:
            self.add(t)

    def add(self, task):
        """Adds a given task to this flow."""
        assert isinstance(task, collections.Callable)
        self._validate_provides(task)
        self._tasks.append(task)

    def _validate_provides(self, task):
        # Ensure that some previous task provides this input.
        missing_requires = []
        for r in utils.get_attr(task, 'requires', []):
            found_provider = False
            for prev_task in reversed(self._tasks):
                if r in utils.get_attr(prev_task, 'provides', []):
                    found_provider = True
                    break
            if not found_provider:
                missing_requires.append(r)
        # Ensure that the last task provides all the needed input for this
        # task to run correctly.
        if len(missing_requires):
            msg = ("There is no previous task providing the outputs %s"
                   " for %s to correctly execute.") % (missing_requires, task)
            raise exc.InvalidStateException(msg)

    def __str__(self):
        lines = ["LinearFlow: %s" % (self.name)]
        lines.append("  Number of tasks: %s" % (len(self._tasks)))
        lines.append("  Last index: %s" % (self._left_off_at))
        lines.append("  State: %s" % (self.state))
        return "\n".join(lines)

    def _ordering(self):
        return list(self._tasks)

    def _fetch_task_inputs(self, task):
        """Retrieves and additional kwargs inputs to provide to the task when
        said task is being applied."""
        would_like = set(utils.get_attr(task, 'requires', []))
        would_like.update(utils.get_attr(task, 'optional', []))

        inputs = {}
        for n in would_like:
            # Find the last task that provided this.
            for (last_task, last_results) in reversed(self.results):
                if n not in utils.get_attr(last_task, 'provides', []):
                    continue
                if last_results and n in last_results:
                    inputs[n] = last_results[n]
                else:
                    inputs[n] = None
                # Some task said they had it, get the next requirement.
                break
        return inputs

    def run(self, context, *args, **kwargs):
        super(Flow, self).run(context, *args, **kwargs)

        if self.result_fetcher:
            result_fetcher = functools.partial(self.result_fetcher, context)
        else:
            result_fetcher = None

        self._change_state(context, states.STARTED)
        try:
            task_order = self._ordering()
            if self._left_off_at > 0:
                task_order = task_order[self._left_off_at:]
        except Exception:
            with excutils.save_and_reraise_exception():
                try:
                    self._change_state(context, states.FAILURE)
                except Exception:
                    LOG.exception("Dropping exception catched when"
                                  " notifying about ordering failure.")

        def run_task(task, failed=False, result=None, simulate_run=False):
            try:
                self._on_task_start(context, task)
                # Add the task to be rolled back *immediately* so that even if
                # the task fails while producing results it will be given a
                # chance to rollback.
                rb = utils.RollbackTask(context, task, result=None)
                self._accumulator.add(rb)
                if not simulate_run:
                    inputs = self._fetch_task_inputs(task)
                    if not inputs:
                        inputs = {}
                    inputs.update(kwargs)
                    result = task(context, *args, **inputs)
                else:
                    if failed:
                        if not result:
                            # If no exception or exception message was provided
                            # or captured from the previous run then we need to
                            # form one for this task.
                            result = "%s failed running." % (task)
                        if isinstance(result, basestring):
                            result = exc.InvalidStateException(result)
                        if not isinstance(result, Exception):
                            LOG.warn("Can not raise a non-exception"
                                     " object: %s", result)
                            result = exc.InvalidStateException()
                        raise result
                # Adjust the task result in the accumulator before
                # notifying others that the task has finished to
                # avoid the case where a listener might throw an
                # exception.
                #
                # Note(harlowja): Keep the original result in the
                # accumulator only and give a duplicated copy to
                # avoid the original result being altered by other
                # tasks.
                #
                # This is due to python being by reference (which means
                # some task could alter this result intentionally or not
                # intentionally).
                rb.result = result
                # Alter the index we have ran at.
                self._left_off_at += 1
                result_copy = copy.deepcopy(result)
                self.results.append((task, result_copy))
                self._on_task_finish(context, task, result_copy)
            except Exception as e:
                cause = utils.FlowFailure(task, self, e)
                with excutils.save_and_reraise_exception():
                    try:
                        self._on_task_error(context, task, e)
                    except Exception:
                        LOG.exception("Dropping exception catched when"
                                      " notifying about task failure.")
                    self.rollback(context, cause)

        last_task = 0
        was_interrupted = False
        if result_fetcher:
            self._change_state(context, states.RESUMING)
            for (i, task) in enumerate(task_order):
                if self.state == states.INTERRUPTED:
                    was_interrupted = True
                    break
                (has_result, was_error, result) = result_fetcher(self, task)
                if not has_result:
                    break
                # Fake running the task so that we trigger the same
                # notifications and state changes (and rollback that
                # would have happened in a normal flow).
                last_task = i + 1
                run_task(task, failed=was_error, result=result,
                         simulate_run=True)

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
        super(Flow, self).reset()
        self.results = []
        self.result_fetcher = None
        self._accumulator.reset()
        self._left_off_at = 0

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
