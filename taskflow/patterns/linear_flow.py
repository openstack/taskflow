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

from taskflow import decorators
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
        # Tasks results are stored here. Lookup is by the uuid that was
        # returned from the add function.
        self.results = {}
        # The last index in the order we left off at before being
        # interrupted (or failing).
        self._left_off_at = 0
        # All runners to run are collected here.
        self._runners = []

    @decorators.locked
    def add_many(self, tasks):
        uuids = []
        for t in tasks:
            uuids.append(self.add(t))
        return uuids

    @decorators.locked
    def add(self, task):
        """Adds a given task to this flow."""
        assert isinstance(task, collections.Callable)
        r = utils.Runner(task)
        r.runs_before = list(reversed(self._runners))
        self._associate_providers(r)
        self._runners.append(r)
        return r.uuid

    def _associate_providers(self, runner):
        # Ensure that some previous task provides this input.
        who_provides = {}
        task_requires = set(utils.get_attr(runner.task, 'requires', []))
        LOG.debug("Finding providers of %s for %s", task_requires, runner)
        for r in task_requires:
            provider = None
            for before_me in runner.runs_before:
                if r in set(utils.get_attr(before_me.task, 'provides', [])):
                    provider = before_me
                    break
            if provider:
                LOG.debug("Found provider of %s from %s", r, provider)
                who_provides[r] = provider
        # Ensure that the last task provides all the needed input for this
        # task to run correctly.
        missing_requires = task_requires - set(who_provides.keys())
        if missing_requires:
            raise exc.MissingDependencies(runner, sorted(missing_requires))
        runner.providers.update(who_provides)

    def __str__(self):
        lines = ["LinearFlow: %s" % (self.name)]
        lines.append("  Number of tasks: %s" % (len(self._runners)))
        lines.append("  Last index: %s" % (self._left_off_at))
        lines.append("  State: %s" % (self.state))
        return "\n".join(lines)

    def _ordering(self):
        return self._runners

    @decorators.locked
    def run(self, context, *args, **kwargs):
        super(Flow, self).run(context, *args, **kwargs)

        if self.result_fetcher:
            result_fetcher = functools.partial(self.result_fetcher, context)
        else:
            result_fetcher = None

        self._change_state(context, states.STARTED)
        try:
            run_order = self._ordering()
            if self._left_off_at > 0:
                run_order = run_order[self._left_off_at:]
        except Exception:
            with excutils.save_and_reraise_exception():
                try:
                    self._change_state(context, states.FAILURE)
                except Exception:
                    LOG.exception("Dropping exception catched when"
                                  " notifying about ordering failure.")

        def run_it(runner, failed=False, result=None, simulate_run=False):
            try:
                self._on_task_start(context, runner.task)
                # Add the task to be rolled back *immediately* so that even if
                # the task fails while producing results it will be given a
                # chance to rollback.
                rb = utils.RollbackTask(context, runner.task, result=None)
                self._accumulator.add(rb)
                if not simulate_run:
                    result = runner(context, *args, **kwargs)
                else:
                    if failed:
                        if not result:
                            # If no exception or exception message was provided
                            # or captured from the previous run then we need to
                            # form one for this task.
                            result = "%s failed running." % (runner.task)
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
                runner.result = result
                # Alter the index we have ran at.
                self._left_off_at += 1
                self.results[runner.uuid] = copy.deepcopy(result)
                self._on_task_finish(context, runner.task, result)
            except Exception as e:
                cause = utils.FlowFailure(runner.task, self, e)
                with excutils.save_and_reraise_exception():
                    try:
                        self._on_task_error(context, runner.task, e)
                    except Exception:
                        LOG.exception("Dropping exception catched when"
                                      " notifying about task failure.")
                    self.rollback(context, cause)

        # Ensure in a ready to run state.
        for runner in run_order:
            runner.reset()

        last_runner = 0
        was_interrupted = False
        if result_fetcher:
            self._change_state(context, states.RESUMING)
            for (i, runner) in enumerate(run_order):
                if self.state == states.INTERRUPTED:
                    was_interrupted = True
                    break
                (has_result, was_error, result) = result_fetcher(self,
                                                                 runner.task)
                if not has_result:
                    break
                # Fake running the task so that we trigger the same
                # notifications and state changes (and rollback that
                # would have happened in a normal flow).
                last_runner = i + 1
                run_it(runner, failed=was_error, result=result,
                       simulate_run=True)

        if was_interrupted:
            return

        self._change_state(context, states.RUNNING)
        for runner in run_order[last_runner:]:
            if self.state == states.INTERRUPTED:
                was_interrupted = True
                break
            run_it(runner)

        if not was_interrupted:
            # Only gets here if everything went successfully.
            self._change_state(context, states.SUCCESS)

    @decorators.locked
    def reset(self):
        super(Flow, self).reset()
        self.results = {}
        self.result_fetcher = None
        self._accumulator.reset()
        self._left_off_at = 0

    @decorators.locked
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
