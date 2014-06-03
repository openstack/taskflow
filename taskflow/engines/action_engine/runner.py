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

from taskflow import states as st
from taskflow.utils import misc


_WAITING_TIMEOUT = 60  # in seconds


class Runner(object):
    """Runner that iterates while executing nodes using the given runtime.

    This runner acts as the action engine run loop, it resumes the workflow,
    schedules all task it can for execution using the runtimes scheduler and
    analyzer components, and than waits on returned futures and then activates
    the runtimes completion component to finish up those tasks.

    This process repeats until the analzyer runs out of next nodes, when the
    scheduler can no longer schedule tasks or when the the engine has been
    suspended or a task has failed and that failure could not be resolved.

    NOTE(harlowja): If the runtimes scheduler component is able to schedule
    tasks in parallel, this enables parallel running and/or reversion.
    """

    # Informational states this action yields while running, not useful to
    # have the engine record but useful to provide to end-users when doing
    # execution iterations.
    ignorable_states = (st.SCHEDULING, st.WAITING, st.RESUMING, st.ANALYZING)

    def __init__(self, runtime, waiter):
        self._scheduler = runtime.scheduler
        self._completer = runtime.completer
        self._storage = runtime.storage
        self._analyzer = runtime.analyzer
        self._waiter = waiter

    def is_running(self):
        return self._storage.get_flow_state() == st.RUNNING

    def run_iter(self, timeout=None):
        """Runs the nodes using the runtime components.

        NOTE(harlowja): the states that this generator will go through are:

        RESUMING -> SCHEDULING
        SCHEDULING -> WAITING
        WAITING -> ANALYZING
        ANALYZING -> SCHEDULING

        Between any of these yielded states if the engine has been suspended
        or the engine has failed (due to a non-resolveable task failure or
        scheduling failure) the engine will stop executing new tasks (currently
        running tasks will be allowed to complete) and this iteration loop
        will be broken.
        """
        if timeout is None:
            timeout = _WAITING_TIMEOUT

        # Prepare flow to be resumed
        yield st.RESUMING
        next_nodes = self._completer.resume()
        next_nodes.update(self._analyzer.get_next_nodes())

        # Schedule nodes to be worked on
        yield st.SCHEDULING
        if self.is_running():
            not_done, failures = self._scheduler.schedule(next_nodes)
        else:
            not_done, failures = (set(), [])

        # Run!
        #
        # At this point we need to ensure we wait for all active nodes to
        # finish running (even if we are asked to suspend) since we can not
        # preempt those tasks (maybe in the future we will be better able to do
        # this).
        while not_done:
            yield st.WAITING

            # TODO(harlowja): maybe we should start doing 'yield from' this
            # call sometime in the future, or equivalent that will work in
            # py2 and py3.
            done, not_done = self._waiter.wait_for_any(not_done, timeout)

            # Analyze the results and schedule more nodes (unless we had
            # failures). If failures occurred just continue processing what
            # is running (so that we don't leave it abandoned) but do not
            # schedule anything new.
            yield st.ANALYZING
            next_nodes = set()
            for future in done:
                try:
                    node, event, result = future.result()
                    retain = self._completer.complete(node, event, result)
                    if retain and isinstance(result, misc.Failure):
                        failures.append(result)
                except Exception:
                    failures.append(misc.Failure())
                else:
                    try:
                        more_nodes = self._analyzer.get_next_nodes(node)
                    except Exception:
                        failures.append(misc.Failure())
                    else:
                        next_nodes.update(more_nodes)
            if next_nodes and not failures and self.is_running():
                yield st.SCHEDULING
                # Recheck incase someone suspended it.
                if self.is_running():
                    more_not_done, failures = self._scheduler.schedule(
                        next_nodes)
                    not_done.update(more_not_done)

        if failures:
            misc.Failure.reraise_if_any(failures)
        if self._analyzer.get_next_nodes():
            yield st.SUSPENDED
        elif self._analyzer.is_success():
            yield st.SUCCESS
        else:
            yield st.REVERTED
