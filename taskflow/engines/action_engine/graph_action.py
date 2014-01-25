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

from taskflow import states as st
from taskflow.utils import misc


_WAITING_TIMEOUT = 60  # in seconds


class FutureGraphAction(object):
    """Graph action build around futures returned by task action.

    This graph action schedules all task it can for execution and than
    waits on returned futures. If task executor is able to execute tasks
    in parallel, this enables parallel flow run and reversion.
    """

    def __init__(self, analyzer, storage, task_action):
        self._analyzer = analyzer
        self._storage = storage
        self._task_action = task_action

    def is_running(self):
        return self._storage.get_flow_state() == st.RUNNING

    def is_reverting(self):
        return self._storage.get_flow_state() == st.REVERTING

    def execute(self):
        was_suspended = self._run(
            self.is_running,
            self._task_action.schedule_execution,
            self._task_action.complete_execution,
            self._analyzer.browse_nodes_for_execute)
        return st.SUSPENDED if was_suspended else st.SUCCESS

    def revert(self):
        was_suspended = self._run(
            self.is_reverting,
            self._task_action.schedule_reversion,
            self._task_action.complete_reversion,
            self._analyzer.browse_nodes_for_revert)
        return st.SUSPENDED if was_suspended else st.REVERTED

    def _run(self, running, schedule_node, complete_node, get_next_nodes):

        def schedule(nodes, not_done):
            for node in nodes:
                future = schedule_node(node)
                if future is not None:
                    not_done.append(future)
                else:
                    schedule(get_next_nodes(node), not_done)

        failures = []
        not_done = []
        schedule(get_next_nodes(), not_done)
        was_suspended = False
        while not_done:
            # NOTE(imelnikov): if timeout occurs before any of futures
            # completes, done list will be empty and we'll just go
            # for next iteration.
            done, not_done = self._task_action.wait_for_any(
                not_done, _WAITING_TIMEOUT)

            not_done = list(not_done)
            next_nodes = []
            for future in done:
                # NOTE(harlowja): event will be used in the future for smart
                # reversion (ignoring it for now).
                node, _event, result = future.result()
                complete_node(node, result)
                if isinstance(result, misc.Failure):
                    failures.append(result)
                else:
                    next_nodes.extend(get_next_nodes(node))

            if next_nodes:
                if running() and not failures:
                    schedule(next_nodes, not_done)
                else:
                    # NOTE(imelnikov): engine stopped while there were
                    # still some tasks to do, so we either failed
                    # or were suspended.
                    was_suspended = True

        misc.Failure.reraise_if_any(failures)
        return was_suspended
