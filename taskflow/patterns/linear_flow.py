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

from taskflow import exceptions as exc
from taskflow.patterns import ordered_flow


class Flow(ordered_flow.Flow):
    """A linear chain of tasks that can be applied as one unit or
       rolled back as one unit. Each task in the chain may have requirements
       which are satisfied by the previous task/s in the chain."""

    def __init__(self, name, parents=None):
        super(Flow, self).__init__(name, parents)
        self._tasks = []

    def _fetch_task_inputs(self, task):
        inputs = {}
        for r in task.requires:
            # Find the last task that provided this.
            for (last_task, last_results) in reversed(self.results):
                if r not in last_task.provides:
                    continue
                if last_results and r in last_results:
                    inputs[r] = last_results[r]
                else:
                    inputs[r] = None
                # Some task said they had it, get the next requirement.
                break
        return inputs

    def _validate_provides(self, task):
        # Ensure that some previous task provides this input.
        missing_requires = []
        for r in task.requires:
            found_provider = False
            for prev_task in reversed(self._tasks):
                if r in prev_task.provides:
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

    def add(self, task):
        self._validate_provides(task)
        self._tasks.append(task)

    def order(self):
        return list(self._tasks)
