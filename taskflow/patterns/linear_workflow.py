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

from taskflow.patterns import ordered_workflow


class Workflow(ordered_workflow.Workflow):
    """A linear chain of *independent* tasks that can be applied as one unit or
       rolled back as one unit."""

    def __init__(self, name, tolerant=False, parents=None):
        super(Workflow, self).__init__(name, tolerant, parents)
        self._tasks = []

    def add(self, task):
        self._tasks.append(task)

    def order(self):
        return list(self._tasks)
