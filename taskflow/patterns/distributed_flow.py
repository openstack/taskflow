# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
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

import logging

from taskflow import logbook

from celery import chord

LOG = logging.getLogger(__name__)


class Flow(object):
    """A linear chain of independent tasks that can be applied as one unit or
       rolled back as one unit."""

    def __init__(self, name, parents=None):
        self.name = name
        self.root = None
        self._tasks = []
        logbook.add_workflow(name)


    def chain_listeners(self, context, initial_task, callback_task):
        """ Register one listener for a task """
        if self.root is None:
            initial_task.name = '%s.%s' % (self.name, initial_task.name)
            self.root = initial_task.s(context)
            self._tasks.append(initial_task)
            LOG.info('WF %s root task set to %s' % (self.name, initial_task.name))

        callback_task.name = '%s.%s' % (self.name, callback_task.name)
        self._tasks.append(callback_task)

        initial_task.link(callback_task.s(context))

    def split_listeners(self, context, initial_task, callback_tasks):
        """ Register multiple listeners for one task """
        if self.root is None:
            initial_task.name = '%s.%s' % (self.name, initial_task.name)
            self.root = initial_task.s(context)
            self._tasks.append(initial_task)
            LOG.info('WF %s root task set to %s' % (self.name, initial_task.name))
        for task in callback_tasks:
            task.name = '%s.%s' % (self.name, task.name)
            self._tasks.append(task)
            initial_task.link(task.s(context))

    def merge_listeners(self, context, inital_tasks, callback_task):
        """ Register one listener for multiple tasks """
        header = []
        if self.root is None:
            self.root = []
        for task in initial_tasks:
            task.name = '%s.%s' % (self.name, task.name)
            self._tasks.append(task)
            header.append(task.s(context))
            if isinstance(self.root, list):
                self.root.append(task.s(context))
                LOG.info('WF %s added root task %s' %
                         (self.name, task.name))
        callback_task.name = '%s.%s' % (self.name, callback_task.name)
        self._tasks.append(callback_task)

        #TODO: Need to set up chord so that it's not executed immediately
        c = chord(header, body=callback_task)

    def run(self, context, *args, **kwargs):
        """ Start root task and kick off workflow """
        self.root(context)
        LOG.info('WF %s has been started' % (self.name,))
