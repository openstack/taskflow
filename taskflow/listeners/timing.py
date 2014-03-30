# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

from __future__ import absolute_import

import logging

from taskflow import exceptions as exc
from taskflow.listeners import base
from taskflow import states
from taskflow.utils import misc

STARTING_STATES = (states.RUNNING, states.REVERTING)
FINISHED_STATES = base.FINISH_STATES + (states.REVERTED,)
WATCH_STATES = frozenset(FINISHED_STATES + STARTING_STATES +
                         (states.PENDING,))

LOG = logging.getLogger(__name__)


class TimingListener(base.ListenerBase):
    """Listener that captures task duration.

    It records how long a task took to execute (or fail)
    to storage. It saves the duration in seconds as float value
    to task metadata with key ``'duration'``.
    """
    def __init__(self, engine):
        super(TimingListener, self).__init__(engine,
                                             task_listen_for=WATCH_STATES,
                                             flow_listen_for=[])
        self._timers = {}

    def deregister(self):
        super(TimingListener, self).deregister()
        self._timers.clear()

    def _record_ending(self, timer, task_name):
        meta_update = {
            'duration': float(timer.elapsed()),
        }
        try:
            # Don't let storage failures throw exceptions in a listener method.
            self._engine.storage.update_atom_metadata(task_name, meta_update)
        except exc.StorageFailure:
            LOG.warn("Failure to store duration update %s for task %s",
                     meta_update, task_name, exc_info=True)

    def _task_receiver(self, state, details):
        task_name = details['task_name']
        if state == states.PENDING:
            self._timers.pop(task_name, None)
        elif state in STARTING_STATES:
            self._timers[task_name] = misc.StopWatch()
            self._timers[task_name].start()
        elif state in FINISHED_STATES:
            if task_name in self._timers:
                self._record_ending(self._timers[task_name], task_name)
