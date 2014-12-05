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

import itertools

from taskflow import exceptions as exc
from taskflow.listeners import base
from taskflow import logging
from taskflow import states
from taskflow.types import timing as tt

STARTING_STATES = frozenset((states.RUNNING, states.REVERTING))
FINISHED_STATES = frozenset((base.FINISH_STATES + (states.REVERTED,)))
WATCH_STATES = frozenset(itertools.chain(FINISHED_STATES, STARTING_STATES,
                                         [states.PENDING]))

LOG = logging.getLogger(__name__)


# TODO(harlowja): get rid of this when we can just support python 3.x and use
# its print function directly instead of having to wrap it in a helper function
# due to how python 2.x print is a language built-in and not a function...
def _printer(message):
    print(message)


class TimingListener(base.Listener):
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
        # There should be none that still exist at deregistering time, so log a
        # warning if there were any that somehow still got left behind...
        leftover_timers = len(self._timers)
        if leftover_timers:
            LOG.warn("%s task(s) did not enter %s states", leftover_timers,
                     FINISHED_STATES)
        self._timers.clear()

    def _record_ending(self, timer, task_name):
        meta_update = {
            'duration': timer.elapsed(),
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
            self._timers[task_name] = tt.StopWatch().start()
        elif state in FINISHED_STATES:
            timer = self._timers.pop(task_name, None)
            if timer is not None:
                timer.stop()
                self._record_ending(timer, task_name)


class PrintingTimingListener(TimingListener):
    """Listener that prints the start & stop timing as well as recording it."""

    def __init__(self, engine, printer=None):
        super(PrintingTimingListener, self).__init__(engine)
        if printer is None:
            self._printer = _printer
        else:
            self._printer = printer

    def _record_ending(self, timer, task_name):
        super(PrintingTimingListener, self)._record_ending(timer, task_name)
        self._printer("It took task '%s' %0.2f seconds to"
                      " finish." % (task_name, timer.elapsed()))

    def _task_receiver(self, state, details):
        super(PrintingTimingListener, self)._task_receiver(state, details)
        if state in STARTING_STATES:
            self._printer("'%s' task started." % (details['task_name']))
