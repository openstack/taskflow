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
import six
import time

from oslo_utils import timeutils

from taskflow.engines.action_engine import compiler as co
from taskflow import exceptions as exc
from taskflow.listeners import base
from taskflow import logging
from taskflow import states

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


class DurationListener(base.Listener):
    """Listener that captures task duration.

    It records how long a task took to execute (or fail)
    to storage. It saves the duration in seconds as float value
    to task metadata with key ``'duration'``.
    """
    def __init__(self, engine):
        super(DurationListener, self).__init__(engine,
                                               task_listen_for=WATCH_STATES,
                                               flow_listen_for=WATCH_STATES)
        self._timers = {co.TASK: {}, co.FLOW: {}}

    def deregister(self):
        super(DurationListener, self).deregister()
        # There should be none that still exist at deregistering time, so log a
        # warning if there were any that somehow still got left behind...
        for item_type, timers in six.iteritems(self._timers):
            leftover_timers = len(timers)
            if leftover_timers:
                LOG.warning("%s %s(s) did not enter %s states",
                            leftover_timers,
                            item_type, FINISHED_STATES)
            timers.clear()

    def _record_ending(self, timer, item_type, item_name, state):
        meta_update = {
            'duration': timer.elapsed(),
        }

        try:
            storage = self._engine.storage
            # Don't let storage failures throw exceptions in a listener method.
            if item_type == co.FLOW:
                storage.update_flow_metadata(meta_update)
            else:
                storage.update_atom_metadata(item_name, meta_update)
        except exc.StorageFailure:
            LOG.warning("Failure to store duration update %s for %s %s",
                        meta_update, item_type, item_name, exc_info=True)

    def _task_receiver(self, state, details):
        task_name = details['task_name']
        self._receiver(co.TASK, task_name, state)

    def _flow_receiver(self, state, details):
        flow_name = details['flow_name']
        self._receiver(co.FLOW, flow_name, state)

    def _receiver(self, item_type, item_name, state):
        if state == states.PENDING:
            self._timers[item_type].pop(item_name, None)
        elif state in STARTING_STATES:
            self._timers[item_type][item_name] = timeutils.StopWatch().start()
        elif state in FINISHED_STATES:
            timer = self._timers[item_type].pop(item_name, None)
            if timer is not None:
                timer.stop()
                self._record_ending(timer, item_type, item_name, state)


class PrintingDurationListener(DurationListener):
    """Listener that prints the duration as well as recording it."""

    def __init__(self, engine, printer=None):
        super(PrintingDurationListener, self).__init__(engine)
        if printer is None:
            self._printer = _printer
        else:
            self._printer = printer

    def _record_ending(self, timer, item_type, item_name, state):
        super(PrintingDurationListener, self)._record_ending(
            timer, item_type, item_name, state)
        self._printer("It took %s '%s' %0.2f seconds to"
                      " finish." % (item_type, item_name, timer.elapsed()))

    def _receiver(self, item_type, item_name, state):
        super(PrintingDurationListener, self)._receiver(item_type,
                                                        item_name, state)
        if state in STARTING_STATES:
            self._printer("'%s' %s started." % (item_name, item_type))


class EventTimeListener(base.Listener):
    """Listener that captures task, flow, and retry event timestamps.

    It records how when an event is received (using unix time) to
    storage. It saves the timestamps under keys (in atom or flow details
    metadata) of the format ``{event}-timestamp`` where ``event`` is the
    state/event name that has been received.

    This information can be later extracted/examined to derive durations...
    """

    def __init__(self, engine,
                 task_listen_for=base.DEFAULT_LISTEN_FOR,
                 flow_listen_for=base.DEFAULT_LISTEN_FOR,
                 retry_listen_for=base.DEFAULT_LISTEN_FOR):
        super(EventTimeListener, self).__init__(
            engine, task_listen_for=task_listen_for,
            flow_listen_for=flow_listen_for, retry_listen_for=retry_listen_for)

    def _record_atom_event(self, state, atom_name):
        meta_update = {'%s-timestamp' % state: time.time()}
        try:
            # Don't let storage failures throw exceptions in a listener method.
            self._engine.storage.update_atom_metadata(atom_name, meta_update)
        except exc.StorageFailure:
            LOG.warning("Failure to store timestamp %s for atom %s",
                        meta_update, atom_name, exc_info=True)

    def _flow_receiver(self, state, details):
        meta_update = {'%s-timestamp' % state: time.time()}
        try:
            # Don't let storage failures throw exceptions in a listener method.
            self._engine.storage.update_flow_metadata(meta_update)
        except exc.StorageFailure:
            LOG.warning("Failure to store timestamp %s for flow %s",
                        meta_update, details['flow_name'], exc_info=True)

    def _task_receiver(self, state, details):
        self._record_atom_event(state, details['task_name'])

    def _retry_receiver(self, state, details):
        self._record_atom_event(state, details['retry_name'])
