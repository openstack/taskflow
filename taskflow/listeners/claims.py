# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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
import os

import six

from taskflow import exceptions
from taskflow.listeners import base
from taskflow import states

LOG = logging.getLogger(__name__)


class CheckingClaimListener(base.Listener):
    """Listener that interacts [engine, job, jobboard]; ensures claim is valid.

    This listener (or a derivative) can be associated with an engines
    notification system after the job has been claimed (so that the jobs work
    can be worked on by that engine). This listener (after associated) will
    check that the job is still claimed *whenever* the engine notifies of a
    task or flow state change. If the job is not claimed when a state change
    occurs, a associated handler (or the default) will be activated to
    determine how to react to this *hopefully* exceptional case.

    NOTE(harlowja): this may create more traffic than desired to the
    jobboard backend (zookeeper or other), since the amount of state change
    per task and flow is non-zero (and checking during each state change will
    result in quite a few calls to that management system to check the jobs
    claim status); this could be later optimized to check less (or only check
    on a smaller set of states)

    NOTE(harlowja): if a custom ``on_job_loss`` callback is provided it must
    accept three positional arguments, the first being the current engine being
    ran, the second being the 'task/flow' state and the third being the details
    that were sent from the engine to listeners for inspection.
    """

    def __init__(self, engine, job, board, owner, on_job_loss=None):
        super(CheckingClaimListener, self).__init__(engine)
        self._job = job
        self._board = board
        self._owner = owner
        if on_job_loss is None:
            self._on_job_loss = self._suspend_engine_on_loss
        else:
            if not six.callable(on_job_loss):
                raise ValueError("Custom 'on_job_loss' handler must be"
                                 " callable")
            self._on_job_loss = on_job_loss

    def _suspend_engine_on_loss(self, engine, state, details):
        """The default strategy for handling claims being lost."""
        try:
            engine.suspend()
        except exceptions.TaskFlowException as e:
            LOG.warn("Failed suspending engine '%s', (previously owned by"
                     " '%s'):%s%s", engine, self._owner, os.linesep,
                     e.pformat())

    def _flow_receiver(self, state, details):
        self._claim_checker(state, details)

    def _task_receiver(self, state, details):
        self._claim_checker(state, details)

    def _has_been_lost(self):
        try:
            job_state = self._job.state
            job_owner = self._board.find_owner(self._job)
        except (exceptions.NotFound, exceptions.JobFailure):
            return True
        else:
            if job_state == states.UNCLAIMED or self._owner != job_owner:
                return True
            else:
                return False

    def _claim_checker(self, state, details):
        if not self._has_been_lost():
            LOG.debug("Job '%s' is still claimed (actively owned by '%s')",
                      self._job, self._owner)
        else:
            LOG.warn("Job '%s' has lost its claim (previously owned by '%s')",
                     self._job, self._owner)
            self._on_job_loss(self._engine, state, details)
