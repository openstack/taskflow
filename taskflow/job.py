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

import abc
import logging

from taskflow import exceptions as exc
from taskflow import states
from taskflow import utils

from taskflow.openstack.common import uuidutils

LOG = logging.getLogger(__name__)


class Claimer(object):
    """A base class for objects that can attempt to claim a given
    job, so that said job can be worked on."""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def claim(self, job, owner):
        """This method will attempt to claim said job and must
        either succeed at this or throw an exception signaling the job can not
        be claimed."""
        raise NotImplementedError()

    @abc.abstractmethod
    def unclaim(self, job, owner):
        """This method will attempt to unclaim said job and must
        either succeed at this or throw an exception signaling the job can not
        be unclaimed."""
        raise NotImplementedError()


class Job(object):
    """A job is connection to some set of work to be done by some agent. Basic
    information is provided about said work to be able to attempt to
    fullfill said work."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, name, context, catalog, claimer, uuid=None):
        self.name = name
        self.context = context
        self.owner = None
        self.posted_on = []
        self._catalog = catalog
        self._claimer = claimer
        self._logbook = None
        if not uuid:
            self._id = uuidutils.generate_uuid()
        else:
            self._id = str(uuid)
        self._state = states.UNCLAIMED

    def __str__(self):
        lines = ['Job: %s' % (self.name)]
        lines.append("%s" % (self.uuid))
        lines.append("%s" % (self.state))
        return "; ".join(lines)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state):
        self._change_state(new_state)

    def _change_state(self, new_state):
        if self.state != new_state:
            self._state = new_state
            # TODO(harlowja): add logbook info?

    @property
    def logbook(self):
        """Fetches (or creates) a logbook entry for this job."""
        if self._logbook is None:
            self._logbook = self._catalog.create_or_fetch(self)
        return self._logbook

    def claim(self, owner):
        """This can be used to attempt transition this job from unclaimed
        to claimed.

        This must be done in a way that likely uses some type of locking or
        ownership transfer so that only a single entity gets this job to work
        on. This will avoid multi-job ownership, which can lead to
        inconsistent state."""
        if self.state != states.UNCLAIMED:
            raise exc.UnclaimableJobException("Unable to claim job when job is"
                                              " in state %s" % (self.state))
        self._claimer.claim(self, owner)
        self._change_state(states.CLAIMED)

    def run(self, flow, *args, **kwargs):
        if flow.state != states.PENDING:
            raise exc.InvalidStateException("Unable to run %s when in"
                                            " state %s" % (flow, flow.state))
        return flow.run(self.context, *args, **kwargs)

    def unclaim(self):
        """Atomically transitions this job from claimed to unclaimed."""
        if self.state == states.UNCLAIMED:
            return
        self._claimer.unclaim(self, self.owner)
        self._change_state(states.UNCLAIMED)

    def erase(self):
        """Erases any traces of this job from its associated resources."""
        for b in self.posted_on:
            b.erase(self)
        self._catalog.erase(self)
        if self._logbook is not None:
            self._logbook.close()
            self._logbook = None
        if self.state != states.UNCLAIMED:
            self._claimer.unclaim(self, self.owner)

    def await(self, timeout=None):
        """Awaits until either the job fails or succeeds or the provided
        timeout is reached."""

        def check_functor():
            if self.state not in (states.FAILURE, states.SUCCESS):
                return False
            return True

        return utils.await(check_functor, timeout)

    @property
    def uuid(self):
        """Returns a tracking *unique* identifier that can be used to identify
        this job among other jobs."""
        return "j-%s" % (self._id)
