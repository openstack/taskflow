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
import uuid

from taskflow import exceptions as exc
from taskflow import states


class Claimer(object):
    """A base class for objects that can attempt to claim a given claim a given
    job, so that said job can be worked on."""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def claim(self, job, owner):
        """This method will attempt to claim said job and must
        either succeed at this or throw an exception signaling the job can not
        be claimed."""
        raise NotImplementedError()


class Job(object):
    """A job is connection to some set of work to be done by some agent. Basic
    information is provided about said work to be able to attempt to 
    fullfill said work."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, name, context, catalog, claimer):
        self.name = name
        self.context = context
        self.state = states.UNCLAIMED
        self.owner = None
        self.posted_on = []
        self._catalog = catalog
        self._claimer = claimer
        self._logbook = None
        self._id = str(uuid.uuid4().hex)

    @property
    def logbook(self):
        """Fetches (or creates) a logbook entry for this job."""
        if self._logbook is None:
            if self in self._catalog:
                self._logbook = self._catalog.fetch(self)
            else:
                self._logbook = self._catalog.create(self)
        return self._logbook

    def claim(self, owner):
        """ This can be used to attempt transition this job from unclaimed
        to claimed.

        This must be done in a way that likely uses some type of locking or
        ownership transfer so that only a single entity gets this job to work
        on. This will avoid multi-job ownership, which can lead to
        inconsistent state."""
        if self.state != states.UNCLAIMED:
            raise exc.UnclaimableJobException("Unable to claim job when job is"
                                              " in state %s" % (self.state))
        self._claimer.claim(self, owner)
        self.owner = owner
        self._change_state(states.CLAIMED)
 
    def _change_state(self, new_state):
        self.state = new_state
        # TODO(harlowja): update the logbook

    def await(self, blocking=True, timeout=None):
        """Attempts to wait until the job fails or finishes."""
        raise NotImplementedError()

    def erase(self):
        """Erases any traces of this job from its associated resources."""
        for b in self.posted_on:
            b.erase(self)
        self._catalog.erase(self)
        self._logbook = None

    @property
    def tracking_id(self):
        return "j-%s-%s" % (self.name, self._id)
