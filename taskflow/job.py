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
import time
import uuid

from taskflow import exceptions as exc
from taskflow import logbook
from taskflow import states
from taskflow import utils


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

    def __init__(self, name, context, catalog, claimer):
        self.name = name
        self.context = context
        self.owner = None
        self.posted_on = []
        self._catalog = catalog
        self._claimer = claimer
        self._logbook = None
        self._id = str(uuid.uuid4().hex)
        self._state = states.UNCLAIMED

    def __str__(self):
        return "Job (%s, %s): %s" % (self.name, self.tracking_id, self.state)

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

    def associate(self, wf, task_state_name_functor=None):
        """Attachs the needed resumption and state change tracking listeners
        to the given workflow so that the workflow can be resumed/tracked
        using the jobs components."""

        # TODO(harlowja): should this be in the job class or the workflow class
        # or neither, still not quite sure...

        def generate_task_name(task, state):
            return "%s:%s" % (task.name, state)

        if not task_state_name_functor:
            task_state_name_functor = generate_task_name

        def wf_state_change_listener(context, wf, old_state):
            if wf.name in self.logbook:
                return
            self.logbook.add_flow(wf.name)

        def task_result_fetcher(context, wf, task):
            wf_details = self.logbook[wf.name]
            # See if it completed before so that we can use its results instead
            # of having to recompute them.
            td_name = task_state_name_functor(task, states.SUCCESS)
            if td_name in wf_details:
                # TODO(harlowja): should we be a little more cautious about
                # duplicate task results? Maybe we shouldn't allow them to
                # have the same name in the first place?
                task_details = wf_details[td_name][0]
                if task_details.metadata and 'result' in task_details.metadata:
                    return (True, task_details.metadata['result'])
            return (False, None)

        def task_state_change_listener(context, state, wf, task, result=None):
                metadata = None
                wf_details = self.logbook[wf.name]
                if state == states.SUCCESS:
                    metadata = {
                        'result': result,
                    }
                td_name = task_state_name_functor(task, state)
                if td_name not in wf_details:
                    td_details = wf_details.add_task(td_name)
                    td_details.metadata = metadata

        wf.task_listeners.append(task_state_change_listener)
        wf.listeners.append(wf_state_change_listener)
        wf.result_fetcher = task_result_fetcher

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
    def tracking_id(self):
        return "j-%s-%s" % (self.name, self._id)
