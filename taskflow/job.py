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
import types

from taskflow import exceptions as exc
from taskflow import states
from taskflow import utils

from taskflow.openstack.common import uuidutils


def task_and_state(task, state):
    """Combines a task objects string representation with a state to
    create a uniquely identifying task+state name."""

    task_name = ""
    if isinstance(task, types.FunctionType):
        # If its a function look for the attributes that should have been
        # set using the task() decorator provided in the decorators file. If
        # those have not been set, then we should at least have enough basic
        # information (not a version) to form a useful task name.
        if hasattr(task, 'name'):
            task_name = str(task.name)
        else:
            name_pieces = [a for a in utils.get_many_attr(task,
                                                          '__module__',
                                                          '__name__')
                           if a is not None]
            task_name = utils.join(name_pieces, ".")
        task_version = getattr(task, '__version__', None)
        if isinstance(task_version, (list, tuple)):
            task_version = utils.join(task_version, with_what=".")
        if task_version is not None:
            task_name += "==%s" % (task_version)
    else:
        task_name = str(task)
    return "%s;%s" % (task_name, state)


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

    def __init__(self, name, context, catalog, claimer, jid=None):
        self.name = name
        self.context = context
        self.owner = None
        self.posted_on = []
        self._catalog = catalog
        self._claimer = claimer
        self._logbook = None
        if not jid:
            self._id = uuidutils.generate_uuid()
        else:
            self._id = str(jid)
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

    def _workflow_listener(self, _context, flow, _old_state):
        """Ensure that when we receive an event from said workflow that we
        make sure a logbook entry exists for that flow."""
        if flow.name in self.logbook:
            return
        self.logbook.add_flow(flow.name)

    def _task_listener(self, _context, state, flow, task, result=None):
        """Store the result of the task under the given flow in the log
        book so that it can be retrieved later."""
        metadata = None
        flow_details = self.logbook[flow.name]
        if state in (states.SUCCESS, states.FAILURE):
            metadata = {
                'result': result,
            }
        task_state = task_and_state(task, state)
        if task_state not in flow_details:
            flow_details.add_task(task_state, metadata)

    def _task_result_fetcher(self, _context, flow, task):
        flow_details = self.logbook[flow.name]
        # See if it completed before (or failed before) so that we can use its
        # results instead of having to recompute it.
        for s in (states.SUCCESS, states.FAILURE):
            name = task_and_state(task, s)
            if name in flow_details:
                # TODO(harlowja): should we be a little more cautious about
                # duplicate task results? Maybe we shouldn't allow them to
                # have the same name in the first place?
                details = flow_details[name][0]
                if details.metadata and 'result' in details.metadata:
                    return (True, s == states.FAILURE,
                            details.metadata['result'])
        return (False, False, None)

    def associate(self, flow, parents=True):
        """Attachs the needed resumption and state change tracking listeners
        to the given workflow so that the workflow can be resumed/tracked
        using the jobs components."""

        if self._task_listener not in flow.task_listeners:
            flow.task_listeners.append(self._task_listener)
        if self._workflow_listener not in flow.listeners:
            flow.listeners.append(self._workflow_listener)
        flow.result_fetcher = self._task_result_fetcher

        # Associate the parents as well (if desired)
        if parents and flow.parents:
            for p in flow.parents:
                self.associate(p, parents)

    def disassociate(self, flow, parents=True):
        """Detaches the needed resumption and state change tracking listeners
        from the given workflow."""

        if self._task_listener in flow.task_listeners:
            flow.task_listeners.remove(self._task_listener)
        if self._workflow_listener in flow.listeners:
            flow.listeners.remove(self._workflow_listener)
        if flow.result_fetcher is self._task_result_fetcher:
            flow.result_fetcher = None

        # Disassociate from the flows parents (if desired)
        if parents and flow.parents:
            for p in flow.parents:
                self.disassociate(p, parents)

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
        already_associated = []

        def associate_all(a_flow):
            if a_flow in already_associated:
                return
            # Associate with the flow.
            self.associate(a_flow)
            already_associated.append(a_flow)
            # Ensure we are associated with all the flows parents.
            if a_flow.parents:
                for p in a_flow.parents:
                    associate_all(p)

        if flow.state != states.PENDING:
            raise exc.InvalidStateException("Unable to run %s when in"
                                            " state %s" % (flow, flow.state))

        associate_all(flow)
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
    def tracking_id(self):
        """Returns a tracking *unique* identifier that can be used to identify
        this job among other jobs."""
        return "j-%s-%s" % (self.name, self._id)
