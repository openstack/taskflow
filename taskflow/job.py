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
import re
import types

from taskflow import exceptions as exc
from taskflow import states
from taskflow import utils

from taskflow.openstack.common import uuidutils

LOG = logging.getLogger(__name__)


def _get_task_version(task):
    """Gets a tasks *string* version, whether it is a task object/function."""
    task_version = utils.get_attr(task, 'version')
    if isinstance(task_version, (list, tuple)):
        task_version = utils.join(task_version, with_what=".")
    if task_version is not None and not isinstance(task_version, basestring):
        task_version = str(task_version)
    return task_version


def _get_task_name(task):
    """Gets a tasks *string* name, whether it is a task object/function."""
    task_name = ""
    if isinstance(task, (types.MethodType, types.FunctionType)):
        # If its a function look for the attributes that should have been
        # set using the task() decorator provided in the decorators file. If
        # those have not been set, then we should at least have enough basic
        # information (not a version) to form a useful task name.
        task_name = utils.get_attr(task, 'name')
        if not task_name:
            name_pieces = [a for a in utils.get_many_attr(task,
                                                          '__module__',
                                                          '__name__')
                           if a is not None]
            task_name = utils.join(name_pieces, ".")
    else:
        task_name = str(task)
    return task_name


def _is_version_compatible(version_1, version_2):
    """Checks for major version compatibility of two *string" versions."""
    if version_1 == version_2:
        # Equivalent exactly, so skip the rest.
        return True

    def _convert_to_pieces(version):
        try:
            pieces = []
            for p in version.split("."):
                p = p.strip()
                if not len(p):
                    pieces.append(0)
                    continue
                # Clean off things like 1alpha, or 2b and just select the
                # digit that starts that entry instead.
                p_match = re.match(r"(\d+)([A-Za-z]*)(.*)", p)
                if p_match:
                    p = p_match.group(1)
                pieces.append(int(p))
        except (AttributeError, TypeError, ValueError):
            pieces = []
        return pieces

    version_1_pieces = _convert_to_pieces(version_1)
    version_2_pieces = _convert_to_pieces(version_2)
    if len(version_1_pieces) == 0 or len(version_2_pieces) == 0:
        return False

    # Ensure major version compatibility to start.
    major1 = version_1_pieces[0]
    major2 = version_2_pieces[0]
    if major1 != major2:
        return False
    return True


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

    def _workflow_listener(self, state, details):
        """Ensure that when we receive an event from said workflow that we
        make sure a logbook entry exists for that flow."""
        flow = details['flow']
        if flow.name in self.logbook:
            return
        self.logbook.add_flow(flow.name)

    def _task_listener(self, state, details):
        """Store the result of the task under the given flow in the log
        book so that it can be retrieved later."""
        flow = details['flow']
        metadata = {}
        flow_details = self.logbook[flow.name]
        if state in (states.SUCCESS, states.FAILURE):
            metadata['result'] = details['result']

        task = details['task']
        name = _get_task_name(task)
        if name not in flow_details:
            metadata['states'] = [state]
            metadata['version'] = _get_task_version(task)
            flow_details.add_task(name, metadata)
        else:
            details = flow_details[name]

            # Warn about task versions possibly being incompatible
            my_version = _get_task_version(task)
            prev_version = details.metadata.get('version')
            if not _is_version_compatible(my_version, prev_version):
                LOG.warn("Updating a task with a different version than the"
                         " one being listened to (%s != %s)",
                         prev_version, my_version)

            past_states = details.metadata.get('states', [])
            past_states.append(state)
            details.metadata['states'] = past_states
            details.metadata.update(metadata)

    def _task_result_fetcher(self, _context, flow, task, task_uuid):
        flow_details = self.logbook[flow.name]

        # See if it completed before (or failed before) so that we can use its
        # results instead of having to recompute it.
        not_found = (False, False, None)
        name = _get_task_name(task)
        if name not in flow_details:
            return not_found

        details = flow_details[name]
        has_completed = False
        was_failure = False
        task_states = details.metadata.get('states', [])
        for state in task_states:
            if state in (states.SUCCESS, states.FAILURE):
                if state == states.FAILURE:
                    was_failure = True
                has_completed = True
                break

        # Warn about task versions possibly being incompatible
        my_version = _get_task_version(task)
        prev_version = details.metadata.get('version')
        if not _is_version_compatible(my_version, prev_version):
            LOG.warn("Fetching task results from a task with a different"
                     " version from the one being requested (%s != %s)",
                     prev_version, my_version)

        if has_completed:
            return (True, was_failure, details.metadata.get('result'))

        return not_found

    def associate(self, flow, parents=True):
        """Attachs the needed resumption and state change tracking listeners
        to the given workflow so that the workflow can be resumed/tracked
        using the jobs components."""
        flow.task_notifier.register('*', self._task_listener)
        flow.notifier.register('*', self._workflow_listener)
        flow.result_fetcher = self._task_result_fetcher
        if parents and flow.parents:
            for p in flow.parents:
                self.associate(p, parents)

    def disassociate(self, flow, parents=True):
        """Detaches the needed resumption and state change tracking listeners
        from the given workflow."""
        flow.notifier.deregister('*', self._workflow_listener)
        flow.task_notifier.deregister('*', self._task_listener)
        flow.result_fetcher = None
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
