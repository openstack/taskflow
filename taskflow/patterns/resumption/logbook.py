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

import logging

from taskflow import states
from taskflow import utils

LOG = logging.getLogger(__name__)


class Resumption(object):
    # NOTE(harlowja): This allows for resumption by skipping tasks which
    # have already occurred, aka fast-forwarding through a workflow to
    # the last point it stopped (if possible).
    def __init__(self, logbook):
        self._logbook = logbook

    def record_for(self, flow):

        def _task_listener(state, details):
            """Store the result of the task under the given flow in the log
            book so that it can be retrieved later.
            """
            runner = details['runner']
            flow = details['flow']
            LOG.debug("Recording %s of %s has finished state %s",
                      runner, flow, state)
            metadata = {}
            flow_details = self._logbook[flow.uuid]
            if state in (states.SUCCESS, states.FAILURE):
                metadata['result'] = runner.result
            if runner.uuid not in flow_details:
                metadata['states'] = [state]
                metadata['version'] = runner.version
                flow_details.add_task(runner.uuid, metadata)
            else:
                details = flow_details[runner.uuid]
                immediate_version = runner.version
                recorded_version = details.metadata.get('version')
                if recorded_version is not None:
                    if not utils.is_version_compatible(recorded_version,
                                                       immediate_version):
                        LOG.warn("Updating a task with a different version"
                                 " than the one being listened to (%s != %s)",
                                 recorded_version, immediate_version)
                past_states = details.metadata.get('states', [])
                if state not in past_states:
                    past_states.append(state)
                    details.metadata['states'] = past_states
                if metadata:
                    details.metadata.update(metadata)

        def _workflow_listener(state, details):
            """Ensure that when we receive an event from said workflow that we
            make sure a logbook entry exists for that flow.
            """
            flow = details['flow']
            old_state = details['old_state']
            LOG.debug("%s has transitioned from %s to %s", flow, old_state,
                      state)
            if flow.uuid in self._logbook:
                return
            self._logbook.add_flow(flow.uuid)

        flow.task_notifier.register('*', _task_listener)
        flow.notifier.register('*', _workflow_listener)

    def _reconcile_versions(self, desired_version, task_details):
        # For now don't do anything to reconcile the desired version
        # from the actual version present in the task details, but in the
        # future we could try to alter the task details to be in the older
        # format (or more complicated logic...)
        return task_details

    def _get_details(self, flow_details, runner):
        if runner.uuid not in flow_details:
            return (False, None)
        details = flow_details[runner.uuid]
        has_completed = False
        for state in details.metadata.get('states', []):
            if state in (states.SUCCESS, states.FAILURE):
                has_completed = True
                break
        if not has_completed:
            return (False, None)
        immediate_version = runner.version
        recorded_version = details.metadata.get('version')
        if recorded_version is not None:
            if not utils.is_version_compatible(recorded_version,
                                               immediate_version):
                LOG.warn("Fetching runner metadata from a task with"
                         " a different version from the one being"
                         " processed (%s != %s)", recorded_version,
                         immediate_version)
                details = self._reconcile_versions(immediate_version, details)
        return (True, details)

    def resume(self, flow, ordering):
        """Splits the initial ordering into two segments, the first which
        has already completed (or errored) and the second which has not
        completed or errored.
        """

        flow_id = flow.uuid
        if flow_id not in self._logbook:
            LOG.debug("No record of %s", flow)
            return ([], ordering)
        flow_details = self._logbook[flow_id]
        ran_already = []
        for r in ordering:
            LOG.debug("Checking if ran %s of %s", r, flow)
            (has_ran, details) = self._get_details(flow_details, r)
            LOG.debug(has_ran)
            if not has_ran:
                # We need to put back the last task we took out since it did
                # not run and therefore needs to, thats why we have this
                # different iterator (which can do this).
                return (ran_already, utils.LastFedIter(r, ordering))
            LOG.debug("Already ran %s", r)
            ran_already.append((r, details.metadata))
        return (ran_already, iter([]))
