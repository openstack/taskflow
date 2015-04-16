# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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

from taskflow import exceptions as excp
from taskflow import states
from taskflow import test


class TestStates(test.TestCase):
    def test_valid_flow_states(self):
        for start_state, end_state in states._ALLOWED_FLOW_TRANSITIONS:
            self.assertTrue(states.check_flow_transition(start_state,
                                                         end_state))

    def test_ignored_flow_states(self):
        for start_state, end_state in states._IGNORED_FLOW_TRANSITIONS:
            self.assertFalse(states.check_flow_transition(start_state,
                                                          end_state))

    def test_invalid_flow_states(self):
        invalids = [
            # Not a comprhensive set/listing...
            (states.RUNNING, states.PENDING),
            (states.REVERTED, states.RUNNING),
            (states.RESUMING, states.RUNNING),
        ]
        for start_state, end_state in invalids:
            self.assertRaises(excp.InvalidState,
                              states.check_flow_transition,
                              start_state, end_state)

    def test_valid_job_states(self):
        for start_state, end_state in states._ALLOWED_JOB_TRANSITIONS:
            self.assertTrue(states.check_job_transition(start_state,
                                                        end_state))

    def test_ignored_job_states(self):
        ignored = []
        for start_state, end_state in states._ALLOWED_JOB_TRANSITIONS:
            ignored.append((start_state, start_state))
            ignored.append((end_state, end_state))
        for start_state, end_state in ignored:
            self.assertFalse(states.check_job_transition(start_state,
                                                         end_state))

    def test_invalid_job_states(self):
        invalids = [
            (states.COMPLETE, states.UNCLAIMED),
            (states.UNCLAIMED, states.COMPLETE),
        ]
        for start_state, end_state in invalids:
            self.assertRaises(excp.InvalidState,
                              states.check_job_transition,
                              start_state, end_state)

    def test_valid_task_states(self):
        for start_state, end_state in states._ALLOWED_TASK_TRANSITIONS:
            self.assertTrue(states.check_task_transition(start_state,
                                                         end_state))

    def test_invalid_task_states(self):
        invalids = [
            # Not a comprhensive set/listing...
            (states.RUNNING, states.PENDING),
            (states.PENDING, states.REVERTED),
            (states.PENDING, states.SUCCESS),
            (states.PENDING, states.FAILURE),
            (states.RETRYING, states.PENDING),
        ]
        for start_state, end_state in invalids:
            # TODO(harlowja): fix this so that it raises instead of
            # returning false...
            self.assertFalse(
                states.check_task_transition(start_state, end_state))
