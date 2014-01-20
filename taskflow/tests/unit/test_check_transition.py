# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

from taskflow import exceptions as exc
from taskflow import states
from taskflow import test


class TransitionTest(test.TestCase):

    def assertTransitionAllowed(self, from_state, to_state):
        self.assertTrue(self.check_transition(from_state, to_state))

    def assertTransitionIgnored(self, from_state, to_state):
        self.assertFalse(self.check_transition(from_state, to_state))

    def assertTransitionForbidden(self, from_state, to_state):
        self.assertRaisesRegexp(exc.InvalidState, self.transition_exc_regexp,
                                self.check_transition, from_state, to_state)

    def assertTransitions(self, from_state, allowed=None, ignored=None,
                          forbidden=None):
        for a in allowed or []:
            self.assertTransitionAllowed(from_state, a)
        for i in ignored or []:
            self.assertTransitionIgnored(from_state, i)
        for f in forbidden or []:
            self.assertTransitionForbidden(from_state, f)


class CheckFlowTransitionTest(TransitionTest):

    def setUp(self):
        super(CheckFlowTransitionTest, self).setUp()
        self.check_transition = states.check_flow_transition
        self.transition_exc_regexp = '^Flow transition.*not allowed'

    def test_to_same_state(self):
        self.assertTransitionIgnored(states.SUCCESS, states.SUCCESS)

    def test_rerunning_allowed(self):
        self.assertTransitionAllowed(states.SUCCESS, states.RUNNING)

    def test_no_resuming_from_pending(self):
        self.assertTransitionIgnored(states.PENDING, states.RESUMING)

    def test_resuming_from_running(self):
        self.assertTransitionAllowed(states.RUNNING, states.RESUMING)

    def test_bad_transition_raises(self):
        self.assertTransitionForbidden(states.FAILURE, states.SUCCESS)


class CheckTaskTransitionTest(TransitionTest):

    def setUp(self):
        super(CheckTaskTransitionTest, self).setUp()
        self.check_transition = states.check_task_transition
        self.transition_exc_regexp = '^Task transition.*not allowed'

    def test_from_pending_state(self):
        self.assertTransitions(from_state=states.PENDING,
                               allowed=(states.RUNNING,),
                               ignored=(states.PENDING, states.REVERTING),
                               forbidden=(states.SUCCESS, states.FAILURE,
                                          states.REVERTED))

    def test_from_running_state(self):
        self.assertTransitions(from_state=states.RUNNING,
                               allowed=(states.RUNNING, states.SUCCESS,
                                        states.FAILURE, states.REVERTING),
                               forbidden=(states.PENDING, states.REVERTED))

    def test_from_success_state(self):
        self.assertTransitions(from_state=states.SUCCESS,
                               allowed=(states.REVERTING,),
                               ignored=(states.RUNNING, states.SUCCESS),
                               forbidden=(states.PENDING, states.FAILURE,
                                          states.REVERTED))

    def test_from_failure_state(self):
        self.assertTransitions(from_state=states.FAILURE,
                               allowed=(states.REVERTING,),
                               ignored=(states.FAILURE,),
                               forbidden=(states.PENDING, states.RUNNING,
                                          states.SUCCESS, states.REVERTED))

    def test_from_reverting_state(self):
        self.assertTransitions(from_state=states.REVERTING,
                               allowed=(states.RUNNING, states.FAILURE,
                                        states.REVERTING, states.REVERTED),
                               forbidden=(states.PENDING, states.SUCCESS))

    def test_from_reverted_state(self):
        self.assertTransitions(from_state=states.REVERTED,
                               allowed=(states.PENDING,),
                               ignored=(states.REVERTING, states.REVERTED),
                               forbidden=(states.RUNNING, states.SUCCESS,
                                          states.FAILURE))
