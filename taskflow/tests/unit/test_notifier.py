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

import collections
import functools

from taskflow import states
from taskflow import test
from taskflow.types import notifier as nt
from taskflow.utils import misc


class DeprecatedTestCase(test.TestCase):
    def test_deprecated(self):
        notifier = misc.Notifier()
        self.assertIsInstance(notifier, misc.Notifier)
        self.assertIsInstance(notifier, nt.Notifier)
        self.assertTrue(hasattr(misc.Notifier, 'ANY'))


class NotifierTest(test.TestCase):

    def test_notify_called(self):
        call_collector = []

        def call_me(state, details):
            call_collector.append((state, details))

        notifier = nt.Notifier()
        notifier.register(nt.Notifier.ANY, call_me)
        notifier.notify(states.SUCCESS, {})
        notifier.notify(states.SUCCESS, {})

        self.assertEqual(2, len(call_collector))
        self.assertEqual(1, len(notifier))

    def test_notify_register_deregister(self):

        def call_me(state, details):
            pass

        class A(object):
            def call_me_too(self, state, details):
                pass

        notifier = nt.Notifier()
        notifier.register(nt.Notifier.ANY, call_me)
        a = A()
        notifier.register(nt.Notifier.ANY, a.call_me_too)

        self.assertEqual(2, len(notifier))
        notifier.deregister(nt.Notifier.ANY, call_me)
        notifier.deregister(nt.Notifier.ANY, a.call_me_too)
        self.assertEqual(0, len(notifier))

    def test_notify_reset(self):

        def call_me(state, details):
            pass

        notifier = nt.Notifier()
        notifier.register(nt.Notifier.ANY, call_me)
        self.assertEqual(1, len(notifier))

        notifier.reset()
        self.assertEqual(0, len(notifier))

    def test_bad_notify(self):

        def call_me(state, details):
            pass

        notifier = nt.Notifier()
        self.assertRaises(KeyError, notifier.register,
                          nt.Notifier.ANY, call_me,
                          kwargs={'details': 5})

    def test_selective_notify(self):
        call_counts = collections.defaultdict(list)

        def call_me_on(registered_state, state, details):
            call_counts[registered_state].append((state, details))

        notifier = nt.Notifier()
        notifier.register(states.SUCCESS,
                          functools.partial(call_me_on, states.SUCCESS))
        notifier.register(nt.Notifier.ANY,
                          functools.partial(call_me_on,
                                            nt.Notifier.ANY))

        self.assertEqual(2, len(notifier))
        notifier.notify(states.SUCCESS, {})

        self.assertEqual(1, len(call_counts[nt.Notifier.ANY]))
        self.assertEqual(1, len(call_counts[states.SUCCESS]))

        notifier.notify(states.FAILURE, {})
        self.assertEqual(2, len(call_counts[nt.Notifier.ANY]))
        self.assertEqual(1, len(call_counts[states.SUCCESS]))
        self.assertEqual(2, len(call_counts))
