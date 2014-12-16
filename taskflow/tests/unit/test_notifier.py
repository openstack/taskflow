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

    def test_notify_not_called(self):
        call_collector = []

        def call_me(state, details):
            call_collector.append((state, details))

        notifier = nt.Notifier()
        notifier.register(nt.Notifier.ANY, call_me)
        notifier.notify(nt.Notifier.ANY, {})
        self.assertFalse(notifier.can_trigger_notification(nt.Notifier.ANY))

        self.assertEqual(0, len(call_collector))
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

    def test_not_callable(self):
        notifier = nt.Notifier()
        self.assertRaises(ValueError, notifier.register,
                          nt.Notifier.ANY, 2)

    def test_restricted_notifier(self):
        notifier = nt.RestrictedNotifier(['a', 'b'])
        self.assertRaises(ValueError, notifier.register,
                          'c', lambda *args, **kargs: None)
        notifier.register('b', lambda *args, **kargs: None)
        self.assertEqual(1, len(notifier))

    def test_restricted_notifier_any(self):
        notifier = nt.RestrictedNotifier(['a', 'b'])
        self.assertRaises(ValueError, notifier.register,
                          'c', lambda *args, **kargs: None)
        notifier.register('b', lambda *args, **kargs: None)
        self.assertEqual(1, len(notifier))
        notifier.register(nt.RestrictedNotifier.ANY,
                          lambda *args, **kargs: None)
        self.assertEqual(2, len(notifier))

    def test_restricted_notifier_no_any(self):
        notifier = nt.RestrictedNotifier(['a', 'b'], allow_any=False)
        self.assertRaises(ValueError, notifier.register,
                          nt.RestrictedNotifier.ANY,
                          lambda *args, **kargs: None)
        notifier.register('b', lambda *args, **kargs: None)
        self.assertEqual(1, len(notifier))

    def test_selective_notify(self):
        call_counts = collections.defaultdict(list)

        def call_me_on(registered_state, state, details):
            call_counts[registered_state].append((state, details))

        notifier = nt.Notifier()

        call_me_on_success = functools.partial(call_me_on, states.SUCCESS)
        notifier.register(states.SUCCESS, call_me_on_success)
        self.assertTrue(notifier.is_registered(states.SUCCESS,
                                               call_me_on_success))

        call_me_on_any = functools.partial(call_me_on, nt.Notifier.ANY)
        notifier.register(nt.Notifier.ANY, call_me_on_any)
        self.assertTrue(notifier.is_registered(nt.Notifier.ANY,
                                               call_me_on_any))

        self.assertEqual(2, len(notifier))
        notifier.notify(states.SUCCESS, {})

        self.assertEqual(1, len(call_counts[nt.Notifier.ANY]))
        self.assertEqual(1, len(call_counts[states.SUCCESS]))

        notifier.notify(states.FAILURE, {})
        self.assertEqual(2, len(call_counts[nt.Notifier.ANY]))
        self.assertEqual(1, len(call_counts[states.SUCCESS]))
        self.assertEqual(2, len(call_counts))

    def test_details_filter(self):
        call_counts = collections.defaultdict(list)

        def call_me_on(registered_state, state, details):
            call_counts[registered_state].append((state, details))

        def when_red(details):
            return details.get('color') == 'red'

        notifier = nt.Notifier()

        call_me_on_success = functools.partial(call_me_on, states.SUCCESS)
        notifier.register(states.SUCCESS, call_me_on_success,
                          details_filter=when_red)
        self.assertEqual(1, len(notifier))
        self.assertTrue(notifier.is_registered(
            states.SUCCESS, call_me_on_success, details_filter=when_red))

        notifier.notify(states.SUCCESS, {})
        self.assertEqual(0, len(call_counts[states.SUCCESS]))
        notifier.notify(states.SUCCESS, {'color': 'red'})
        self.assertEqual(1, len(call_counts[states.SUCCESS]))
        notifier.notify(states.SUCCESS, {'color': 'green'})
        self.assertEqual(1, len(call_counts[states.SUCCESS]))

    def test_different_details_filter(self):
        call_counts = collections.defaultdict(list)

        def call_me_on(registered_state, state, details):
            call_counts[registered_state].append((state, details))

        def when_red(details):
            return details.get('color') == 'red'

        def when_blue(details):
            return details.get('color') == 'blue'

        notifier = nt.Notifier()

        call_me_on_success = functools.partial(call_me_on, states.SUCCESS)
        notifier.register(states.SUCCESS, call_me_on_success,
                          details_filter=when_red)
        notifier.register(states.SUCCESS, call_me_on_success,
                          details_filter=when_blue)
        self.assertEqual(2, len(notifier))
        self.assertTrue(notifier.is_registered(
            states.SUCCESS, call_me_on_success, details_filter=when_blue))
        self.assertTrue(notifier.is_registered(
            states.SUCCESS, call_me_on_success, details_filter=when_red))

        notifier.notify(states.SUCCESS, {})
        self.assertEqual(0, len(call_counts[states.SUCCESS]))
        notifier.notify(states.SUCCESS, {'color': 'red'})
        self.assertEqual(1, len(call_counts[states.SUCCESS]))
        notifier.notify(states.SUCCESS, {'color': 'blue'})
        self.assertEqual(2, len(call_counts[states.SUCCESS]))
        notifier.notify(states.SUCCESS, {'color': 'green'})
        self.assertEqual(2, len(call_counts[states.SUCCESS]))
