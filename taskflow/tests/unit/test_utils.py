# -*- coding: utf-8 -*-

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

import collections
import inspect
import random
import time

import testscenarios

from taskflow import test
from taskflow.utils import misc
from taskflow.utils import threading_utils


class CachedPropertyTest(test.TestCase):
    def test_attribute_caching(self):

        class A(object):
            def __init__(self):
                self.call_counter = 0

            @misc.cachedproperty
            def b(self):
                self.call_counter += 1
                return 'b'

        a = A()
        self.assertEqual('b', a.b)
        self.assertEqual('b', a.b)
        self.assertEqual(1, a.call_counter)

    def test_custom_property(self):

        class A(object):
            @misc.cachedproperty('_c')
            def b(self):
                return 'b'

        a = A()
        self.assertEqual('b', a.b)
        self.assertEqual('b', a._c)

    def test_no_delete(self):

        def try_del(a):
            del a.b

        class A(object):
            @misc.cachedproperty
            def b(self):
                return 'b'

        a = A()
        self.assertEqual('b', a.b)
        self.assertRaises(AttributeError, try_del, a)
        self.assertEqual('b', a.b)

    def test_set(self):

        def try_set(a):
            a.b = 'c'

        class A(object):
            @misc.cachedproperty
            def b(self):
                return 'b'

        a = A()
        self.assertEqual('b', a.b)
        self.assertRaises(AttributeError, try_set, a)
        self.assertEqual('b', a.b)

    def test_documented_property(self):

        class A(object):
            @misc.cachedproperty
            def b(self):
                """I like bees."""
                return 'b'

        self.assertEqual("I like bees.", inspect.getdoc(A.b))

    def test_undocumented_property(self):

        class A(object):
            @misc.cachedproperty
            def b(self):
                return 'b'

        self.assertIsNone(inspect.getdoc(A.b))

    def test_threaded_access_property(self):
        called = collections.deque()

        class A(object):
            @misc.cachedproperty
            def b(self):
                called.append(1)
                # NOTE(harlowja): wait for a little and give some time for
                # another thread to potentially also get in this method to
                # also create the same property...
                time.sleep(random.random() * 0.5)
                return 'b'

        a = A()
        threads = []
        try:
            for _i in range(0, 20):
                t = threading_utils.daemon_thread(lambda: a.b)
                threads.append(t)
            for t in threads:
                t.start()
        finally:
            while threads:
                t = threads.pop()
                t.join()

        self.assertEqual(1, len(called))
        self.assertEqual('b', a.b)


class UriParseTest(test.TestCase):
    def test_parse(self):
        url = "zookeeper://192.168.0.1:2181/a/b/?c=d"
        parsed = misc.parse_uri(url)
        self.assertEqual('zookeeper', parsed.scheme)
        self.assertEqual(2181, parsed.port)
        self.assertEqual('192.168.0.1', parsed.hostname)
        self.assertEqual('', parsed.fragment)
        self.assertEqual('/a/b/', parsed.path)
        self.assertEqual({'c': 'd'}, parsed.params())

    def test_port_provided(self):
        url = "rabbitmq://www.yahoo.com:5672"
        parsed = misc.parse_uri(url)
        self.assertEqual('rabbitmq', parsed.scheme)
        self.assertEqual('www.yahoo.com', parsed.hostname)
        self.assertEqual(5672, parsed.port)
        self.assertEqual('', parsed.path)

    def test_ipv6_host(self):
        url = "rsync://[2001:db8:0:1]:873"
        parsed = misc.parse_uri(url)
        self.assertEqual('rsync', parsed.scheme)
        self.assertEqual('2001:db8:0:1', parsed.hostname)
        self.assertEqual(873, parsed.port)

    def test_user_password(self):
        url = "rsync://test:test_pw@www.yahoo.com:873"
        parsed = misc.parse_uri(url)
        self.assertEqual('test', parsed.username)
        self.assertEqual('test_pw', parsed.password)
        self.assertEqual('www.yahoo.com', parsed.hostname)

    def test_user(self):
        url = "rsync://test@www.yahoo.com:873"
        parsed = misc.parse_uri(url)
        self.assertEqual('test', parsed.username)
        self.assertIsNone(parsed.password)


class TestSequenceMinus(test.TestCase):

    def test_simple_case(self):
        result = misc.sequence_minus([1, 2, 3, 4], [2, 3])
        self.assertEqual([1, 4], result)

    def test_subtrahend_has_extra_elements(self):
        result = misc.sequence_minus([1, 2, 3, 4], [2, 3, 5, 7, 13])
        self.assertEqual([1, 4], result)

    def test_some_items_are_equal(self):
        result = misc.sequence_minus([1, 1, 1, 1], [1, 1, 3])
        self.assertEqual([1, 1], result)

    def test_equal_items_not_continious(self):
        result = misc.sequence_minus([1, 2, 3, 1], [1, 3])
        self.assertEqual([2, 1], result)


class _StartStop(object):
    def __init__(self, name, started, stopped):
        self.started = started
        self.stopped = stopped
        self.name = name

    def start(self):
        self.started.append(self.name)

    def stop(self):
        self.stopped.append(self.name)


class _BlowUpOnStopStartStop(_StartStop):
    def stop(self):
        raise RuntimeError("broken")


class _BlowUpOnStartStartStop(_StartStop):
    def start(self):
        raise RuntimeError("broken")


class TestActivator(test.TestCase):
    def test_activation_order(self):
        started = []
        stopped = []
        things = [
            _StartStop('a', started, stopped),
            _StartStop('b', started, stopped),
            _StartStop('c', started, stopped),
        ]
        a = misc.Activator(things)
        a.start()
        self.assertEqual(0, a.need_to_be_started)
        self.assertEqual(3, a.need_to_be_stopped)
        self.assertEqual(['a', 'b', 'c'], started)
        a.stop()
        self.assertEqual(3, a.need_to_be_started)
        self.assertEqual(0, a.need_to_be_stopped)
        self.assertEqual(['c', 'b', 'a'], stopped)

    def test_repeat_start(self):
        started = []
        stopped = []
        things = [
            _StartStop('a', started, stopped),
            _StartStop('b', started, stopped),
            _StartStop('c', started, stopped),
        ]
        a = misc.Activator(things)
        a.start()
        self.assertEqual(0, a.need_to_be_started)
        self.assertEqual(3, a.need_to_be_stopped)
        a.start()
        self.assertEqual(0, a.need_to_be_started)

    def test_repeat_stop(self):
        started = []
        stopped = []
        things = [
            _StartStop('a', started, stopped),
            _StartStop('b', started, stopped),
            _StartStop('c', started, stopped),
        ]
        a = misc.Activator(things)
        a.start()
        self.assertEqual(0, a.need_to_be_started)
        self.assertEqual(3, a.need_to_be_stopped)
        a.stop()
        self.assertEqual(0, a.need_to_be_stopped)
        a.stop()
        self.assertEqual(0, a.need_to_be_stopped)

    def test_fail_activation(self):
        started = []
        stopped = []
        things = [
            _StartStop('a', started, stopped),
            _StartStop('b', started, stopped),
            _BlowUpOnStartStartStop('c', started, stopped),
            _StartStop('d', started, stopped),
        ]
        a = misc.Activator(things)
        self.assertRaises(RuntimeError, a.start)
        self.assertEqual(['a', 'b'], started)
        self.assertEqual(2, a.need_to_be_started)
        self.assertEqual(2, a.need_to_be_stopped)
        a.stop()
        self.assertEqual(4, a.need_to_be_started)
        self.assertEqual(['b', 'a'], stopped)

    def test_fail_stop(self):
        started = []
        stopped = []
        things = [
            _StartStop('a', started, stopped),
            _StartStop('b', started, stopped),
            _BlowUpOnStopStartStop('c', started, stopped),
            _StartStop('d', started, stopped),
        ]
        a = misc.Activator(things)
        a.start()
        self.assertEqual(4, a.need_to_be_stopped)
        self.assertEqual(['a', 'b', 'c', 'd'], started)
        self.assertRaises(RuntimeError, a.stop)
        self.assertEqual(3, a.need_to_be_stopped)
        self.assertRaises(RuntimeError, a.stop)
        self.assertEqual(['d'], stopped)
        self.assertEqual(3, a.need_to_be_stopped)

    def test_invalid_things(self):
        for thing in [object(), 1, "", 1.0, False]:
            self.assertRaises(AttributeError, misc.Activator, [thing])


class TestMergeUri(test.TestCase):
    def test_merge(self):
        url = "http://www.yahoo.com/?a=b&c=d"
        parsed = misc.parse_uri(url)
        joined = misc.merge_uri(parsed, {})
        self.assertEqual('b', joined.get('a'))
        self.assertEqual('d', joined.get('c'))
        self.assertEqual('www.yahoo.com', joined.get('hostname'))

    def test_merge_existing_hostname(self):
        url = "http://www.yahoo.com/"
        parsed = misc.parse_uri(url)
        joined = misc.merge_uri(parsed, {'hostname': 'b.com'})
        self.assertEqual('b.com', joined.get('hostname'))

    def test_merge_user_password(self):
        url = "http://josh:harlow@www.yahoo.com/"
        parsed = misc.parse_uri(url)
        joined = misc.merge_uri(parsed, {})
        self.assertEqual('www.yahoo.com', joined.get('hostname'))
        self.assertEqual('josh', joined.get('username'))
        self.assertEqual('harlow', joined.get('password'))

    def test_merge_user_password_existing(self):
        url = "http://josh:harlow@www.yahoo.com/"
        parsed = misc.parse_uri(url)
        existing = {
            'username': 'joe',
            'password': 'biggie',
        }
        joined = misc.merge_uri(parsed, existing)
        self.assertEqual('www.yahoo.com', joined.get('hostname'))
        self.assertEqual('joe', joined.get('username'))
        self.assertEqual('biggie', joined.get('password'))


class TestClamping(test.TestCase):
    def test_simple_clamp(self):
        result = misc.clamp(1.0, 2.0, 3.0)
        self.assertEqual(2.0, result)
        result = misc.clamp(4.0, 2.0, 3.0)
        self.assertEqual(3.0, result)
        result = misc.clamp(3.0, 4.0, 4.0)
        self.assertEqual(4.0, result)

    def test_invalid_clamp(self):
        self.assertRaises(ValueError, misc.clamp, 0.0, 2.0, 1.0)

    def test_clamped_callback(self):
        calls = []

        def on_clamped():
            calls.append(True)

        misc.clamp(-1, 0.0, 1.0, on_clamped=on_clamped)
        self.assertEqual(1, len(calls))
        calls.pop()

        misc.clamp(0.0, 0.0, 1.0, on_clamped=on_clamped)
        self.assertEqual(0, len(calls))

        misc.clamp(2, 0.0, 1.0, on_clamped=on_clamped)
        self.assertEqual(1, len(calls))


class TestIterable(test.TestCase):
    def test_string_types(self):
        self.assertFalse(misc.is_iterable('string'))
        self.assertFalse(misc.is_iterable(u'string'))

    def test_list(self):
        self.assertTrue(misc.is_iterable(list()))

    def test_tuple(self):
        self.assertTrue(misc.is_iterable(tuple()))

    def test_dict(self):
        self.assertTrue(misc.is_iterable(dict()))


class TestSafeCopyDict(testscenarios.TestWithScenarios, test.TestCase):
    scenarios = [
        ('none', {'original': None, 'expected': {}}),
        ('empty_dict', {'original': {}, 'expected': {}}),
        ('empty_list', {'original': [], 'expected': {}}),
        ('dict', {'original': {'a': 1, 'b': 2}, 'expected': {'a': 1, 'b': 2}}),
    ]

    def test_expected(self):
        self.assertEqual(self.expected, misc.safe_copy_dict(self.original))
        self.assertFalse(self.expected is misc.safe_copy_dict(self.original))

    def test_mutated_post_copy(self):
        a = {"a": "b"}
        a_2 = misc.safe_copy_dict(a)
        a['a'] = 'c'
        self.assertEqual("b", a_2['a'])
        self.assertEqual("c", a['a'])


class TestSafeCopyDictRaises(testscenarios.TestWithScenarios, test.TestCase):
    scenarios = [
        ('list', {'original': [1, 2], 'exception': TypeError}),
        ('tuple', {'original': (1, 2), 'exception': TypeError}),
        ('set', {'original': set([1, 2]), 'exception': TypeError}),
    ]

    def test_exceptions(self):
        self.assertRaises(self.exception, misc.safe_copy_dict, self.original)
