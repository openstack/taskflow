# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

import string

import six
from six.moves import range as compat_range

from taskflow import test
from taskflow.utils import iter_utils


def forever_it():
    i = 0
    while True:
        yield i
        i += 1


class IterUtilsTest(test.TestCase):
    def test_fill_empty(self):
        self.assertEqual([], list(iter_utils.fill([1, 2, 3], 0)))

    def test_bad_unique_seen(self):
        iters = [
            ['a', 'b'],
            2,
            None,
            object(),
        ]
        self.assertRaises(ValueError,
                          iter_utils.unique_seen, iters)

    def test_generate_delays(self):
        it = iter_utils.generate_delays(1, 60)
        self.assertEqual(1, six.next(it))
        self.assertEqual(2, six.next(it))
        self.assertEqual(4, six.next(it))
        self.assertEqual(8, six.next(it))
        self.assertEqual(16, six.next(it))
        self.assertEqual(32, six.next(it))
        self.assertEqual(60, six.next(it))
        self.assertEqual(60, six.next(it))

    def test_generate_delays_custom_multiplier(self):
        it = iter_utils.generate_delays(1, 60, multiplier=4)
        self.assertEqual(1, six.next(it))
        self.assertEqual(4, six.next(it))
        self.assertEqual(16, six.next(it))
        self.assertEqual(60, six.next(it))
        self.assertEqual(60, six.next(it))

    def test_generate_delays_bad(self):
        self.assertRaises(ValueError, iter_utils.generate_delays, -1, -1)
        self.assertRaises(ValueError, iter_utils.generate_delays, -1, 2)
        self.assertRaises(ValueError, iter_utils.generate_delays, 2, -1)
        self.assertRaises(ValueError, iter_utils.generate_delays, 1, 1,
                          multiplier=0.5)

    def test_unique_seen(self):
        iters = [
            ['a', 'b'],
            ['a', 'c', 'd'],
            ['a', 'e', 'f'],
            ['f', 'm', 'n'],
        ]
        self.assertEqual(['a', 'b', 'c', 'd', 'e', 'f', 'm', 'n'],
                         list(iter_utils.unique_seen(iters)))

    def test_unique_seen_empty(self):
        iters = []
        self.assertEqual([], list(iter_utils.unique_seen(iters)))

    def test_unique_seen_selector(self):
        iters = [
            [(1, 'a'), (1, 'a')],
            [(2, 'b')],
            [(3, 'c')],
            [(1, 'a'), (3, 'c')],
        ]
        it = iter_utils.unique_seen(iters,
                                    seen_selector=lambda value: value[0])
        self.assertEqual([(1, 'a'), (2, 'b'), (3, 'c')], list(it))

    def test_bad_fill(self):
        self.assertRaises(ValueError, iter_utils.fill, 2, 2)

    def test_fill_many_empty(self):
        result = list(iter_utils.fill(compat_range(0, 50), 500))
        self.assertEqual(450, sum(1 for x in result if x is None))
        self.assertEqual(50, sum(1 for x in result if x is not None))

    def test_fill_custom_filler(self):
        self.assertEqual("abcd",
                         "".join(iter_utils.fill("abc", 4, filler='d')))

    def test_fill_less_needed(self):
        self.assertEqual("ab", "".join(iter_utils.fill("abc", 2)))

    def test_fill(self):
        self.assertEqual([None, None], list(iter_utils.fill([], 2)))
        self.assertEqual((None, None), tuple(iter_utils.fill([], 2)))

    def test_bad_find_first_match(self):
        self.assertRaises(ValueError,
                          iter_utils.find_first_match, 2, lambda v: False)

    def test_find_first_match(self):
        it = forever_it()
        self.assertEqual(100, iter_utils.find_first_match(it,
                                                          lambda v: v == 100))

    def test_find_first_match_not_found(self):
        it = iter(string.ascii_lowercase)
        self.assertIsNone(iter_utils.find_first_match(it,
                                                      lambda v: v == ''))

    def test_bad_count(self):
        self.assertRaises(ValueError, iter_utils.count, 2)

    def test_count(self):
        self.assertEqual(0, iter_utils.count([]))
        self.assertEqual(1, iter_utils.count(['a']))
        self.assertEqual(10, iter_utils.count(compat_range(0, 10)))
        self.assertEqual(1000, iter_utils.count(compat_range(0, 1000)))
        self.assertEqual(0, iter_utils.count(compat_range(0)))
        self.assertEqual(0, iter_utils.count(compat_range(-1)))

    def test_bad_while_is_not(self):
        self.assertRaises(ValueError, iter_utils.while_is_not, 2, 'a')

    def test_while_is_not(self):
        it = iter(string.ascii_lowercase)
        self.assertEqual(['a'],
                         list(iter_utils.while_is_not(it, 'a')))
        it = iter(string.ascii_lowercase)
        self.assertEqual(['a', 'b'],
                         list(iter_utils.while_is_not(it, 'b')))
        self.assertEqual(list(string.ascii_lowercase[2:]),
                         list(iter_utils.while_is_not(it, 'zzz')))
        it = iter(string.ascii_lowercase)
        self.assertEqual(list(string.ascii_lowercase),
                         list(iter_utils.while_is_not(it, '')))
