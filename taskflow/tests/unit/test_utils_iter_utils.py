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

from six.moves import range as compat_range

from taskflow import test
from taskflow.utils import iter_utils


def forever_it():
    i = 0
    while True:
        yield i
        i += 1


class IterUtilsTest(test.TestCase):
    def test_find_first_match(self):
        it = forever_it()
        self.assertEqual(100, iter_utils.find_first_match(it,
                                                          lambda v: v == 100))

    def test_find_first_match_not_found(self):
        it = iter(string.ascii_lowercase)
        self.assertIsNone(iter_utils.find_first_match(it,
                                                      lambda v: v == ''))

    def test_count(self):
        self.assertEqual(0, iter_utils.count([]))
        self.assertEqual(1, iter_utils.count(['a']))
        self.assertEqual(10, iter_utils.count(compat_range(0, 10)))
        self.assertEqual(1000, iter_utils.count(compat_range(0, 1000)))
        self.assertEqual(0, iter_utils.count(compat_range(0)))
        self.assertEqual(0, iter_utils.count(compat_range(-1)))

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
