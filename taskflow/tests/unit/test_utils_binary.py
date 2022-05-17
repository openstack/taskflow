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

from taskflow import test
from taskflow.utils import misc


def _bytes(data):
    return data.encode(encoding='utf-8')


class BinaryEncodeTest(test.TestCase):

    def _check(self, data, expected_result):
        result = misc.binary_encode(data)
        self.assertIsInstance(result, bytes)
        self.assertEqual(expected_result, result)

    def test_simple_binary(self):
        data = _bytes('hello')
        self._check(data, data)

    def test_unicode_binary(self):
        data = _bytes('привет')
        self._check(data, data)

    def test_simple_text(self):
        self._check(u'hello', _bytes('hello'))

    def test_unicode_text(self):
        self._check(u'привет', _bytes('привет'))

    def test_unicode_other_encoding(self):
        result = misc.binary_encode(u'mañana', 'latin-1')
        self.assertIsInstance(result, bytes)
        self.assertEqual(u'mañana'.encode('latin-1'), result)


class BinaryDecodeTest(test.TestCase):

    def _check(self, data, expected_result):
        result = misc.binary_decode(data)
        self.assertIsInstance(result, str)
        self.assertEqual(expected_result, result)

    def test_simple_text(self):
        data = u'hello'
        self._check(data, data)

    def test_unicode_text(self):
        data = u'привет'
        self._check(data, data)

    def test_simple_binary(self):
        self._check(_bytes('hello'), u'hello')

    def test_unicode_binary(self):
        self._check(_bytes('привет'), u'привет')

    def test_unicode_other_encoding(self):
        data = u'mañana'.encode('latin-1')
        result = misc.binary_decode(data, 'latin-1')
        self.assertIsInstance(result, str)
        self.assertEqual(u'mañana', result)


class DecodeJsonTest(test.TestCase):

    def test_it_works(self):
        self.assertEqual({"foo": 1},
                         misc.decode_json(_bytes('{"foo": 1}')))

    def test_it_works_with_unicode(self):
        data = _bytes('{"foo": "фуу"}')
        self.assertEqual({"foo": u'фуу'}, misc.decode_json(data))

    def test_handles_invalid_unicode(self):
        self.assertRaises(ValueError, misc.decode_json,
                          '{"\xf1": 1}'.encode('latin-1'))

    def test_handles_bad_json(self):
        self.assertRaises(ValueError, misc.decode_json,
                          _bytes('{"foo":'))

    def test_handles_wrong_types(self):
        self.assertRaises(ValueError, misc.decode_json,
                          _bytes('42'))
