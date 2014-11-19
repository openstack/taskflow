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

import sys

import six

from taskflow import exceptions
from taskflow import test
from taskflow.tests import utils as test_utils
from taskflow.types import failure


def _captured_failure(msg):
    try:
        raise RuntimeError(msg)
    except Exception:
        return failure.Failure()


def _make_exc_info(msg):
    try:
        raise RuntimeError(msg)
    except Exception:
        return sys.exc_info()


class GeneralFailureObjTestsMixin(object):

    def test_captures_message(self):
        self.assertEqual(self.fail_obj.exception_str, 'Woot!')

    def test_str(self):
        self.assertEqual(str(self.fail_obj),
                         'Failure: RuntimeError: Woot!')

    def test_exception_types(self):
        self.assertEqual(list(self.fail_obj),
                         test_utils.RUNTIME_ERROR_CLASSES[:-2])

    def test_pformat_no_traceback(self):
        text = self.fail_obj.pformat()
        self.assertNotIn("Traceback", text)

    def test_check_str(self):
        val = 'Exception'
        self.assertEqual(self.fail_obj.check(val), val)

    def test_check_str_not_there(self):
        val = 'ValueError'
        self.assertEqual(self.fail_obj.check(val), None)

    def test_check_type(self):
        self.assertIs(self.fail_obj.check(RuntimeError), RuntimeError)

    def test_check_type_not_there(self):
        self.assertIs(self.fail_obj.check(ValueError), None)


class CaptureFailureTestCase(test.TestCase, GeneralFailureObjTestsMixin):

    def setUp(self):
        super(CaptureFailureTestCase, self).setUp()
        self.fail_obj = _captured_failure('Woot!')

    def test_captures_value(self):
        self.assertIsInstance(self.fail_obj.exception, RuntimeError)

    def test_captures_exc_info(self):
        exc_info = self.fail_obj.exc_info
        self.assertEqual(len(exc_info), 3)
        self.assertEqual(exc_info[0], RuntimeError)
        self.assertIs(exc_info[1], self.fail_obj.exception)

    def test_reraises(self):
        self.assertRaisesRegexp(RuntimeError, '^Woot!$', self.fail_obj.reraise)


class ReCreatedFailureTestCase(test.TestCase, GeneralFailureObjTestsMixin):

    def setUp(self):
        super(ReCreatedFailureTestCase, self).setUp()
        fail_obj = _captured_failure('Woot!')
        self.fail_obj = failure.Failure(exception_str=fail_obj.exception_str,
                                        traceback_str=fail_obj.traceback_str,
                                        exc_type_names=list(fail_obj))

    def test_value_lost(self):
        self.assertIs(self.fail_obj.exception, None)

    def test_no_exc_info(self):
        self.assertIs(self.fail_obj.exc_info, None)

    def test_pformat_traceback(self):
        text = self.fail_obj.pformat(traceback=True)
        self.assertIn("Traceback (most recent call last):", text)

    def test_reraises(self):
        exc = self.assertRaises(exceptions.WrappedFailure,
                                self.fail_obj.reraise)
        self.assertIs(exc.check(RuntimeError), RuntimeError)

    def test_no_type_names(self):
        fail_obj = _captured_failure('Woot!')
        fail_obj = failure.Failure(exception_str=fail_obj.exception_str,
                                   traceback_str=fail_obj.traceback_str,
                                   exc_type_names=[])
        self.assertEqual([], list(fail_obj))
        self.assertEqual("Failure: Woot!", fail_obj.pformat())


class FromExceptionTestCase(test.TestCase, GeneralFailureObjTestsMixin):

    def setUp(self):
        super(FromExceptionTestCase, self).setUp()
        self.fail_obj = failure.Failure.from_exception(RuntimeError('Woot!'))

    def test_pformat_no_traceback(self):
        text = self.fail_obj.pformat(traceback=True)
        self.assertIn("Traceback not available", text)


class FailureObjectTestCase(test.TestCase):

    def test_dont_catch_base_exception(self):
        try:
            raise SystemExit()
        except BaseException:
            self.assertRaises(TypeError, failure.Failure)

    def test_unknown_argument(self):
        exc = self.assertRaises(TypeError, failure.Failure,
                                exception_str='Woot!',
                                traceback_str=None,
                                exc_type_names=['Exception'],
                                hi='hi there')
        expected = "Failure.__init__ got unexpected keyword argument(s): hi"
        self.assertEqual(str(exc), expected)

    def test_empty_does_not_reraise(self):
        self.assertIs(failure.Failure.reraise_if_any([]), None)

    def test_reraises_one(self):
        fls = [_captured_failure('Woot!')]
        self.assertRaisesRegexp(RuntimeError, '^Woot!$',
                                failure.Failure.reraise_if_any, fls)

    def test_reraises_several(self):
        fls = [
            _captured_failure('Woot!'),
            _captured_failure('Oh, not again!')
        ]
        exc = self.assertRaises(exceptions.WrappedFailure,
                                failure.Failure.reraise_if_any, fls)
        self.assertEqual(list(exc), fls)

    def test_failure_copy(self):
        fail_obj = _captured_failure('Woot!')

        copied = fail_obj.copy()
        self.assertIsNot(fail_obj, copied)
        self.assertEqual(fail_obj, copied)
        self.assertTrue(fail_obj.matches(copied))

    def test_failure_copy_recaptured(self):
        captured = _captured_failure('Woot!')
        fail_obj = failure.Failure(exception_str=captured.exception_str,
                                   traceback_str=captured.traceback_str,
                                   exc_type_names=list(captured))
        copied = fail_obj.copy()
        self.assertIsNot(fail_obj, copied)
        self.assertEqual(fail_obj, copied)
        self.assertFalse(fail_obj != copied)
        self.assertTrue(fail_obj.matches(copied))

    def test_recaptured_not_eq(self):
        captured = _captured_failure('Woot!')
        fail_obj = failure.Failure(exception_str=captured.exception_str,
                                   traceback_str=captured.traceback_str,
                                   exc_type_names=list(captured))
        self.assertFalse(fail_obj == captured)
        self.assertTrue(fail_obj != captured)
        self.assertTrue(fail_obj.matches(captured))

    def test_two_captured_eq(self):
        captured = _captured_failure('Woot!')
        captured2 = _captured_failure('Woot!')
        self.assertEqual(captured, captured2)

    def test_two_recaptured_neq(self):
        captured = _captured_failure('Woot!')
        fail_obj = failure.Failure(exception_str=captured.exception_str,
                                   traceback_str=captured.traceback_str,
                                   exc_type_names=list(captured))
        new_exc_str = captured.exception_str.replace('Woot', 'w00t')
        fail_obj2 = failure.Failure(exception_str=new_exc_str,
                                    traceback_str=captured.traceback_str,
                                    exc_type_names=list(captured))
        self.assertNotEqual(fail_obj, fail_obj2)
        self.assertFalse(fail_obj2.matches(fail_obj))

    def test_compares_to_none(self):
        captured = _captured_failure('Woot!')
        self.assertNotEqual(captured, None)
        self.assertFalse(captured.matches(None))

    def test_pformat_traceback(self):
        captured = _captured_failure('Woot!')
        text = captured.pformat(traceback=True)
        self.assertIn("Traceback (most recent call last):", text)

    def test_pformat_traceback_captured_no_exc_info(self):
        captured = _captured_failure('Woot!')
        captured = failure.Failure.from_dict(captured.to_dict())
        text = captured.pformat(traceback=True)
        self.assertIn("Traceback (most recent call last):", text)


class WrappedFailureTestCase(test.TestCase):

    def test_simple_iter(self):
        fail_obj = _captured_failure('Woot!')
        wf = exceptions.WrappedFailure([fail_obj])
        self.assertEqual(len(wf), 1)
        self.assertEqual(list(wf), [fail_obj])

    def test_simple_check(self):
        fail_obj = _captured_failure('Woot!')
        wf = exceptions.WrappedFailure([fail_obj])
        self.assertEqual(wf.check(RuntimeError), RuntimeError)
        self.assertEqual(wf.check(ValueError), None)

    def test_two_failures(self):
        fls = [
            _captured_failure('Woot!'),
            _captured_failure('Oh, not again!')
        ]
        wf = exceptions.WrappedFailure(fls)
        self.assertEqual(len(wf), 2)
        self.assertEqual(list(wf), fls)

    def test_flattening(self):
        f1 = _captured_failure('Wrap me')
        f2 = _captured_failure('Wrap me, too')
        f3 = _captured_failure('Woot!')
        try:
            raise exceptions.WrappedFailure([f1, f2])
        except Exception:
            fail_obj = failure.Failure()

        wf = exceptions.WrappedFailure([fail_obj, f3])
        self.assertEqual(list(wf), [f1, f2, f3])


class NonAsciiExceptionsTestCase(test.TestCase):

    def test_exception_with_non_ascii_str(self):
        bad_string = chr(200)
        fail = failure.Failure.from_exception(ValueError(bad_string))
        self.assertEqual(fail.exception_str, bad_string)
        self.assertEqual(str(fail), 'Failure: ValueError: %s' % bad_string)

    def test_exception_non_ascii_unicode(self):
        hi_ru = u'привет'
        fail = failure.Failure.from_exception(ValueError(hi_ru))
        self.assertEqual(fail.exception_str, hi_ru)
        self.assertIsInstance(fail.exception_str, six.text_type)
        self.assertEqual(six.text_type(fail),
                         u'Failure: ValueError: %s' % hi_ru)

    def test_wrapped_failure_non_ascii_unicode(self):
        hi_cn = u'嗨'
        fail = ValueError(hi_cn)
        self.assertEqual(hi_cn, exceptions.exception_message(fail))
        fail = failure.Failure.from_exception(fail)
        wrapped_fail = exceptions.WrappedFailure([fail])
        if six.PY2:
            # Python 2.x will unicode escape it, while python 3.3+ will not,
            # so we sadly have to differentiate between these two...
            expected_result = (u"WrappedFailure: "
                               "[u'Failure: ValueError: %s']"
                               % (hi_cn.encode("unicode-escape")))
        else:
            expected_result = (u"WrappedFailure: "
                               "['Failure: ValueError: %s']" % (hi_cn))
        self.assertEqual(expected_result, six.text_type(wrapped_fail))

    def test_failure_equality_with_non_ascii_str(self):
        bad_string = chr(200)
        fail = failure.Failure.from_exception(ValueError(bad_string))
        copied = fail.copy()
        self.assertEqual(fail, copied)

    def test_failure_equality_non_ascii_unicode(self):
        hi_ru = u'привет'
        fail = failure.Failure.from_exception(ValueError(hi_ru))
        copied = fail.copy()
        self.assertEqual(fail, copied)


class ExcInfoUtilsTest(test.TestCase):
    def test_copy_none(self):
        result = failure._copy_exc_info(None)
        self.assertIsNone(result)

    def test_copy_exc_info(self):
        exc_info = _make_exc_info("Woot!")
        result = failure._copy_exc_info(exc_info)
        self.assertIsNot(result, exc_info)
        self.assertIs(result[0], RuntimeError)
        self.assertIsNot(result[1], exc_info[1])
        self.assertIs(result[2], exc_info[2])

    def test_none_equals(self):
        self.assertTrue(failure._are_equal_exc_info_tuples(None, None))

    def test_none_ne_tuple(self):
        exc_info = _make_exc_info("Woot!")
        self.assertFalse(failure._are_equal_exc_info_tuples(None, exc_info))

    def test_tuple_nen_none(self):
        exc_info = _make_exc_info("Woot!")
        self.assertFalse(failure._are_equal_exc_info_tuples(exc_info, None))

    def test_tuple_equals_itself(self):
        exc_info = _make_exc_info("Woot!")
        self.assertTrue(failure._are_equal_exc_info_tuples(exc_info, exc_info))

    def test_typle_equals_copy(self):
        exc_info = _make_exc_info("Woot!")
        copied = failure._copy_exc_info(exc_info)
        self.assertTrue(failure._are_equal_exc_info_tuples(exc_info, copied))
