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


from taskflow import exceptions
from taskflow import test
from taskflow.tests import utils as test_utils

from taskflow.utils import misc


def _captured_failure(msg):
        try:
            raise RuntimeError(msg)
        except Exception:
            return misc.Failure()


class GeneralFailureObjTestsMixin(object):

    def test_captures_message(self):
        self.assertEqual(self.fail_obj.exception_str, 'Woot!')

    def test_str(self):
        self.assertEqual(str(self.fail_obj),
                         'Failure: RuntimeError: Woot!')

    def test_exception_types(self):
        self.assertEqual(list(self.fail_obj),
                         test_utils.RUNTIME_ERROR_CLASSES[:-2])

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
        with self.assertRaisesRegexp(RuntimeError, '^Woot!$'):
            self.fail_obj.reraise()


class ReCreatedFailureTestCase(test.TestCase, GeneralFailureObjTestsMixin):

    def setUp(self):
        super(ReCreatedFailureTestCase, self).setUp()
        fail_obj = _captured_failure('Woot!')
        self.fail_obj = misc.Failure(exception_str=fail_obj.exception_str,
                                     traceback_str=fail_obj.traceback_str,
                                     exc_type_names=list(fail_obj))

    def test_value_lost(self):
        self.assertIs(self.fail_obj.exception, None)

    def test_no_exc_info(self):
        self.assertIs(self.fail_obj.exc_info, None)

    def test_reraises(self):
        with self.assertRaises(exceptions.WrappedFailure) as ctx:
            self.fail_obj.reraise()
        exc = ctx.exception
        self.assertIs(exc.check(RuntimeError), RuntimeError)


class FailureObjectTestCase(test.TestCase):

    def test_dont_catch_base_exception(self):
        try:
            raise SystemExit()
        except BaseException:
            self.assertRaises(TypeError, misc.Failure)

    def test_unknown_argument(self):
        with self.assertRaises(TypeError) as ctx:
            misc.Failure(
                exception_str='Woot!',
                traceback_str=None,
                exc_type_names=['Exception'],
                hi='hi there')
        expected = "Failure.__init__ got unexpected keyword argument(s): hi"
        self.assertEqual(str(ctx.exception), expected)

    def test_empty_does_not_reraise(self):
        self.assertIs(misc.Failure.reraise_if_any([]), None)

    def test_reraises_one(self):
        fls = [_captured_failure('Woot!')]
        with self.assertRaisesRegexp(RuntimeError, '^Woot!$'):
            misc.Failure.reraise_if_any(fls)

    def test_reraises_several(self):
        fls = [
            _captured_failure('Woot!'),
            _captured_failure('Oh, not again!')
        ]
        with self.assertRaises(exceptions.WrappedFailure) as ctx:
            misc.Failure.reraise_if_any(fls)
        self.assertEqual(list(ctx.exception), fls)

    def test_failure_copy(self):
        fail_obj = _captured_failure('Woot!')

        copied = fail_obj.copy()
        self.assertIsNot(fail_obj, copied)
        self.assertEqual(fail_obj, copied)
        self.assertTrue(fail_obj.matches(copied))

    def test_failure_copy_recaptured(self):
        captured = _captured_failure('Woot!')
        fail_obj = misc.Failure(exception_str=captured.exception_str,
                                traceback_str=captured.traceback_str,
                                exc_type_names=list(captured))
        copied = fail_obj.copy()
        self.assertIsNot(fail_obj, copied)
        self.assertEqual(fail_obj, copied)
        self.assertFalse(fail_obj != copied)
        self.assertTrue(fail_obj.matches(copied))

    def test_recaptured_not_eq(self):
        captured = _captured_failure('Woot!')
        fail_obj = misc.Failure(exception_str=captured.exception_str,
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
        fail_obj = misc.Failure(exception_str=captured.exception_str,
                                traceback_str=captured.traceback_str,
                                exc_type_names=list(captured))
        new_exc_str = captured.exception_str.replace('Woot', 'w00t')
        fail_obj2 = misc.Failure(exception_str=new_exc_str,
                                 traceback_str=captured.traceback_str,
                                 exc_type_names=list(captured))
        self.assertNotEqual(fail_obj, fail_obj2)
        self.assertFalse(fail_obj2.matches(fail_obj))

    def test_compares_to_none(self):
        captured = _captured_failure('Woot!')
        self.assertNotEqual(captured, None)
        self.assertFalse(captured.matches(None))


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
            fail_obj = misc.Failure()

        wf = exceptions.WrappedFailure([fail_obj, f3])
        self.assertEqual(list(wf), [f1, f2, f3])
