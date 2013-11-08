# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import sys

from taskflow import test
from taskflow.tests import utils as test_utils
from taskflow.utils import lock_utils
from taskflow.utils import misc
from taskflow.utils import reflection


def mere_function(a, b):
    pass


def function_with_defs(a, b, optional=None):
    pass


def function_with_kwargs(a, b, **kwargs):
    pass


class Class(object):

    def method(self, c, d):
        pass

    @staticmethod
    def static_method(e, f):
        pass

    @classmethod
    def class_method(cls, g, h):
        pass


class CallableClass(object):
    def __call__(self, i, j):
        pass


class ClassWithInit(object):
    def __init__(self, k, l):
        pass


class GetCallableNameTest(test.TestCase):

    def test_mere_function(self):
        name = reflection.get_callable_name(mere_function)
        self.assertEqual(name, '.'.join((__name__, 'mere_function')))

    def test_method(self):
        name = reflection.get_callable_name(Class.method)
        self.assertEqual(name, '.'.join((__name__, 'method')))

    def test_instance_method(self):
        name = reflection.get_callable_name(Class().method)
        self.assertEqual(name, '.'.join((__name__, 'Class', 'method')))

    def test_static_method(self):
        # NOTE(imelnikov): static method are just functions, class name
        #   is not recorded anywhere in them
        name = reflection.get_callable_name(Class.static_method)
        self.assertEqual(name, '.'.join((__name__, 'static_method')))

    def test_class_method(self):
        name = reflection.get_callable_name(Class.class_method)
        self.assertEqual(name, '.'.join((__name__, 'Class', 'class_method')))

    def test_constructor(self):
        name = reflection.get_callable_name(Class)
        self.assertEqual(name, '.'.join((__name__, 'Class')))

    def test_callable_class(self):
        name = reflection.get_callable_name(CallableClass())
        self.assertEqual(name, '.'.join((__name__, 'CallableClass')))

    def test_callable_class_call(self):
        name = reflection.get_callable_name(CallableClass().__call__)
        self.assertEqual(name, '.'.join((__name__, 'CallableClass',
                                         '__call__')))


class GetCallableArgsTest(test.TestCase):

    def test_mere_function(self):
        result = reflection.get_callable_args(mere_function)
        self.assertEqual(['a', 'b'], result)

    def test_function_with_defaults(self):
        result = reflection.get_callable_args(function_with_defs)
        self.assertEqual(['a', 'b', 'optional'], result)

    def test_required_only(self):
        result = reflection.get_callable_args(function_with_defs,
                                              required_only=True)
        self.assertEqual(['a', 'b'], result)

    def test_method(self):
        result = reflection.get_callable_args(Class.method)
        self.assertEqual(['self', 'c', 'd'], result)

    def test_instance_method(self):
        result = reflection.get_callable_args(Class().method)
        self.assertEqual(['c', 'd'], result)

    def test_class_method(self):
        result = reflection.get_callable_args(Class.class_method)
        self.assertEqual(['g', 'h'], result)

    def test_class_constructor(self):
        result = reflection.get_callable_args(ClassWithInit)
        self.assertEqual(['k', 'l'], result)

    def test_class_with_call(self):
        result = reflection.get_callable_args(CallableClass())
        self.assertEqual(['i', 'j'], result)

    def test_decorators_work(self):
        @lock_utils.locked
        def locked_fun(x, y):
            pass
        result = reflection.get_callable_args(locked_fun)
        self.assertEqual(['x', 'y'], result)


class AcceptsKwargsTest(test.TestCase):

    def test_no_kwargs(self):
        self.assertEqual(
            reflection.accepts_kwargs(mere_function), False)

    def test_with_kwargs(self):
        self.assertEqual(
            reflection.accepts_kwargs(function_with_kwargs), True)


class GetClassNameTest(test.TestCase):

    def test_std_exception(self):
        name = reflection.get_class_name(RuntimeError)
        self.assertEqual(name, 'RuntimeError')

    def test_global_class(self):
        name = reflection.get_class_name(misc.Failure)
        self.assertEqual(name, 'taskflow.utils.misc.Failure')

    def test_class(self):
        name = reflection.get_class_name(Class)
        self.assertEqual(name, '.'.join((__name__, 'Class')))

    def test_instance(self):
        name = reflection.get_class_name(Class())
        self.assertEqual(name, '.'.join((__name__, 'Class')))

    def test_int(self):
        name = reflection.get_class_name(42)
        self.assertEqual(name, 'int')


class GetAllClassNamesTest(test.TestCase):

    def test_std_class(self):
        names = list(reflection.get_all_class_names(RuntimeError))
        self.assertEqual(names, test_utils.RUNTIME_ERROR_CLASSES)

    def test_std_class_up_to(self):
        names = list(reflection.get_all_class_names(RuntimeError,
                                                    up_to=Exception))
        self.assertEqual(names, test_utils.RUNTIME_ERROR_CLASSES[:-2])


class AttrDictTest(test.TestCase):
    def test_ok_create(self):
        attrs = {
            'a': 1,
            'b': 2,
        }
        obj = misc.AttrDict(**attrs)
        self.assertEqual(obj.a, 1)
        self.assertEqual(obj.b, 2)

    def test_private_create(self):
        attrs = {
            '_a': 1,
        }
        self.assertRaises(AttributeError, misc.AttrDict, **attrs)

    def test_invalid_create(self):
        attrs = {
            # Python attributes can't start with a number
            '123_abc': 1,
        }
        self.assertRaises(AttributeError, misc.AttrDict, **attrs)

    def test_no_overwrite(self):
        attrs = {
            # Python attributes can't start with a number
            'update': 1,
        }
        self.assertRaises(AttributeError, misc.AttrDict, **attrs)

    def test_back_todict(self):
        attrs = {
            'a': 1,
        }
        obj = misc.AttrDict(**attrs)
        self.assertEqual(obj.a, 1)
        self.assertEqual(attrs, dict(obj))

    def test_runtime_invalid_set(self):
        attrs = {
            'a': 1,
        }
        obj = misc.AttrDict(**attrs)
        self.assertEqual(obj.a, 1)
        with self.assertRaises(AttributeError):
            obj._123 = 'b'

    def test_bypass_get(self):
        attrs = {
            'a': 1,
        }
        obj = misc.AttrDict(**attrs)
        self.assertEqual(1, obj['a'])

    def test_bypass_set_no_get(self):
        attrs = {
            'a': 1,
        }
        obj = misc.AttrDict(**attrs)
        self.assertEqual(1, obj['a'])
        obj['_b'] = 'c'
        with self.assertRaises(AttributeError):
            obj._b = 'e'
        self.assertEqual('c', obj['_b'])


class ExcInfoUtilsTest(test.TestCase):

    def _make_ex_info(self):
        try:
            raise RuntimeError('Woot!')
        except Exception:
            return sys.exc_info()

    def test_copy_none(self):
        result = misc.copy_exc_info(None)
        self.assertIsNone(result)

    def test_copy_exc_info(self):
        exc_info = self._make_ex_info()
        result = misc.copy_exc_info(exc_info)
        self.assertIsNot(result, exc_info)
        self.assertIs(result[0], RuntimeError)
        self.assertIsNot(result[1], exc_info[1])
        self.assertIs(result[2], exc_info[2])

    def test_none_equals(self):
        self.assertTrue(misc.are_equal_exc_info_tuples(None, None))

    def test_none_ne_tuple(self):
        exc_info = self._make_ex_info()
        self.assertFalse(misc.are_equal_exc_info_tuples(None, exc_info))

    def test_tuple_nen_none(self):
        exc_info = self._make_ex_info()
        self.assertFalse(misc.are_equal_exc_info_tuples(exc_info, None))

    def test_tuple_equals_itself(self):
        exc_info = self._make_ex_info()
        self.assertTrue(misc.are_equal_exc_info_tuples(exc_info, exc_info))

    def test_typle_equals_copy(self):
        exc_info = self._make_ex_info()
        copied = misc.copy_exc_info(exc_info)
        self.assertTrue(misc.are_equal_exc_info_tuples(exc_info, copied))
