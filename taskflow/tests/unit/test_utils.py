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

import functools

from taskflow import decorators
from taskflow import test
from taskflow import utils


class UtilTest(test.TestCase):
    def test_rollback_accum(self):
        context = {}

        def caller(token, e):
            context[token] = True

        accum = utils.RollbackAccumulator()

        def blowup():
            for i in range(0, 10):
                accum.add(functools.partial(caller, i))
            self.assertEquals(0, len(context))
            raise Exception

        # Test manual triggering
        self.assertEquals(0, len(accum))
        self.assertRaises(Exception, blowup)
        self.assertEquals(10, len(accum))
        self.assertEquals(0, len(context))
        accum.rollback(Exception())
        self.assertEquals(10, len(context))

        # Test context manager triggering
        context = {}
        accum.reset()
        self.assertEquals(0, len(accum))
        try:
            with accum:
                blowup()
        except Exception:
            pass
        self.assertEquals(10, len(accum))
        self.assertEquals(10, len(context))


def mere_function(a, b):
    pass


def function_with_defaults(a, b, optional=None):
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
        name = utils.get_callable_name(mere_function)
        self.assertEquals(name, '.'.join((__name__, 'mere_function')))

    def test_method(self):
        name = utils.get_callable_name(Class.method)
        self.assertEquals(name, '.'.join((__name__, 'Class', 'method')))

    def test_instance_method(self):
        name = utils.get_callable_name(Class().method)
        self.assertEquals(name, '.'.join((__name__, 'Class', 'method')))

    def test_static_method(self):
        # NOTE(imelnikov): static method are just functions, class name
        #   is not recorded anywhere in them
        name = utils.get_callable_name(Class.static_method)
        self.assertEquals(name, '.'.join((__name__, 'static_method')))

    def test_class_method(self):
        name = utils.get_callable_name(Class.class_method)
        self.assertEquals(name, '.'.join((__name__, 'Class', 'class_method')))

    def test_constructor(self):
        name = utils.get_callable_name(Class)
        self.assertEquals(name, '.'.join((__name__, 'Class')))

    def test_callable_class(self):
        name = utils.get_callable_name(CallableClass())
        self.assertEquals(name, '.'.join((__name__, 'CallableClass')))

    def test_callable_class_call(self):
        name = utils.get_callable_name(CallableClass().__call__)
        self.assertEquals(name, '.'.join((__name__, 'CallableClass',
                                          '__call__')))


class GetRequiredCallableArgsTest(test.TestCase):

    def test_mere_function(self):
        self.assertEquals(['a', 'b'],
                          utils.get_required_callable_args(mere_function))

    def test_function_with_defaults(self):
        self.assertEquals(['a', 'b'],
                          utils.get_required_callable_args(
                              function_with_defaults))

    def test_method(self):
        self.assertEquals(['self', 'c', 'd'],
                          utils.get_required_callable_args(Class.method))

    def test_instance_method(self):
        self.assertEquals(['c', 'd'],
                          utils.get_required_callable_args(Class().method))

    def test_class_method(self):
        self.assertEquals(['g', 'h'],
                          utils.get_required_callable_args(
                              Class.class_method))

    def test_class_constructor(self):
        self.assertEquals(['k', 'l'],
                          utils.get_required_callable_args(
                              ClassWithInit))

    def test_class_with_call(self):
        self.assertEquals(['i', 'j'],
                          utils.get_required_callable_args(
                              CallableClass()))

    def test_decorators_work(self):
        @decorators.locked
        def locked_fun(x, y):
            pass
        self.assertEquals(['x', 'y'],
                          utils.get_required_callable_args(locked_fun))