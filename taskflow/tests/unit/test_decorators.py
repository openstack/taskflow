# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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

from taskflow import decorators
from taskflow.patterns import linear_flow
from taskflow import test


class WrapableObjectsTest(test.TestCase):

    def test_simple_function(self):
        values = []

        def revert_one(self, *args, **kwargs):
            values.append('revert one')

        @decorators.task(revert_with=revert_one)
        def run_one(self, *args, **kwargs):
            values.append('one')

        @decorators.task
        def run_fail(self, *args, **kwargs):
            values.append('fail')
            raise RuntimeError('Woot!')

        flow = linear_flow.Flow('test')
        flow.add_many((
            run_one,
            run_fail
        ))
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            flow.run(None)
        self.assertEquals(values, ['one', 'fail', 'revert one'])

    def test_simple_method(self):
        class MyTasks(object):
            def __init__(self):
                # NOTE(imelnikov): that's really *bad thing* to pass
                # data between task like this; though, its good enough
                # for our testing here
                self.values = []

            @decorators.task
            def run_one(self, *args, **kwargs):
                self.values.append('one')

            @decorators.task
            def run_fail(self, *args, **kwargs):
                self.values.append('fail')
                raise RuntimeError('Woot!')

        tasks = MyTasks()
        flow = linear_flow.Flow('test')
        flow.add_many((
            tasks.run_one,
            tasks.run_fail
        ))
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            flow.run(None)
        self.assertEquals(tasks.values, ['one', 'fail'])

    def test_static_method(self):
        values = []

        class MyTasks(object):
            @decorators.task
            @staticmethod
            def run_one(*args, **kwargs):
                values.append('one')

            # NOTE(imelnikov): decorators should work in any order:
            @staticmethod
            @decorators.task
            def run_fail(*args, **kwargs):
                values.append('fail')
                raise RuntimeError('Woot!')

        flow = linear_flow.Flow('test')
        flow.add_many((
            MyTasks.run_one,
            MyTasks.run_fail
        ))
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            flow.run(None)
        self.assertEquals(values, ['one', 'fail'])

    def test_class_method(self):

        class MyTasks(object):
            values = []

            @decorators.task
            @classmethod
            def run_one(cls, *args, **kwargs):
                cls.values.append('one')

            # NOTE(imelnikov): decorators should work in any order:
            @classmethod
            @decorators.task
            def run_fail(cls, *args, **kwargs):
                cls.values.append('fail')
                raise RuntimeError('Woot!')

        flow = linear_flow.Flow('test')
        flow.add_many((
            MyTasks.run_one,
            MyTasks.run_fail
        ))
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            flow.run(None)
        self.assertEquals(MyTasks.values, ['one', 'fail'])
