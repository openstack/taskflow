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


from taskflow import task
from taskflow import test


class MyTask(task.Task):
    def execute(self, context, spam, eggs):
        pass


class KwargsTask(task.Task):
    def execute(self, spam, **kwargs):
        pass


class TaskTestCase(test.TestCase):

    def test_passed_name(self):
        my_task = MyTask(name='my name')
        self.assertEquals(my_task.name, 'my name')

    def test_generated_name(self):
        my_task = MyTask()
        self.assertEquals(my_task.name,
                          '%s.%s' % (__name__, 'MyTask'))

    def test_no_provides(self):
        my_task = MyTask()
        self.assertEquals(my_task.provides, {})

    def test_provides(self):
        my_task = MyTask(provides='food')
        self.assertEquals(my_task.provides, {'food': None})

    def test_multi_provides(self):
        my_task = MyTask(provides=('food', 'water'))
        self.assertEquals(my_task.provides, {'food': 0, 'water': 1})

    def test_unpack(self):
        my_task = MyTask(provides=('food',))
        self.assertEquals(my_task.provides, {'food': 0})

    def test_bad_provides(self):
        with self.assertRaisesRegexp(TypeError, '^Task provides'):
            MyTask(provides=object())

    def test_requires_by_default(self):
        my_task = MyTask()
        self.assertEquals(my_task.requires, {
            'spam': 'spam',
            'eggs': 'eggs',
            'context': 'context'
        })

    def test_requires_amended(self):
        my_task = MyTask(requires=('spam', 'eggs'))
        self.assertEquals(my_task.requires, {
            'spam': 'spam',
            'eggs': 'eggs',
            'context': 'context'
        })

    def test_requires_explicit(self):
        my_task = MyTask(auto_extract=False,
                         requires=('spam', 'eggs', 'context'))
        self.assertEquals(my_task.requires, {
            'spam': 'spam',
            'eggs': 'eggs',
            'context': 'context'
        })

    def test_requires_explicit_not_enough(self):
        with self.assertRaisesRegexp(ValueError, '^Missing arguments'):
            MyTask(auto_extract=False, requires=('spam', 'eggs'))

    def test_rebind_all_args(self):
        my_task = MyTask(rebind={'spam': 'a', 'eggs': 'b', 'context': 'c'})
        self.assertEquals(my_task.requires, {
            'spam': 'a',
            'eggs': 'b',
            'context': 'c'
        })

    def test_rebind_partial(self):
        my_task = MyTask(rebind={'spam': 'a', 'eggs': 'b'})
        self.assertEquals(my_task.requires, {
            'spam': 'a',
            'eggs': 'b',
            'context': 'context'
        })

    def test_rebind_unknown(self):
        with self.assertRaisesRegexp(ValueError, '^Extra arguments'):
            MyTask(rebind={'foo': 'bar'})

    def test_rebind_unknown_kwargs(self):
        task = KwargsTask(rebind={'foo': 'bar'})
        self.assertEquals(task.requires, {
            'foo': 'bar',
            'spam': 'spam'
        })

    def test_rebind_list_all(self):
        my_task = MyTask(rebind=('a', 'b', 'c'))
        self.assertEquals(my_task.requires, {
            'context': 'a',
            'spam': 'b',
            'eggs': 'c'
        })

    def test_rebind_list_partial(self):
        my_task = MyTask(rebind=('a', 'b'))
        self.assertEquals(my_task.requires, {
            'context': 'a',
            'spam': 'b',
            'eggs': 'eggs'
        })

    def test_rebind_list_more(self):
        with self.assertRaisesRegexp(ValueError, '^Extra arguments'):
            MyTask(rebind=('a', 'b', 'c', 'd'))

    def test_rebind_list_more_kwargs(self):
        task = KwargsTask(rebind=('a', 'b', 'c'))
        self.assertEquals(task.requires, {
            'spam': 'a',
            'b': 'b',
            'c': 'c'
        })

    def test_rebind_list_bad_value(self):
        with self.assertRaisesRegexp(TypeError, '^Invalid rebind value:'):
            MyTask(rebind=object())
