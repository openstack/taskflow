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


class DefaultArgTask(task.Task):
    def execute(self, spam, eggs=()):
        pass


class DefaultProvidesTask(task.Task):
    default_provides = 'def'

    def execute(self):
        return None


class TaskTestCase(test.TestCase):

    def test_passed_name(self):
        my_task = MyTask(name='my name')
        self.assertEqual(my_task.name, 'my name')

    def test_generated_name(self):
        my_task = MyTask()
        self.assertEqual(my_task.name,
                         '%s.%s' % (__name__, 'MyTask'))

    def test_no_provides(self):
        my_task = MyTask()
        self.assertEqual(my_task.save_as, {})

    def test_provides(self):
        my_task = MyTask(provides='food')
        self.assertEqual(my_task.save_as, {'food': None})

    def test_multi_provides(self):
        my_task = MyTask(provides=('food', 'water'))
        self.assertEqual(my_task.save_as, {'food': 0, 'water': 1})

    def test_unpack(self):
        my_task = MyTask(provides=('food',))
        self.assertEqual(my_task.save_as, {'food': 0})

    def test_bad_provides(self):
        with self.assertRaisesRegexp(TypeError, '^Task provides'):
            MyTask(provides=object())

    def test_requires_by_default(self):
        my_task = MyTask()
        self.assertEqual(my_task.rebind, {
            'spam': 'spam',
            'eggs': 'eggs',
            'context': 'context'
        })

    def test_requires_amended(self):
        my_task = MyTask(requires=('spam', 'eggs'))
        self.assertEqual(my_task.rebind, {
            'spam': 'spam',
            'eggs': 'eggs',
            'context': 'context'
        })

    def test_requires_explicit(self):
        my_task = MyTask(auto_extract=False,
                         requires=('spam', 'eggs', 'context'))
        self.assertEqual(my_task.rebind, {
            'spam': 'spam',
            'eggs': 'eggs',
            'context': 'context'
        })

    def test_requires_explicit_not_enough(self):
        with self.assertRaisesRegexp(ValueError, '^Missing arguments'):
            MyTask(auto_extract=False, requires=('spam', 'eggs'))

    def test_requires_ignores_optional(self):
        my_task = DefaultArgTask()
        self.assertEqual(my_task.requires, set(['spam']))

    def test_requires_allows_optional(self):
        my_task = DefaultArgTask(requires=('spam', 'eggs'))
        self.assertEqual(my_task.requires, set(['spam', 'eggs']))

    def test_rebind_all_args(self):
        my_task = MyTask(rebind={'spam': 'a', 'eggs': 'b', 'context': 'c'})
        self.assertEqual(my_task.rebind, {
            'spam': 'a',
            'eggs': 'b',
            'context': 'c'
        })

    def test_rebind_partial(self):
        my_task = MyTask(rebind={'spam': 'a', 'eggs': 'b'})
        self.assertEqual(my_task.rebind, {
            'spam': 'a',
            'eggs': 'b',
            'context': 'context'
        })

    def test_rebind_unknown(self):
        with self.assertRaisesRegexp(ValueError, '^Extra arguments'):
            MyTask(rebind={'foo': 'bar'})

    def test_rebind_unknown_kwargs(self):
        task = KwargsTask(rebind={'foo': 'bar'})
        self.assertEqual(task.rebind, {
            'foo': 'bar',
            'spam': 'spam'
        })

    def test_rebind_list_all(self):
        my_task = MyTask(rebind=('a', 'b', 'c'))
        self.assertEqual(my_task.rebind, {
            'context': 'a',
            'spam': 'b',
            'eggs': 'c'
        })

    def test_rebind_list_partial(self):
        my_task = MyTask(rebind=('a', 'b'))
        self.assertEqual(my_task.rebind, {
            'context': 'a',
            'spam': 'b',
            'eggs': 'eggs'
        })

    def test_rebind_list_more(self):
        with self.assertRaisesRegexp(ValueError, '^Extra arguments'):
            MyTask(rebind=('a', 'b', 'c', 'd'))

    def test_rebind_list_more_kwargs(self):
        task = KwargsTask(rebind=('a', 'b', 'c'))
        self.assertEqual(task.rebind, {
            'spam': 'a',
            'b': 'b',
            'c': 'c'
        })

    def test_rebind_list_bad_value(self):
        with self.assertRaisesRegexp(TypeError, '^Invalid rebind value:'):
            MyTask(rebind=object())

    def test_default_provides(self):
        task = DefaultProvidesTask()
        self.assertEqual(task.provides, set(['def']))
        self.assertEqual(task.save_as, {'def': None})

    def test_default_provides_can_be_overridden(self):
        task = DefaultProvidesTask(provides=('spam', 'eggs'))
        self.assertEqual(task.provides, set(['spam', 'eggs']))
        self.assertEqual(task.save_as, {'spam': 0, 'eggs': 1})
