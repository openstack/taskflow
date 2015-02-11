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

from taskflow import task
from taskflow import test
from taskflow.test import mock
from taskflow.types import notifier


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


class ProgressTask(task.Task):
    def execute(self, values, **kwargs):
        for value in values:
            self.update_progress(value)


class TaskTest(test.TestCase):

    def test_passed_name(self):
        my_task = MyTask(name='my name')
        self.assertEqual(my_task.name, 'my name')

    def test_generated_name(self):
        my_task = MyTask()
        self.assertEqual(my_task.name,
                         '%s.%s' % (__name__, 'MyTask'))

    def test_task_str(self):
        my_task = MyTask(name='my')
        self.assertEqual(str(my_task), 'my==1.0')

    def test_task_repr(self):
        my_task = MyTask(name='my')
        self.assertEqual(repr(my_task), '<%s.MyTask my==1.0>' % __name__)

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
        self.assertRaisesRegexp(TypeError, '^Atom provides',
                                MyTask, provides=object())

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
        self.assertRaisesRegexp(ValueError, '^Missing arguments',
                                MyTask,
                                auto_extract=False, requires=('spam', 'eggs'))

    def test_requires_ignores_optional(self):
        my_task = DefaultArgTask()
        self.assertEqual(my_task.requires, set(['spam']))
        self.assertEqual(my_task.optional, set(['eggs']))

    def test_requires_allows_optional(self):
        my_task = DefaultArgTask(requires=('spam', 'eggs'))
        self.assertEqual(my_task.requires, set(['spam', 'eggs']))
        self.assertEqual(my_task.optional, set())

    def test_rebind_includes_optional(self):
        my_task = DefaultArgTask()
        self.assertEqual(my_task.rebind, {
            'spam': 'spam',
            'eggs': 'eggs',
        })

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
        self.assertRaisesRegexp(ValueError, '^Extra arguments',
                                MyTask, rebind={'foo': 'bar'})

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
        self.assertRaisesRegexp(ValueError, '^Extra arguments',
                                MyTask, rebind=('a', 'b', 'c', 'd'))

    def test_rebind_list_more_kwargs(self):
        task = KwargsTask(rebind=('a', 'b', 'c'))
        self.assertEqual(task.rebind, {
            'spam': 'a',
            'b': 'b',
            'c': 'c'
        })

    def test_rebind_list_bad_value(self):
        self.assertRaisesRegexp(TypeError, '^Invalid rebind value',
                                MyTask, rebind=object())

    def test_default_provides(self):
        task = DefaultProvidesTask()
        self.assertEqual(task.provides, set(['def']))
        self.assertEqual(task.save_as, {'def': None})

    def test_default_provides_can_be_overridden(self):
        task = DefaultProvidesTask(provides=('spam', 'eggs'))
        self.assertEqual(task.provides, set(['spam', 'eggs']))
        self.assertEqual(task.save_as, {'spam': 0, 'eggs': 1})

    def test_update_progress_within_bounds(self):
        values = [0.0, 0.5, 1.0]
        result = []

        def progress_callback(event_type, details):
            result.append(details.pop('progress'))

        a_task = ProgressTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, progress_callback)
        a_task.execute(values)
        self.assertEqual(result, values)

    @mock.patch.object(task.LOG, 'warn')
    def test_update_progress_lower_bound(self, mocked_warn):
        result = []

        def progress_callback(event_type, details):
            result.append(details.pop('progress'))

        a_task = ProgressTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, progress_callback)
        a_task.execute([-1.0, -0.5, 0.0])
        self.assertEqual(result, [0.0, 0.0, 0.0])
        self.assertEqual(mocked_warn.call_count, 2)

    @mock.patch.object(task.LOG, 'warn')
    def test_update_progress_upper_bound(self, mocked_warn):
        result = []

        def progress_callback(event_type, details):
            result.append(details.pop('progress'))

        a_task = ProgressTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, progress_callback)
        a_task.execute([1.0, 1.5, 2.0])
        self.assertEqual(result, [1.0, 1.0, 1.0])
        self.assertEqual(mocked_warn.call_count, 2)

    @mock.patch.object(notifier.LOG, 'warn')
    def test_update_progress_handler_failure(self, mocked_warn):

        def progress_callback(*args, **kwargs):
            raise Exception('Woot!')

        a_task = ProgressTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, progress_callback)
        a_task.execute([0.5])
        mocked_warn.assert_called_once()

    def test_register_handler_is_none(self):
        a_task = MyTask()
        self.assertRaises(ValueError, a_task.notifier.register,
                          task.EVENT_UPDATE_PROGRESS, None)
        self.assertEqual(len(a_task.notifier), 0)

    def test_deregister_any_handler(self):
        a_task = MyTask()
        self.assertEqual(len(a_task.notifier), 0)
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS,
                                 lambda event_type, details: None)
        self.assertEqual(len(a_task.notifier), 1)
        a_task.notifier.deregister_event(task.EVENT_UPDATE_PROGRESS)
        self.assertEqual(len(a_task.notifier), 0)

    def test_deregister_any_handler_empty_listeners(self):
        a_task = MyTask()
        self.assertEqual(len(a_task.notifier), 0)
        self.assertFalse(a_task.notifier.deregister_event(
            task.EVENT_UPDATE_PROGRESS))
        self.assertEqual(len(a_task.notifier), 0)

    def test_deregister_non_existent_listener(self):
        handler1 = lambda event_type, details: None
        handler2 = lambda event_type, details: None
        a_task = MyTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, handler1)
        self.assertEqual(len(list(a_task.notifier.listeners_iter())), 1)
        a_task.notifier.deregister(task.EVENT_UPDATE_PROGRESS, handler2)
        self.assertEqual(len(list(a_task.notifier.listeners_iter())), 1)
        a_task.notifier.deregister(task.EVENT_UPDATE_PROGRESS, handler1)
        self.assertEqual(len(list(a_task.notifier.listeners_iter())), 0)

    def test_bind_not_callable(self):
        a_task = MyTask()
        self.assertRaises(ValueError, a_task.notifier.register,
                          task.EVENT_UPDATE_PROGRESS, 2)

    def test_copy_no_listeners(self):
        handler1 = lambda event_type, details: None
        a_task = MyTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, handler1)
        b_task = a_task.copy(retain_listeners=False)
        self.assertEqual(len(a_task.notifier), 1)
        self.assertEqual(len(b_task.notifier), 0)

    def test_copy_listeners(self):
        handler1 = lambda event_type, details: None
        handler2 = lambda event_type, details: None
        a_task = MyTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, handler1)
        b_task = a_task.copy()
        self.assertEqual(len(b_task.notifier), 1)
        self.assertTrue(a_task.notifier.deregister_event(
            task.EVENT_UPDATE_PROGRESS))
        self.assertEqual(len(a_task.notifier), 0)
        self.assertEqual(len(b_task.notifier), 1)
        b_task.notifier.register(task.EVENT_UPDATE_PROGRESS, handler2)
        listeners = dict(list(b_task.notifier.listeners_iter()))
        self.assertEqual(len(listeners[task.EVENT_UPDATE_PROGRESS]), 2)
        self.assertEqual(len(a_task.notifier), 0)


class FunctorTaskTest(test.TestCase):

    def test_creation_with_version(self):
        version = (2, 0)
        f_task = task.FunctorTask(lambda: None, version=version)
        self.assertEqual(f_task.version, version)

    def test_execute_not_callable(self):
        self.assertRaises(ValueError, task.FunctorTask, 2)

    def test_revert_not_callable(self):
        self.assertRaises(ValueError, task.FunctorTask, lambda: None,
                          revert=2)
