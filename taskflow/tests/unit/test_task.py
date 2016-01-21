# -*- coding: utf-8 -*-

#    Copyright 2015 Hewlett-Packard Development Company, L.P.
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


class SeparateRevertTask(task.Task):
    def execute(self, execute_arg):
        pass

    def revert(self, revert_arg, result, flow_failures):
        pass


class SeparateRevertOptionalTask(task.Task):
    def execute(self, execute_arg=None):
        pass

    def revert(self, result, flow_failures, revert_arg=None):
        pass


class TaskTest(test.TestCase):

    def test_passed_name(self):
        my_task = MyTask(name='my name')
        self.assertEqual('my name', my_task.name)

    def test_generated_name(self):
        my_task = MyTask()
        self.assertEqual('%s.%s' % (__name__, 'MyTask'),
                         my_task.name)

    def test_task_str(self):
        my_task = MyTask(name='my')
        self.assertEqual('my==1.0', str(my_task))

    def test_task_repr(self):
        my_task = MyTask(name='my')
        self.assertEqual('<%s.MyTask my==1.0>' % __name__, repr(my_task))

    def test_no_provides(self):
        my_task = MyTask()
        self.assertEqual({}, my_task.save_as)

    def test_provides(self):
        my_task = MyTask(provides='food')
        self.assertEqual({'food': None}, my_task.save_as)

    def test_multi_provides(self):
        my_task = MyTask(provides=('food', 'water'))
        self.assertEqual({'food': 0, 'water': 1}, my_task.save_as)

    def test_unpack(self):
        my_task = MyTask(provides=('food',))
        self.assertEqual({'food': 0}, my_task.save_as)

    def test_bad_provides(self):
        self.assertRaisesRegexp(TypeError, '^Atom provides',
                                MyTask, provides=object())

    def test_requires_by_default(self):
        my_task = MyTask()
        expected = {
            'spam': 'spam',
            'eggs': 'eggs',
            'context': 'context'
        }
        self.assertEqual(expected,
                         my_task.rebind)
        self.assertEqual(set(['spam', 'eggs', 'context']),
                         my_task.requires)

    def test_requires_amended(self):
        my_task = MyTask(requires=('spam', 'eggs'))
        expected = {
            'spam': 'spam',
            'eggs': 'eggs',
            'context': 'context'
        }
        self.assertEqual(expected, my_task.rebind)

    def test_requires_explicit(self):
        my_task = MyTask(auto_extract=False,
                         requires=('spam', 'eggs', 'context'))
        expected = {
            'spam': 'spam',
            'eggs': 'eggs',
            'context': 'context'
        }
        self.assertEqual(expected, my_task.rebind)

    def test_requires_explicit_not_enough(self):
        self.assertRaisesRegexp(ValueError, '^Missing arguments',
                                MyTask,
                                auto_extract=False, requires=('spam', 'eggs'))

    def test_requires_ignores_optional(self):
        my_task = DefaultArgTask()
        self.assertEqual(set(['spam']), my_task.requires)
        self.assertEqual(set(['eggs']), my_task.optional)

    def test_requires_allows_optional(self):
        my_task = DefaultArgTask(requires=('spam', 'eggs'))
        self.assertEqual(set(['spam', 'eggs']), my_task.requires)
        self.assertEqual(set(), my_task.optional)

    def test_rebind_includes_optional(self):
        my_task = DefaultArgTask()
        expected = {
            'spam': 'spam',
            'eggs': 'eggs',
        }
        self.assertEqual(expected, my_task.rebind)

    def test_rebind_all_args(self):
        my_task = MyTask(rebind={'spam': 'a', 'eggs': 'b', 'context': 'c'})
        expected = {
            'spam': 'a',
            'eggs': 'b',
            'context': 'c'
        }
        self.assertEqual(expected, my_task.rebind)
        self.assertEqual(set(['a', 'b', 'c']),
                         my_task.requires)

    def test_rebind_partial(self):
        my_task = MyTask(rebind={'spam': 'a', 'eggs': 'b'})
        expected = {
            'spam': 'a',
            'eggs': 'b',
            'context': 'context'
        }
        self.assertEqual(expected, my_task.rebind)
        self.assertEqual(set(['a', 'b', 'context']),
                         my_task.requires)

    def test_rebind_unknown(self):
        self.assertRaisesRegexp(ValueError, '^Extra arguments',
                                MyTask, rebind={'foo': 'bar'})

    def test_rebind_unknown_kwargs(self):
        task = KwargsTask(rebind={'foo': 'bar'})
        expected = {
            'foo': 'bar',
            'spam': 'spam'
        }
        self.assertEqual(expected, task.rebind)

    def test_rebind_list_all(self):
        my_task = MyTask(rebind=('a', 'b', 'c'))
        expected = {
            'context': 'a',
            'spam': 'b',
            'eggs': 'c'
        }
        self.assertEqual(expected, my_task.rebind)
        self.assertEqual(set(['a', 'b', 'c']),
                         my_task.requires)

    def test_rebind_list_partial(self):
        my_task = MyTask(rebind=('a', 'b'))
        expected = {
            'context': 'a',
            'spam': 'b',
            'eggs': 'eggs'
        }
        self.assertEqual(expected, my_task.rebind)
        self.assertEqual(set(['a', 'b', 'eggs']),
                         my_task.requires)

    def test_rebind_list_more(self):
        self.assertRaisesRegexp(ValueError, '^Extra arguments',
                                MyTask, rebind=('a', 'b', 'c', 'd'))

    def test_rebind_list_more_kwargs(self):
        task = KwargsTask(rebind=('a', 'b', 'c'))
        expected = {
            'spam': 'a',
            'b': 'b',
            'c': 'c'
        }
        self.assertEqual(expected, task.rebind)
        self.assertEqual(set(['a', 'b', 'c']),
                         task.requires)

    def test_rebind_list_bad_value(self):
        self.assertRaisesRegexp(TypeError, '^Invalid rebind value',
                                MyTask, rebind=object())

    def test_default_provides(self):
        task = DefaultProvidesTask()
        self.assertEqual(set(['def']), task.provides)
        self.assertEqual({'def': None}, task.save_as)

    def test_default_provides_can_be_overridden(self):
        task = DefaultProvidesTask(provides=('spam', 'eggs'))
        self.assertEqual(set(['spam', 'eggs']), task.provides)
        self.assertEqual({'spam': 0, 'eggs': 1}, task.save_as)

    def test_update_progress_within_bounds(self):
        values = [0.0, 0.5, 1.0]
        result = []

        def progress_callback(event_type, details):
            result.append(details.pop('progress'))

        a_task = ProgressTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, progress_callback)
        a_task.execute(values)
        self.assertEqual(values, result)

    @mock.patch.object(task.LOG, 'warn')
    def test_update_progress_lower_bound(self, mocked_warn):
        result = []

        def progress_callback(event_type, details):
            result.append(details.pop('progress'))

        a_task = ProgressTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, progress_callback)
        a_task.execute([-1.0, -0.5, 0.0])
        self.assertEqual([0.0, 0.0, 0.0], result)
        self.assertEqual(2, mocked_warn.call_count)

    @mock.patch.object(task.LOG, 'warn')
    def test_update_progress_upper_bound(self, mocked_warn):
        result = []

        def progress_callback(event_type, details):
            result.append(details.pop('progress'))

        a_task = ProgressTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, progress_callback)
        a_task.execute([1.0, 1.5, 2.0])
        self.assertEqual([1.0, 1.0, 1.0], result)
        self.assertEqual(2, mocked_warn.call_count)

    @mock.patch.object(notifier.LOG, 'warn')
    def test_update_progress_handler_failure(self, mocked_warn):

        def progress_callback(*args, **kwargs):
            raise Exception('Woot!')

        a_task = ProgressTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, progress_callback)
        a_task.execute([0.5])
        self.assertEqual(1, mocked_warn.call_count)

    def test_register_handler_is_none(self):
        a_task = MyTask()
        self.assertRaises(ValueError, a_task.notifier.register,
                          task.EVENT_UPDATE_PROGRESS, None)
        self.assertEqual(0, len(a_task.notifier))

    def test_deregister_any_handler(self):
        a_task = MyTask()
        self.assertEqual(0, len(a_task.notifier))
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS,
                                 lambda event_type, details: None)
        self.assertEqual(1, len(a_task.notifier))
        a_task.notifier.deregister_event(task.EVENT_UPDATE_PROGRESS)
        self.assertEqual(0, len(a_task.notifier))

    def test_deregister_any_handler_empty_listeners(self):
        a_task = MyTask()
        self.assertEqual(0, len(a_task.notifier))
        self.assertFalse(a_task.notifier.deregister_event(
            task.EVENT_UPDATE_PROGRESS))
        self.assertEqual(0, len(a_task.notifier))

    def test_deregister_non_existent_listener(self):
        handler1 = lambda event_type, details: None
        handler2 = lambda event_type, details: None
        a_task = MyTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, handler1)
        self.assertEqual(1, len(list(a_task.notifier.listeners_iter())))
        a_task.notifier.deregister(task.EVENT_UPDATE_PROGRESS, handler2)
        self.assertEqual(1, len(list(a_task.notifier.listeners_iter())))
        a_task.notifier.deregister(task.EVENT_UPDATE_PROGRESS, handler1)
        self.assertEqual(0, len(list(a_task.notifier.listeners_iter())))

    def test_bind_not_callable(self):
        a_task = MyTask()
        self.assertRaises(ValueError, a_task.notifier.register,
                          task.EVENT_UPDATE_PROGRESS, 2)

    def test_copy_no_listeners(self):
        handler1 = lambda event_type, details: None
        a_task = MyTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, handler1)
        b_task = a_task.copy(retain_listeners=False)
        self.assertEqual(1, len(a_task.notifier))
        self.assertEqual(0, len(b_task.notifier))

    def test_copy_listeners(self):
        handler1 = lambda event_type, details: None
        handler2 = lambda event_type, details: None
        a_task = MyTask()
        a_task.notifier.register(task.EVENT_UPDATE_PROGRESS, handler1)
        b_task = a_task.copy()
        self.assertEqual(1, len(b_task.notifier))
        self.assertTrue(a_task.notifier.deregister_event(
            task.EVENT_UPDATE_PROGRESS))
        self.assertEqual(0, len(a_task.notifier))
        self.assertEqual(1, len(b_task.notifier))
        b_task.notifier.register(task.EVENT_UPDATE_PROGRESS, handler2)
        listeners = dict(list(b_task.notifier.listeners_iter()))
        self.assertEqual(2, len(listeners[task.EVENT_UPDATE_PROGRESS]))
        self.assertEqual(0, len(a_task.notifier))

    def test_separate_revert_args(self):
        task = SeparateRevertTask(rebind=('a',), revert_rebind=('b',))
        self.assertEqual({'execute_arg': 'a'}, task.rebind)
        self.assertEqual({'revert_arg': 'b'}, task.revert_rebind)
        self.assertEqual(set(['a', 'b']),
                         task.requires)

        task = SeparateRevertTask(requires='execute_arg',
                                  revert_requires='revert_arg')

        self.assertEqual({'execute_arg': 'execute_arg'}, task.rebind)
        self.assertEqual({'revert_arg': 'revert_arg'}, task.revert_rebind)
        self.assertEqual(set(['execute_arg', 'revert_arg']),
                         task.requires)

    def test_separate_revert_optional_args(self):
        task = SeparateRevertOptionalTask()
        self.assertEqual(set(['execute_arg']), task.optional)
        self.assertEqual(set(['revert_arg']), task.revert_optional)


class FunctorTaskTest(test.TestCase):

    def test_creation_with_version(self):
        version = (2, 0)
        f_task = task.FunctorTask(lambda: None, version=version)
        self.assertEqual(version, f_task.version)

    def test_execute_not_callable(self):
        self.assertRaises(ValueError, task.FunctorTask, 2)

    def test_revert_not_callable(self):
        self.assertRaises(ValueError, task.FunctorTask, lambda: None,
                          revert=2)


class ReduceFunctorTaskTest(test.TestCase):

    def test_invalid_functor(self):
        # Functor not callable
        self.assertRaises(ValueError, task.ReduceFunctorTask, 2, requires=5)

        # Functor takes no arguments
        self.assertRaises(ValueError, task.ReduceFunctorTask, lambda: None,
                          requires=5)

        # Functor takes too few arguments
        self.assertRaises(ValueError, task.ReduceFunctorTask, lambda x: None,
                          requires=5)

    def test_functor_invalid_requires(self):
        # Invalid type, requires is not iterable
        self.assertRaises(TypeError, task.ReduceFunctorTask,
                          lambda x, y: None, requires=1)

        # Too few elements in requires
        self.assertRaises(ValueError, task.ReduceFunctorTask,
                          lambda x, y: None, requires=[1])


class MapFunctorTaskTest(test.TestCase):

    def test_invalid_functor(self):
        # Functor not callable
        self.assertRaises(ValueError, task.MapFunctorTask, 2, requires=5)

        # Functor takes no arguments
        self.assertRaises(ValueError, task.MapFunctorTask, lambda: None,
                          requires=5)

        # Functor takes too many arguments
        self.assertRaises(ValueError, task.MapFunctorTask, lambda x, y: None,
                          requires=5)

    def test_functor_invalid_requires(self):
        # Invalid type, requires is not iterable
        self.assertRaises(TypeError, task.MapFunctorTask, lambda x: None,
                          requires=1)
