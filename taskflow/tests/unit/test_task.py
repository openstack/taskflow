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
from taskflow.utils import reflection


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
        self.assertRaisesRegexp(TypeError, '^Invalid rebind value:',
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

        def progress_callback(task, event_data, progress):
            result.append(progress)

        task = ProgressTask()
        with task.autobind('update_progress', progress_callback):
            task.execute(values)
        self.assertEqual(result, values)

    @mock.patch.object(task.LOG, 'warn')
    def test_update_progress_lower_bound(self, mocked_warn):
        result = []

        def progress_callback(task, event_data, progress):
            result.append(progress)

        task = ProgressTask()
        with task.autobind('update_progress', progress_callback):
            task.execute([-1.0, -0.5, 0.0])
        self.assertEqual(result, [0.0, 0.0, 0.0])
        self.assertEqual(mocked_warn.call_count, 2)

    @mock.patch.object(task.LOG, 'warn')
    def test_update_progress_upper_bound(self, mocked_warn):
        result = []

        def progress_callback(task, event_data, progress):
            result.append(progress)

        task = ProgressTask()
        with task.autobind('update_progress', progress_callback):
            task.execute([1.0, 1.5, 2.0])
        self.assertEqual(result, [1.0, 1.0, 1.0])
        self.assertEqual(mocked_warn.call_count, 2)

    @mock.patch.object(task.LOG, 'warn')
    def test_update_progress_handler_failure(self, mocked_warn):
        def progress_callback(*args, **kwargs):
            raise Exception('Woot!')

        task = ProgressTask()
        with task.autobind('update_progress', progress_callback):
            task.execute([0.5])
        mocked_warn.assert_called_once_with(
            mock.ANY, reflection.get_callable_name(progress_callback),
            'update_progress', exc_info=mock.ANY)

    @mock.patch.object(task.LOG, 'warn')
    def test_autobind_non_existent_event(self, mocked_warn):
        event = 'test-event'
        handler = lambda: None
        task = MyTask()
        with task.autobind(event, handler):
            self.assertEqual(len(task._events_listeners), 0)
            mocked_warn.assert_called_once_with(
                mock.ANY, handler, event, task, exc_info=mock.ANY)

    def test_autobind_handler_is_none(self):
        task = MyTask()
        with task.autobind('update_progress', None):
            self.assertEqual(len(task._events_listeners), 0)

    def test_unbind_any_handler(self):
        task = MyTask()
        self.assertEqual(len(task._events_listeners), 0)
        task.bind('update_progress', lambda: None)
        self.assertEqual(len(task._events_listeners), 1)
        self.assertTrue(task.unbind('update_progress'))
        self.assertEqual(len(task._events_listeners), 0)

    def test_unbind_any_handler_empty_listeners(self):
        task = MyTask()
        self.assertEqual(len(task._events_listeners), 0)
        self.assertFalse(task.unbind('update_progress'))
        self.assertEqual(len(task._events_listeners), 0)

    def test_unbind_non_existent_listener(self):
        handler1 = lambda: None
        handler2 = lambda: None
        task = MyTask()
        task.bind('update_progress', handler1)
        self.assertEqual(len(task._events_listeners), 1)
        self.assertFalse(task.unbind('update_progress', handler2))
        self.assertEqual(len(task._events_listeners), 1)


class FunctorTaskTest(test.TestCase):

    def test_creation_with_version(self):
        version = (2, 0)
        f_task = task.FunctorTask(lambda: None, version=version)
        self.assertEqual(f_task.version, version)
