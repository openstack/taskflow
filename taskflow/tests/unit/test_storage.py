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

import contextlib
import threading

import mock

from taskflow import exceptions
from taskflow.openstack.common import uuidutils
from taskflow.persistence.backends import impl_memory
from taskflow.persistence import logbook
from taskflow import states
from taskflow import storage
from taskflow import test
from taskflow.utils import misc
from taskflow.utils import persistence_utils as p_utils


class StorageTest(test.TestCase):
    def setUp(self):
        super(StorageTest, self).setUp()
        self.backend = impl_memory.MemoryBackend(conf={})
        self.thread_count = 50

    def _run_many_threads(self, threads):
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def _get_storage(self, threaded=False):
        _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        if threaded:
            return storage.MultiThreadedStorage(backend=self.backend,
                                                flow_detail=flow_detail)
        else:
            return storage.SingleThreadedStorage(backend=self.backend,
                                                 flow_detail=flow_detail)

    def tearDown(self):
        super(StorageTest, self).tearDown()
        with contextlib.closing(self.backend) as be:
            with contextlib.closing(be.get_connection()) as conn:
                conn.clear_all()

    def test_non_saving_storage(self):
        _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        s = storage.SingleThreadedStorage(flow_detail=flow_detail)
        s.ensure_task('my_task')
        self.assertTrue(
            uuidutils.is_uuid_like(s.get_task_uuid('my_task')))

    def test_flow_name_and_uuid(self):
        fd = logbook.FlowDetail(name='test-fd', uuid='aaaa')
        s = storage.SingleThreadedStorage(flow_detail=fd)
        self.assertEqual(s.flow_name, 'test-fd')
        self.assertEqual(s.flow_uuid, 'aaaa')

    def test_ensure_task(self):
        s = self._get_storage()
        s.ensure_task('my task')
        self.assertEqual(s.get_task_state('my task'), states.PENDING)
        self.assertTrue(
            uuidutils.is_uuid_like(s.get_task_uuid('my task')))

    def test_get_tasks_states(self):
        s = self._get_storage()
        s.ensure_task('my task')
        s.ensure_task('my task2')
        s.save('my task', 'foo')
        expected = {
            'my task': states.SUCCESS,
            'my task2': states.PENDING,
        }
        self.assertEqual(s.get_tasks_states(['my task', 'my task2']), expected)

    def test_ensure_task_fd(self):
        _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        s = storage.SingleThreadedStorage(backend=self.backend,
                                          flow_detail=flow_detail)
        s.ensure_task('my task', '3.11')
        td = flow_detail.find(s.get_task_uuid('my task'))
        self.assertIsNotNone(td)
        self.assertEqual(td.name, 'my task')
        self.assertEqual(td.version, '3.11')
        self.assertEqual(td.state, states.PENDING)

    def test_get_without_save(self):
        _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        td = logbook.TaskDetail(name='my_task', uuid='42')
        flow_detail.add(td)

        s = storage.SingleThreadedStorage(backend=self.backend,
                                          flow_detail=flow_detail)
        self.assertEqual('42', s.get_task_uuid('my_task'))

    def test_ensure_existing_task(self):
        _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        td = logbook.TaskDetail(name='my_task', uuid='42')
        flow_detail.add(td)

        s = storage.SingleThreadedStorage(backend=self.backend,
                                          flow_detail=flow_detail)
        s.ensure_task('my_task')
        self.assertEqual('42', s.get_task_uuid('my_task'))

    def test_save_and_get(self):
        s = self._get_storage()
        s.ensure_task('my task')
        s.save('my task', 5)
        self.assertEqual(s.get('my task'), 5)
        self.assertEqual(s.fetch_all(), {})
        self.assertEqual(s.get_task_state('my task'), states.SUCCESS)

    def test_save_and_get_other_state(self):
        s = self._get_storage()
        s.ensure_task('my task')
        s.save('my task', 5, states.FAILURE)
        self.assertEqual(s.get('my task'), 5)
        self.assertEqual(s.get_task_state('my task'), states.FAILURE)

    def test_save_and_get_failure(self):
        fail = misc.Failure(exc_info=(RuntimeError, RuntimeError(), None))
        s = self._get_storage()
        s.ensure_task('my task')
        s.save('my task', fail, states.FAILURE)
        self.assertEqual(s.get('my task'), fail)
        self.assertEqual(s.get_task_state('my task'), states.FAILURE)
        self.assertIs(s.has_failures(), True)
        self.assertEqual(s.get_failures(), {'my task': fail})

    def test_get_failure_from_reverted_task(self):
        fail = misc.Failure(exc_info=(RuntimeError, RuntimeError(), None))
        s = self._get_storage()
        s.ensure_task('my task')
        s.save('my task', fail, states.FAILURE)

        s.set_task_state('my task', states.REVERTING)
        self.assertEqual(s.get('my task'), fail)

        s.set_task_state('my task', states.REVERTED)
        self.assertEqual(s.get('my task'), fail)

    def test_get_failure_after_reload(self):
        fail = misc.Failure(exc_info=(RuntimeError, RuntimeError(), None))
        s = self._get_storage()
        s.ensure_task('my task')
        s.save('my task', fail, states.FAILURE)

        s2 = storage.SingleThreadedStorage(backend=self.backend,
                                           flow_detail=s._flowdetail)
        self.assertIs(s2.has_failures(), True)
        self.assertEqual(s2.get_failures(), {'my task': fail})
        self.assertEqual(s2.get('my task'), fail)
        self.assertEqual(s2.get_task_state('my task'), states.FAILURE)

    def test_get_non_existing_var(self):
        s = self._get_storage()
        s.ensure_task('my task')
        self.assertRaises(exceptions.NotFound, s.get, 'my task')

    def test_reset(self):
        s = self._get_storage()
        s.ensure_task('my task')
        s.save('my task', 5)
        s.reset('my task')
        self.assertEqual(s.get_task_state('my task'), states.PENDING)
        self.assertRaises(exceptions.NotFound, s.get, 'my task')

    def test_reset_unknown_task(self):
        s = self._get_storage()
        s.ensure_task('my task')
        self.assertEqual(s.reset('my task'), None)

    def test_reset_tasks(self):
        s = self._get_storage()
        s.ensure_task('my task')
        s.save('my task', 5)
        s.ensure_task('my other task')
        s.save('my other task', 7)

        s.reset_tasks()

        self.assertEqual(s.get_task_state('my task'), states.PENDING)
        self.assertRaises(exceptions.NotFound, s.get, 'my task')
        self.assertEqual(s.get_task_state('my other task'), states.PENDING)
        self.assertRaises(exceptions.NotFound, s.get, 'my other task')

    def test_reset_tasks_does_not_breaks_inject(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})

        # NOTE(imelnikov): injecting is implemented as special task
        # so resetting tasks may break it if implemented incorrectly.
        s.reset_tasks()

        self.assertEqual(s.fetch('spam'), 'eggs')
        self.assertEqual(s.fetch_all(), {
            'foo': 'bar',
            'spam': 'eggs',
        })

    def test_fetch_by_name(self):
        s = self._get_storage()
        name = 'my result'
        s.ensure_task('my task', '1.0', {name: None})
        s.save('my task', 5)
        self.assertEqual(s.fetch(name), 5)
        self.assertEqual(s.fetch_all(), {name: 5})

    def test_fetch_unknown_name(self):
        s = self._get_storage()
        self.assertRaisesRegexp(exceptions.NotFound,
                                "^Name 'xxx' is not mapped",
                                s.fetch, 'xxx')

    def test_default_task_progress(self):
        s = self._get_storage()
        s.ensure_task('my task')
        self.assertEqual(s.get_task_progress('my task'), 0.0)
        self.assertEqual(s.get_task_progress_details('my task'), None)

    def test_task_progress(self):
        s = self._get_storage()
        s.ensure_task('my task')

        s.set_task_progress('my task', 0.5, {'test_data': 11})
        self.assertEqual(s.get_task_progress('my task'), 0.5)
        self.assertEqual(s.get_task_progress_details('my task'), {
            'at_progress': 0.5,
            'details': {'test_data': 11}
        })

        s.set_task_progress('my task', 0.7, {'test_data': 17})
        self.assertEqual(s.get_task_progress('my task'), 0.7)
        self.assertEqual(s.get_task_progress_details('my task'), {
            'at_progress': 0.7,
            'details': {'test_data': 17}
        })

        s.set_task_progress('my task', 0.99)
        self.assertEqual(s.get_task_progress('my task'), 0.99)
        self.assertEqual(s.get_task_progress_details('my task'), {
            'at_progress': 0.7,
            'details': {'test_data': 17}
        })

    def test_task_progress_erase(self):
        s = self._get_storage()
        s.ensure_task('my task')

        s.set_task_progress('my task', 0.8, {})
        self.assertEqual(s.get_task_progress('my task'), 0.8)
        self.assertEqual(s.get_task_progress_details('my task'), None)

    def test_fetch_result_not_ready(self):
        s = self._get_storage()
        name = 'my result'
        s.ensure_task('my task', result_mapping={name: None})
        self.assertRaises(exceptions.NotFound, s.get, name)
        self.assertEqual(s.fetch_all(), {})

    def test_save_multiple_results(self):
        s = self._get_storage()
        result_mapping = {'foo': 0, 'bar': 1, 'whole': None}
        s.ensure_task('my task', result_mapping=result_mapping)
        s.save('my task', ('spam', 'eggs'))
        self.assertEqual(s.fetch_all(), {
            'foo': 'spam',
            'bar': 'eggs',
            'whole': ('spam', 'eggs')
        })

    def test_mapping_none(self):
        s = self._get_storage()
        s.ensure_task('my task')
        s.save('my task', 5)
        self.assertEqual(s.fetch_all(), {})

    def test_inject(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        self.assertEqual(s.fetch('spam'), 'eggs')
        self.assertEqual(s.fetch_all(), {
            'foo': 'bar',
            'spam': 'eggs',
        })

    def test_inject_twice(self):
        s = self._get_storage()
        s.inject({'foo': 'bar'})
        self.assertEqual(s.fetch_all(), {'foo': 'bar'})
        s.inject({'spam': 'eggs'})
        self.assertEqual(s.fetch_all(), {
            'foo': 'bar',
            'spam': 'eggs',
        })

    def test_inject_resumed(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        # verify it's there
        self.assertEqual(s.fetch_all(), {
            'foo': 'bar',
            'spam': 'eggs',
        })
        # imagine we are resuming, so we need to make new
        # storage from same flow details
        s2 = storage.SingleThreadedStorage(s._flowdetail, backend=self.backend)
        # injected data should still be there:
        self.assertEqual(s2.fetch_all(), {
            'foo': 'bar',
            'spam': 'eggs',
        })

    def test_many_thread_ensure_same_task(self):
        s = self._get_storage(threaded=True)

        def ensure_my_task():
            s.ensure_task('my_task', result_mapping={})

        threads = []
        for i in range(0, self.thread_count):
            threads.append(threading.Thread(target=ensure_my_task))
        self._run_many_threads(threads)

        # Only one task should have been made, no more.
        self.assertEqual(1, len(s._flowdetail))

    def test_many_thread_one_reset(self):
        s = self._get_storage(threaded=True)
        s.ensure_task('a')
        s.set_task_state('a', states.SUSPENDED)
        s.ensure_task('b')
        s.set_task_state('b', states.SUSPENDED)

        results = []
        result_lock = threading.Lock()

        def reset_all():
            r = s.reset_tasks()
            with result_lock:
                results.append(r)

        threads = []
        for i in range(0, self.thread_count):
            threads.append(threading.Thread(target=reset_all))

        self._run_many_threads(threads)

        # Only one thread should have actually reset (not anymore)
        results = [r for r in results if len(r)]
        self.assertEqual(1, len(results))
        self.assertEqual(['a', 'b'], sorted([a[0] for a in results[0]]))

    def test_many_thread_inject(self):
        s = self._get_storage(threaded=True)

        def inject_values(values):
            s.inject(values)

        threads = []
        for i in range(0, self.thread_count):
            values = {
                str(i): str(i),
            }
            threads.append(threading.Thread(target=inject_values,
                                            args=[values]))

        self._run_many_threads(threads)
        self.assertEqual(self.thread_count, len(s.fetch_all()))
        self.assertEqual(1, len(s._flowdetail))

    def test_fetch_meapped_args(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        self.assertEqual(s.fetch_mapped_args({'viking': 'spam'}),
                         {'viking': 'eggs'})

    def test_fetch_not_found_args(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        self.assertRaises(exceptions.NotFound,
                          s.fetch_mapped_args, {'viking': 'helmet'})

    def test_set_and_get_task_state(self):
        s = self._get_storage()
        state = states.PENDING
        s.ensure_task('my task')
        s.set_task_state('my task', state)
        self.assertEqual(s.get_task_state('my task'), state)

    def test_get_state_of_unknown_task(self):
        s = self._get_storage()
        self.assertRaisesRegexp(exceptions.NotFound, '^Unknown',
                                s.get_task_state, 'my task')

    def test_task_by_name(self):
        s = self._get_storage()
        s.ensure_task('my task')
        self.assertTrue(
            uuidutils.is_uuid_like(s.get_task_uuid('my task')))

    def test_unknown_task_by_name(self):
        s = self._get_storage()
        self.assertRaisesRegexp(exceptions.NotFound,
                                '^Unknown task name:',
                                s.get_task_uuid, '42')

    def test_initial_flow_state(self):
        s = self._get_storage()
        self.assertEqual(s.get_flow_state(), states.PENDING)

    def test_get_flow_state(self):
        _lb, fd = p_utils.temporary_flow_detail(backend=self.backend)
        fd.state = states.FAILURE
        with contextlib.closing(self.backend.get_connection()) as conn:
            fd.update(conn.update_flow_details(fd))
        s = storage.SingleThreadedStorage(flow_detail=fd, backend=self.backend)
        self.assertEqual(s.get_flow_state(), states.FAILURE)

    def test_set_and_get_flow_state(self):
        s = self._get_storage()
        s.set_flow_state(states.SUCCESS)
        self.assertEqual(s.get_flow_state(), states.SUCCESS)

    @mock.patch.object(storage.LOG, 'warning')
    def test_result_is_checked(self, mocked_warning):
        s = self._get_storage()
        s.ensure_task('my task', result_mapping={'result': 'key'})
        s.save('my task', {})
        mocked_warning.assert_called_once_with(
            mock.ANY, 'my task', 'key', 'result')
        self.assertRaisesRegexp(exceptions.NotFound,
                                '^Unable to find result', s.fetch, 'result')

    @mock.patch.object(storage.LOG, 'warning')
    def test_empty_result_is_checked(self, mocked_warning):
        s = self._get_storage()
        s.ensure_task('my task', result_mapping={'a': 0})
        s.save('my task', ())
        mocked_warning.assert_called_once_with(
            mock.ANY, 'my task', 0, 'a')
        self.assertRaisesRegexp(exceptions.NotFound,
                                '^Unable to find result', s.fetch, 'a')

    @mock.patch.object(storage.LOG, 'warning')
    def test_short_result_is_checked(self, mocked_warning):
        s = self._get_storage()
        s.ensure_task('my task', result_mapping={'a': 0, 'b': 1})
        s.save('my task', ['result'])
        mocked_warning.assert_called_once_with(
            mock.ANY, 'my task', 1, 'b')
        self.assertEqual(s.fetch('a'), 'result')
        self.assertRaisesRegexp(exceptions.NotFound,
                                '^Unable to find result', s.fetch, 'b')

    @mock.patch.object(storage.LOG, 'warning')
    def test_multiple_providers_are_checked(self, mocked_warning):
        s = self._get_storage()
        s.ensure_task('my task', result_mapping={'result': 'key'})
        self.assertEqual(mocked_warning.mock_calls, [])
        s.ensure_task('my other task', result_mapping={'result': 'key'})
        mocked_warning.assert_called_once_with(
            mock.ANY, 'result')

    @mock.patch.object(storage.LOG, 'warning')
    def test_multiple_providers_with_inject_are_checked(self, mocked_warning):
        s = self._get_storage()
        s.inject({'result': 'DONE'})
        self.assertEqual(mocked_warning.mock_calls, [])
        s.ensure_task('my other task', result_mapping={'result': 'key'})
        mocked_warning.assert_called_once_with(mock.ANY, 'result')
