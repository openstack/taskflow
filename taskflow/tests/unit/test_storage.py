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

import contextlib
import threading

from oslo_utils import uuidutils

from taskflow import exceptions
from taskflow.persistence import backends
from taskflow.persistence import models
from taskflow import states
from taskflow import storage
from taskflow import test
from taskflow.tests import utils as test_utils
from taskflow.types import failure
from taskflow.utils import persistence_utils as p_utils


class StorageTestMixin(object):
    def setUp(self):
        super(StorageTestMixin, self).setUp()
        self.backend = None
        self.thread_count = 50

    def tearDown(self):
        with contextlib.closing(self.backend) as be:
            with contextlib.closing(be.get_connection()) as conn:
                conn.clear_all()
        super(StorageTestMixin, self).tearDown()

    @staticmethod
    def _run_many_threads(threads):
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def _get_storage(self, flow_detail=None):
        if flow_detail is None:
            _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        return storage.Storage(flow_detail=flow_detail, backend=self.backend)

    def test_non_saving_storage(self):
        _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        s = storage.Storage(flow_detail=flow_detail)
        s.ensure_atom(test_utils.NoopTask('my_task'))
        self.assertTrue(uuidutils.is_uuid_like(s.get_atom_uuid('my_task')))

    def test_flow_name_uuid_and_meta(self):
        flow_detail = models.FlowDetail(name='test-fd', uuid='aaaa')
        flow_detail.meta = {'a': 1}
        s = self._get_storage(flow_detail)
        self.assertEqual('test-fd', s.flow_name)
        self.assertEqual('aaaa', s.flow_uuid)
        self.assertEqual({'a': 1}, s.flow_meta)

    def test_ensure_task(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        self.assertEqual(states.PENDING, s.get_atom_state('my task'))
        self.assertTrue(uuidutils.is_uuid_like(s.get_atom_uuid('my task')))

    def test_get_tasks_states(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        s.ensure_atom(test_utils.NoopTask('my task2'))
        s.save('my task', 'foo')
        expected = {
            'my task': (states.SUCCESS, states.EXECUTE),
            'my task2': (states.PENDING, states.EXECUTE),
        }
        self.assertEqual(expected, s.get_atoms_states(['my task', 'my task2']))

    def test_ensure_task_flow_detail(self):
        _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        s = self._get_storage(flow_detail)
        t = test_utils.NoopTask('my task')
        t.version = (3, 11)
        s.ensure_atom(t)
        td = flow_detail.find(s.get_atom_uuid('my task'))
        self.assertIsNotNone(td)
        self.assertEqual('my task', td.name)
        self.assertEqual('3.11', td.version)
        self.assertEqual(states.PENDING, td.state)

    def test_get_without_save(self):
        _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        td = models.TaskDetail(name='my_task', uuid='42')
        flow_detail.add(td)
        s = self._get_storage(flow_detail)
        self.assertEqual('42', s.get_atom_uuid('my_task'))

    def test_ensure_existing_task(self):
        _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        td = models.TaskDetail(name='my_task', uuid='42')
        flow_detail.add(td)
        s = self._get_storage(flow_detail)
        s.ensure_atom(test_utils.NoopTask('my_task'))
        self.assertEqual('42', s.get_atom_uuid('my_task'))

    def test_save_and_get(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        s.save('my task', 5)
        self.assertEqual(5, s.get('my task'))
        self.assertEqual({}, s.fetch_all())
        self.assertEqual(states.SUCCESS, s.get_atom_state('my task'))

    def test_save_and_get_cached_failure(self):
        a_failure = failure.Failure.from_exception(RuntimeError('Woot!'))
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        s.save('my task', a_failure, states.FAILURE)
        self.assertEqual(a_failure, s.get('my task'))
        self.assertEqual(states.FAILURE, s.get_atom_state('my task'))
        self.assertTrue(s.has_failures())
        self.assertEqual({'my task': a_failure}, s.get_failures())

    def test_save_and_get_non_cached_failure(self):
        a_failure = failure.Failure.from_exception(RuntimeError('Woot!'))
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        s.save('my task', a_failure, states.FAILURE)
        self.assertEqual(a_failure, s.get('my task'))
        s._failures['my task'] = {}
        self.assertTrue(a_failure.matches(s.get('my task')))

    def test_get_failure_from_reverted_task(self):
        a_failure = failure.Failure.from_exception(RuntimeError('Woot!'))

        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        s.save('my task', a_failure, states.FAILURE)

        s.set_atom_state('my task', states.REVERTING)
        self.assertEqual(a_failure, s.get('my task'))

        s.set_atom_state('my task', states.REVERTED)
        self.assertEqual(a_failure, s.get('my task'))

    def test_get_failure_after_reload(self):
        a_failure = failure.Failure.from_exception(RuntimeError('Woot!'))
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        s.save('my task', a_failure, states.FAILURE)
        s2 = self._get_storage(s._flowdetail)
        self.assertTrue(s2.has_failures())
        self.assertEqual(1, len(s2.get_failures()))
        self.assertTrue(a_failure.matches(s2.get('my task')))
        self.assertEqual(states.FAILURE, s2.get_atom_state('my task'))

    def test_get_non_existing_var(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        self.assertRaises(exceptions.NotFound, s.get, 'my task')

    def test_reset(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        s.save('my task', 5)
        s.reset('my task')
        self.assertEqual(states.PENDING, s.get_atom_state('my task'))
        self.assertRaises(exceptions.NotFound, s.get, 'my task')

    def test_reset_unknown_task(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        self.assertIsNone(s.reset('my task'))

    def test_fetch_by_name(self):
        s = self._get_storage()
        name = 'my result'
        s.ensure_atom(test_utils.NoopTask('my task', provides=name))
        s.save('my task', 5)
        self.assertEqual(5, s.fetch(name))
        self.assertEqual({name: 5}, s.fetch_all())

    def test_fetch_unknown_name(self):
        s = self._get_storage()
        self.assertRaisesRegex(exceptions.NotFound,
                               "^Name 'xxx' is not mapped",
                               s.fetch, 'xxx')

    def test_flow_metadata_update(self):
        s = self._get_storage()
        update_with = {'test_data': True}
        s.update_flow_metadata(update_with)
        self.assertTrue(s._flowdetail.meta['test_data'])

    def test_task_metadata_update_with_none(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        s.update_atom_metadata('my task', None)
        self.assertEqual(0.0, s.get_task_progress('my task'))
        s.set_task_progress('my task', 0.5)
        self.assertEqual(0.5, s.get_task_progress('my task'))
        s.update_atom_metadata('my task', None)
        self.assertEqual(0.5, s.get_task_progress('my task'))

    def test_default_task_progress(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        self.assertEqual(0.0, s.get_task_progress('my task'))
        self.assertIsNone(s.get_task_progress_details('my task'))

    def test_task_progress(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))

        s.set_task_progress('my task', 0.5, {'test_data': 11})
        self.assertEqual(0.5, s.get_task_progress('my task'))
        self.assertEqual({
            'at_progress': 0.5,
            'details': {'test_data': 11}
        }, s.get_task_progress_details('my task'))

        s.set_task_progress('my task', 0.7, {'test_data': 17})
        self.assertEqual(0.7, s.get_task_progress('my task'))
        self.assertEqual({
            'at_progress': 0.7,
            'details': {'test_data': 17}
        }, s.get_task_progress_details('my task'))

        s.set_task_progress('my task', 0.99)
        self.assertEqual(0.99, s.get_task_progress('my task'))
        self.assertEqual({
            'at_progress': 0.7,
            'details': {'test_data': 17}
        }, s.get_task_progress_details('my task'))

    def test_task_progress_erase(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))

        s.set_task_progress('my task', 0.8, {})
        self.assertEqual(0.8, s.get_task_progress('my task'))
        self.assertIsNone(s.get_task_progress_details('my task'))

    def test_fetch_result_not_ready(self):
        s = self._get_storage()
        name = 'my result'
        s.ensure_atom(test_utils.NoopTask('my task', provides=name))
        self.assertRaises(exceptions.NotFound, s.get, name)
        self.assertEqual({}, s.fetch_all())

    def test_save_multiple_results(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task', provides=['foo', 'bar']))
        s.save('my task', ('spam', 'eggs'))
        self.assertEqual({
            'foo': 'spam',
            'bar': 'eggs',
        }, s.fetch_all())

    def test_mapping_none(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        s.save('my task', 5)
        self.assertEqual({}, s.fetch_all())

    def test_inject(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        self.assertEqual('eggs', s.fetch('spam'))
        self.assertEqual({
            'foo': 'bar',
            'spam': 'eggs',
        }, s.fetch_all())

    def test_inject_twice(self):
        s = self._get_storage()
        s.inject({'foo': 'bar'})
        self.assertEqual({'foo': 'bar'}, s.fetch_all())
        s.inject({'spam': 'eggs'})
        self.assertEqual({
            'foo': 'bar',
            'spam': 'eggs',
        }, s.fetch_all())

    def test_inject_resumed(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        # verify it's there
        self.assertEqual({
            'foo': 'bar',
            'spam': 'eggs',
        }, s.fetch_all())
        # imagine we are resuming, so we need to make new
        # storage from same flow details
        s2 = self._get_storage(s._flowdetail)
        # injected data should still be there:
        self.assertEqual({
            'foo': 'bar',
            'spam': 'eggs',
        }, s2.fetch_all())

    def test_many_thread_ensure_same_task(self):
        s = self._get_storage()

        def ensure_my_task():
            s.ensure_atom(test_utils.NoopTask('my_task'))

        threads = []
        for i in range(0, self.thread_count):
            threads.append(threading.Thread(target=ensure_my_task))
        self._run_many_threads(threads)

        # Only one task should have been made, no more.
        self.assertEqual(1, len(s._flowdetail))

    def test_many_thread_inject(self):
        s = self._get_storage()

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

    def test_fetch_mapped_args(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        self.assertEqual({'viking': 'eggs'},
                         s.fetch_mapped_args({'viking': 'spam'}))

    def test_fetch_not_found_args(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        self.assertRaises(exceptions.NotFound,
                          s.fetch_mapped_args, {'viking': 'helmet'})

    def test_fetch_optional_args_found(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        self.assertEqual({'viking': 'eggs'},
                         s.fetch_mapped_args({'viking': 'spam'},
                                             optional_args=set(['viking'])))

    def test_fetch_optional_args_not_found(self):
        s = self._get_storage()
        s.inject({'foo': 'bar', 'spam': 'eggs'})
        self.assertEqual({},
                         s.fetch_mapped_args({'viking': 'helmet'},
                                             optional_args=set(['viking'])))

    def test_set_and_get_task_state(self):
        s = self._get_storage()
        state = states.PENDING
        s.ensure_atom(test_utils.NoopTask('my task'))
        s.set_atom_state('my task', state)
        self.assertEqual(state, s.get_atom_state('my task'))

    def test_get_state_of_unknown_task(self):
        s = self._get_storage()
        self.assertRaisesRegex(exceptions.NotFound, '^Unknown',
                               s.get_atom_state, 'my task')

    def test_task_by_name(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        self.assertTrue(uuidutils.is_uuid_like(s.get_atom_uuid('my task')))

    def test_transient_storage_fetch_all(self):
        s = self._get_storage()
        s.inject([("a", "b")], transient=True)
        s.inject([("b", "c")])

        results = s.fetch_all()
        self.assertEqual({"a": "b", "b": "c"}, results)

    def test_transient_storage_fetch_mapped(self):
        s = self._get_storage()
        s.inject([("a", "b")], transient=True)
        s.inject([("b", "c")])
        desired = {
            'y': 'a',
            'z': 'b',
        }
        args = s.fetch_mapped_args(desired)
        self.assertEqual({'y': 'b', 'z': 'c'}, args)

    def test_transient_storage_restore(self):
        _lb, flow_detail = p_utils.temporary_flow_detail(self.backend)
        s = self._get_storage(flow_detail=flow_detail)
        s.inject([("a", "b")], transient=True)
        s.inject([("b", "c")])

        s2 = self._get_storage(flow_detail=flow_detail)
        results = s2.fetch_all()
        self.assertEqual({"b": "c"}, results)

    def test_unknown_task_by_name(self):
        s = self._get_storage()
        self.assertRaisesRegex(exceptions.NotFound,
                               '^Unknown atom',
                               s.get_atom_uuid, '42')

    def test_initial_flow_state(self):
        s = self._get_storage()
        self.assertEqual(states.PENDING, s.get_flow_state())

    def test_get_flow_state(self):
        _lb, flow_detail = p_utils.temporary_flow_detail(backend=self.backend)
        flow_detail.state = states.FAILURE
        with contextlib.closing(self.backend.get_connection()) as conn:
            flow_detail.update(conn.update_flow_details(flow_detail))
        s = self._get_storage(flow_detail)
        self.assertEqual(states.FAILURE, s.get_flow_state())

    def test_set_and_get_flow_state(self):
        s = self._get_storage()
        s.set_flow_state(states.SUCCESS)
        self.assertEqual(states.SUCCESS, s.get_flow_state())

    def test_result_is_checked(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task', provides=set(['result'])))
        s.save('my task', {})
        self.assertRaisesRegex(exceptions.NotFound,
                               '^Unable to find result', s.fetch, 'result')

    def test_empty_result_is_checked(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task', provides=['a']))
        s.save('my task', ())
        self.assertRaisesRegex(exceptions.NotFound,
                               '^Unable to find result', s.fetch, 'a')

    def test_short_result_is_checked(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task', provides=['a', 'b']))
        s.save('my task', ['result'])
        self.assertEqual('result', s.fetch('a'))
        self.assertRaisesRegex(exceptions.NotFound,
                               '^Unable to find result', s.fetch, 'b')

    def test_ensure_retry(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopRetry('my retry'))
        history = s.get_retry_history('my retry')
        self.assertEqual([], list(history))

    def test_ensure_retry_and_task_with_same_name(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my retry'))
        self.assertRaisesRegex(exceptions.Duplicate,
                               '^Atom detail', s.ensure_atom,
                               test_utils.NoopRetry('my retry'))

    def test_save_retry_results(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopRetry('my retry'))
        s.save('my retry', 'a')
        s.save('my retry', 'b')
        history = s.get_retry_history('my retry')
        self.assertEqual([('a', {}), ('b', {})], list(history))
        self.assertEqual(['a', 'b'], list(history.provided_iter()))

    def test_save_retry_results_with_mapping(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopRetry('my retry', provides=['x']))
        s.save('my retry', 'a')
        s.save('my retry', 'b')
        history = s.get_retry_history('my retry')
        self.assertEqual([('a', {}), ('b', {})], list(history))
        self.assertEqual(['a', 'b'], list(history.provided_iter()))
        self.assertEqual({'x': 'b'}, s.fetch_all())
        self.assertEqual('b', s.fetch('x'))

    def test_cleanup_retry_history(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopRetry('my retry', provides=['x']))
        s.save('my retry', 'a')
        s.save('my retry', 'b')
        s.cleanup_retry_history('my retry', states.REVERTED)
        history = s.get_retry_history('my retry')
        self.assertEqual([], list(history))
        self.assertEqual(0, len(history))
        self.assertEqual({}, s.fetch_all())

    def test_cached_retry_failure(self):
        a_failure = failure.Failure.from_exception(RuntimeError('Woot!'))
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopRetry('my retry', provides=['x']))
        s.save('my retry', 'a')
        s.save('my retry', a_failure, states.FAILURE)
        history = s.get_retry_history('my retry')
        self.assertEqual([('a', {})], list(history))
        self.assertTrue(history.caused_by(RuntimeError, include_retry=True))
        self.assertIsNotNone(history.failure)
        self.assertEqual(1, len(history))
        self.assertTrue(s.has_failures())
        self.assertEqual({'my retry': a_failure}, s.get_failures())

    def test_logbook_get_unknown_atom_type(self):
        self.assertRaisesRegex(TypeError,
                               'Unknown atom',
                               models.atom_detail_class, 'some_detail')

    def test_save_task_intention(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my task'))
        s.set_atom_intention('my task', states.REVERT)
        intention = s.get_atom_intention('my task')
        self.assertEqual(states.REVERT, intention)

    def test_save_retry_intention(self):
        s = self._get_storage()
        s.ensure_atom(test_utils.NoopTask('my retry'))
        s.set_atom_intention('my retry', states.RETRY)
        intention = s.get_atom_intention('my retry')
        self.assertEqual(states.RETRY, intention)

    def test_inject_persistent_missing(self):
        t = test_utils.ProgressingTask('my retry', requires=['x'])
        s = self._get_storage()
        s.ensure_atom(t)
        missing = s.fetch_unsatisfied_args(t.name, t.rebind)
        self.assertEqual(set(['x']), missing)
        s.inject_atom_args(t.name, {'x': 2}, transient=False)
        missing = s.fetch_unsatisfied_args(t.name, t.rebind)
        self.assertEqual(set(), missing)
        args = s.fetch_mapped_args(t.rebind, atom_name=t.name)
        self.assertEqual(2, args['x'])

    def test_inject_persistent_and_transient_missing(self):
        t = test_utils.ProgressingTask('my retry', requires=['x'])
        s = self._get_storage()
        s.ensure_atom(t)
        missing = s.fetch_unsatisfied_args(t.name, t.rebind)
        self.assertEqual(set(['x']), missing)
        s.inject_atom_args(t.name, {'x': 2}, transient=False)
        s.inject_atom_args(t.name, {'x': 3}, transient=True)
        missing = s.fetch_unsatisfied_args(t.name, t.rebind)
        self.assertEqual(set(), missing)
        args = s.fetch_mapped_args(t.rebind, atom_name=t.name)
        self.assertEqual(3, args['x'])

    def test_save_fetch(self):
        t = test_utils.GiveBackRevert('my task')
        s = self._get_storage()
        s.ensure_atom(t)
        s.save('my task', 2)
        self.assertEqual(2, s.get('my task'))
        self.assertRaises(exceptions.NotFound,
                          s.get_revert_result, 'my task')

    def test_save_fetch_revert(self):
        t = test_utils.GiveBackRevert('my task')
        s = self._get_storage()
        s.ensure_atom(t)
        s.set_atom_intention('my task', states.REVERT)
        s.save('my task', 2, state=states.REVERTED)
        self.assertRaises(exceptions.NotFound, s.get, 'my task')
        self.assertEqual(2, s.get_revert_result('my task'))

    def test_save_fail_fetch_revert(self):
        t = test_utils.GiveBackRevert('my task')
        s = self._get_storage()
        s.ensure_atom(t)
        s.set_atom_intention('my task', states.REVERT)
        a_failure = failure.Failure.from_exception(RuntimeError('Woot!'))
        s.save('my task', a_failure, state=states.REVERT_FAILURE)
        self.assertEqual(a_failure, s.get_revert_result('my task'))


class StorageMemoryTest(StorageTestMixin, test.TestCase):
    def setUp(self):
        super(StorageMemoryTest, self).setUp()
        self.backend = backends.fetch({'connection': 'memory://'})


class StorageSQLTest(StorageTestMixin, test.TestCase):
    def setUp(self):
        super(StorageSQLTest, self).setUp()
        self.backend = backends.fetch({'connection': 'sqlite://'})
        with contextlib.closing(self.backend.get_connection()) as conn:
            conn.upgrade()
