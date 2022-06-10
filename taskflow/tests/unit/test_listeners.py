# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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
import logging
import threading
import time

from oslo_serialization import jsonutils
from oslo_utils import reflection
from zake import fake_client

import taskflow.engines
from taskflow import exceptions as exc
from taskflow.jobs import backends as jobs
from taskflow.listeners import claims
from taskflow.listeners import logging as logging_listeners
from taskflow.listeners import timing
from taskflow.patterns import linear_flow as lf
from taskflow.persistence.backends import impl_memory
from taskflow import states
from taskflow import task
from taskflow import test
from taskflow.test import mock
from taskflow.tests import utils as test_utils
from taskflow.utils import misc
from taskflow.utils import persistence_utils


_LOG_LEVELS = frozenset([
    logging.CRITICAL,
    logging.DEBUG,
    logging.ERROR,
    logging.INFO,
    logging.NOTSET,
    logging.WARNING,
])


class SleepyTask(task.Task):
    def __init__(self, name, sleep_for=0.0):
        super(SleepyTask, self).__init__(name=name)
        self._sleep_for = float(sleep_for)

    def execute(self):
        if self._sleep_for <= 0:
            return
        else:
            time.sleep(self._sleep_for)


class EngineMakerMixin(object):
    def _make_engine(self, flow, flow_detail=None, backend=None):
        e = taskflow.engines.load(flow,
                                  flow_detail=flow_detail,
                                  backend=backend)
        e.compile()
        e.prepare()
        return e


class TestClaimListener(test.TestCase, EngineMakerMixin):
    def _make_dummy_flow(self, count):
        f = lf.Flow('root')
        for i in range(0, count):
            f.add(test_utils.ProvidesRequiresTask('%s_test' % i, [], []))
        return f

    def setUp(self):
        super(TestClaimListener, self).setUp()
        self.client = fake_client.FakeClient()
        self.addCleanup(self.client.stop)
        self.board = jobs.fetch('test', 'zookeeper', client=self.client)
        self.addCleanup(self.board.close)
        self.board.connect()

    def _post_claim_job(self, job_name, book=None, details=None):
        arrived = threading.Event()

        def set_on_children(children):
            if children:
                arrived.set()

        self.client.ChildrenWatch("/taskflow", set_on_children)
        job = self.board.post('test-1')

        # Make sure it arrived and claimed before doing further work...
        self.assertTrue(arrived.wait(test_utils.WAIT_TIMEOUT))
        arrived.clear()
        self.board.claim(job, self.board.name)
        self.assertTrue(arrived.wait(test_utils.WAIT_TIMEOUT))
        self.assertEqual(states.CLAIMED, job.state)

        return job

    def _destroy_locks(self):
        children = self.client.storage.get_children("/taskflow",
                                                    only_direct=False)
        removed = 0
        for p, data in children.items():
            if p.endswith(".lock"):
                self.client.storage.pop(p)
                removed += 1
        return removed

    def _change_owner(self, new_owner):
        children = self.client.storage.get_children("/taskflow",
                                                    only_direct=False)
        altered = 0
        for p, data in children.items():
            if p.endswith(".lock"):
                self.client.set(p, misc.binary_encode(
                    jsonutils.dumps({'owner': new_owner})))
                altered += 1
        return altered

    def test_bad_create(self):
        job = self._post_claim_job('test')
        f = self._make_dummy_flow(10)
        e = self._make_engine(f)
        self.assertRaises(ValueError, claims.CheckingClaimListener,
                          e, job, self.board, self.board.name,
                          on_job_loss=1)

    def test_claim_lost_suspended(self):
        job = self._post_claim_job('test')
        f = self._make_dummy_flow(10)
        e = self._make_engine(f)

        try_destroy = True
        ran_states = []
        with claims.CheckingClaimListener(e, job,
                                          self.board, self.board.name):
            for state in e.run_iter():
                ran_states.append(state)
                if state == states.SCHEDULING and try_destroy:
                    try_destroy = bool(self._destroy_locks())

        self.assertEqual(states.SUSPENDED, e.storage.get_flow_state())
        self.assertEqual(1, ran_states.count(states.ANALYZING))
        self.assertEqual(1, ran_states.count(states.SCHEDULING))
        self.assertEqual(1, ran_states.count(states.WAITING))

    def test_claim_lost_custom_handler(self):
        job = self._post_claim_job('test')
        f = self._make_dummy_flow(10)
        e = self._make_engine(f)

        handler = mock.MagicMock()
        ran_states = []
        try_destroy = True
        destroyed_at = -1
        with claims.CheckingClaimListener(e, job, self.board,
                                          self.board.name,
                                          on_job_loss=handler):
            for i, state in enumerate(e.run_iter()):
                ran_states.append(state)
                if state == states.SCHEDULING and try_destroy:
                    destroyed = bool(self._destroy_locks())
                    if destroyed:
                        destroyed_at = i
                        try_destroy = False

        self.assertTrue(handler.called)
        self.assertEqual(10, ran_states.count(states.SCHEDULING))
        self.assertNotEqual(-1, destroyed_at)

        after_states = ran_states[destroyed_at:]
        self.assertGreater(0, len(after_states))

    def test_claim_lost_new_owner(self):
        job = self._post_claim_job('test')
        f = self._make_dummy_flow(10)
        e = self._make_engine(f)

        change_owner = True
        ran_states = []
        with claims.CheckingClaimListener(e, job,
                                          self.board, self.board.name):
            for state in e.run_iter():
                ran_states.append(state)
                if state == states.SCHEDULING and change_owner:
                    change_owner = bool(self._change_owner('test-2'))

        self.assertEqual(states.SUSPENDED, e.storage.get_flow_state())
        self.assertEqual(1, ran_states.count(states.ANALYZING))
        self.assertEqual(1, ran_states.count(states.SCHEDULING))
        self.assertEqual(1, ran_states.count(states.WAITING))


class TestDurationListener(test.TestCase, EngineMakerMixin):
    def test_deregister(self):
        """Verify that register and deregister don't blow up"""
        with contextlib.closing(impl_memory.MemoryBackend()) as be:
            flow = lf.Flow("test")
            flow.add(SleepyTask("test-1", sleep_for=0.1))
            (lb, fd) = persistence_utils.temporary_flow_detail(be)
            e = self._make_engine(flow, fd, be)
            l = timing.DurationListener(e)
            l.register()
            l.deregister()

    def test_task_duration(self):
        with contextlib.closing(impl_memory.MemoryBackend()) as be:
            flow = lf.Flow("test")
            flow.add(SleepyTask("test-1", sleep_for=0.1))
            (lb, fd) = persistence_utils.temporary_flow_detail(be)
            e = self._make_engine(flow, fd, be)
            with timing.DurationListener(e):
                e.run()
            t_uuid = e.storage.get_atom_uuid("test-1")
            td = fd.find(t_uuid)
            self.assertIsNotNone(td)
            self.assertIsNotNone(td.meta)
            self.assertIn('duration', td.meta)
            self.assertGreaterEqual(0.1, td.meta['duration'])

    def test_flow_duration(self):
        with contextlib.closing(impl_memory.MemoryBackend()) as be:
            flow = lf.Flow("test")
            flow.add(SleepyTask("test-1", sleep_for=0.1))
            (lb, fd) = persistence_utils.temporary_flow_detail(be)
            e = self._make_engine(flow, fd, be)
            with timing.DurationListener(e):
                e.run()
            self.assertIsNotNone(fd)
            self.assertIsNotNone(fd.meta)
            self.assertIn('duration', fd.meta)
            self.assertGreaterEqual(0.1, fd.meta['duration'])

    @mock.patch.object(timing.LOG, 'warning')
    def test_record_ending_exception(self, mocked_warning):
        with contextlib.closing(impl_memory.MemoryBackend()) as be:
            flow = lf.Flow("test")
            flow.add(test_utils.TaskNoRequiresNoReturns("test-1"))
            (lb, fd) = persistence_utils.temporary_flow_detail(be)
            e = self._make_engine(flow, fd, be)
            duration_listener = timing.DurationListener(e)
            with mock.patch.object(duration_listener._engine.storage,
                                   'update_atom_metadata') as mocked_uam:
                mocked_uam.side_effect = exc.StorageFailure('Woot!')
                with duration_listener:
                    e.run()
        mocked_warning.assert_called_once_with(mock.ANY, mock.ANY, 'task',
                                               'test-1', exc_info=True)


class TestEventTimeListener(test.TestCase, EngineMakerMixin):
    def test_event_time(self):
        flow = lf.Flow('flow1').add(SleepyTask("task1", sleep_for=0.1))
        engine = self._make_engine(flow)
        with timing.EventTimeListener(engine):
            engine.run()
        t_uuid = engine.storage.get_atom_uuid("task1")
        td = engine.storage._flowdetail.find(t_uuid)
        self.assertIsNotNone(td)
        self.assertIsNotNone(td.meta)
        running_field = '%s-timestamp' % states.RUNNING
        success_field = '%s-timestamp' % states.SUCCESS
        self.assertIn(running_field, td.meta)
        self.assertIn(success_field, td.meta)
        td_duration = td.meta[success_field] - td.meta[running_field]
        self.assertGreaterEqual(0.1, td_duration)
        fd_meta = engine.storage._flowdetail.meta
        self.assertIn(running_field, fd_meta)
        self.assertIn(success_field, fd_meta)
        fd_duration = fd_meta[success_field] - fd_meta[running_field]
        self.assertGreaterEqual(0.1, fd_duration)


class TestCapturingListeners(test.TestCase, EngineMakerMixin):
    def test_basic_do_not_capture(self):
        flow = lf.Flow("test")
        flow.add(test_utils.ProgressingTask("task1"))
        e = self._make_engine(flow)
        with test_utils.CaptureListener(e, capture_task=False) as capturer:
            e.run()
        expected = ['test.f RUNNING',
                    'test.f SUCCESS']
        self.assertEqual(expected, capturer.values)


class TestLoggingListeners(test.TestCase, EngineMakerMixin):
    def _make_logger(self, level=logging.DEBUG):
        log = logging.getLogger(
            reflection.get_callable_name(self._get_test_method()))
        log.propagate = False
        for handler in reversed(log.handlers):
            log.removeHandler(handler)
        handler = test.CapturingLoggingHandler(level=level)
        log.addHandler(handler)
        log.setLevel(level)
        self.addCleanup(handler.reset)
        self.addCleanup(log.removeHandler, handler)
        return (log, handler)

    def test_basic(self):
        flow = lf.Flow("test")
        flow.add(test_utils.TaskNoRequiresNoReturns("test-1"))
        e = self._make_engine(flow)
        log, handler = self._make_logger()
        with logging_listeners.LoggingListener(e, log=log):
            e.run()
        self.assertGreater(0, handler.counts[logging.DEBUG])
        for levelno in _LOG_LEVELS - set([logging.DEBUG]):
            self.assertEqual(0, handler.counts[levelno])
        self.assertEqual([], handler.exc_infos)

    def test_basic_customized(self):
        flow = lf.Flow("test")
        flow.add(test_utils.TaskNoRequiresNoReturns("test-1"))
        e = self._make_engine(flow)
        log, handler = self._make_logger()
        listener = logging_listeners.LoggingListener(
            e, log=log, level=logging.INFO)
        with listener:
            e.run()
        self.assertGreater(0, handler.counts[logging.INFO])
        for levelno in _LOG_LEVELS - set([logging.INFO]):
            self.assertEqual(0, handler.counts[levelno])
        self.assertEqual([], handler.exc_infos)

    def test_basic_failure(self):
        flow = lf.Flow("test")
        flow.add(test_utils.TaskWithFailure("test-1"))
        e = self._make_engine(flow)
        log, handler = self._make_logger()
        with logging_listeners.LoggingListener(e, log=log):
            self.assertRaises(RuntimeError, e.run)
        self.assertGreater(0, handler.counts[logging.DEBUG])
        for levelno in _LOG_LEVELS - set([logging.DEBUG]):
            self.assertEqual(0, handler.counts[levelno])
        self.assertEqual(1, len(handler.exc_infos))

    def test_dynamic(self):
        flow = lf.Flow("test")
        flow.add(test_utils.TaskNoRequiresNoReturns("test-1"))
        e = self._make_engine(flow)
        log, handler = self._make_logger()
        with logging_listeners.DynamicLoggingListener(e, log=log):
            e.run()
        self.assertGreater(0, handler.counts[logging.DEBUG])
        for levelno in _LOG_LEVELS - set([logging.DEBUG]):
            self.assertEqual(0, handler.counts[levelno])
        self.assertEqual([], handler.exc_infos)

    def test_dynamic_failure(self):
        flow = lf.Flow("test")
        flow.add(test_utils.TaskWithFailure("test-1"))
        e = self._make_engine(flow)
        log, handler = self._make_logger()
        with logging_listeners.DynamicLoggingListener(e, log=log):
            self.assertRaises(RuntimeError, e.run)
        self.assertGreater(0, handler.counts[logging.WARNING])
        self.assertGreater(0, handler.counts[logging.DEBUG])
        self.assertEqual(1, len(handler.exc_infos))
        for levelno in _LOG_LEVELS - set([logging.DEBUG, logging.WARNING]):
            self.assertEqual(0, handler.counts[levelno])

    def test_dynamic_failure_customized_level(self):
        flow = lf.Flow("test")
        flow.add(test_utils.TaskWithFailure("test-1"))
        e = self._make_engine(flow)
        log, handler = self._make_logger()
        listener = logging_listeners.DynamicLoggingListener(
            e, log=log, failure_level=logging.ERROR)
        with listener:
            self.assertRaises(RuntimeError, e.run)
        self.assertGreater(0, handler.counts[logging.ERROR])
        self.assertGreater(0, handler.counts[logging.DEBUG])
        self.assertEqual(1, len(handler.exc_infos))
        for levelno in _LOG_LEVELS - set([logging.DEBUG, logging.ERROR]):
            self.assertEqual(0, handler.counts[levelno])
