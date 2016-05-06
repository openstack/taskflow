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

import collections
import contextlib
import threading

import futurist
import testscenarios
from zake import fake_client

from taskflow.conductors import backends
from taskflow import engines
from taskflow.jobs.backends import impl_zookeeper
from taskflow.jobs import base
from taskflow.patterns import linear_flow as lf
from taskflow.persistence.backends import impl_memory
from taskflow import states as st
from taskflow import test
from taskflow.tests import utils as test_utils
from taskflow.utils import persistence_utils as pu
from taskflow.utils import threading_utils


@contextlib.contextmanager
def close_many(*closeables):
    try:
        yield
    finally:
        for c in closeables:
            c.close()


def test_factory(blowup):
    f = lf.Flow("test")
    if not blowup:
        f.add(test_utils.ProgressingTask('test1'))
    else:
        f.add(test_utils.FailingTask("test1"))
    return f


def sleep_factory():
    f = lf.Flow("test")
    f.add(test_utils.SleepTask('test1'))
    f.add(test_utils.ProgressingTask('test2'))
    return f


def test_store_factory():
    f = lf.Flow("test")
    f.add(test_utils.TaskMultiArg('task1'))
    return f


def single_factory():
    return futurist.ThreadPoolExecutor(max_workers=1)


ComponentBundle = collections.namedtuple('ComponentBundle',
                                         ['board', 'client',
                                          'persistence', 'conductor'])


class ManyConductorTest(testscenarios.TestWithScenarios,
                        test_utils.EngineTestBase, test.TestCase):
    scenarios = [
        ('blocking', {'kind': 'blocking',
                      'conductor_kwargs': {'wait_timeout': 0.1}}),
        ('nonblocking_many_thread',
         {'kind': 'nonblocking', 'conductor_kwargs': {'wait_timeout': 0.1}}),
        ('nonblocking_one_thread', {'kind': 'nonblocking',
                                    'conductor_kwargs': {
                                        'executor_factory': single_factory,
                                        'wait_timeout': 0.1,
                                    }})
    ]

    def make_components(self):
        client = fake_client.FakeClient()
        persistence = impl_memory.MemoryBackend()
        board = impl_zookeeper.ZookeeperJobBoard('testing', {},
                                                 client=client,
                                                 persistence=persistence)
        conductor_kwargs = self.conductor_kwargs.copy()
        conductor_kwargs['persistence'] = persistence
        conductor = backends.fetch(self.kind, 'testing', board,
                                   **conductor_kwargs)
        return ComponentBundle(board, client, persistence, conductor)

    def test_connection(self):
        components = self.make_components()
        components.conductor.connect()
        with close_many(components.conductor, components.client):
            self.assertTrue(components.board.connected)
            self.assertTrue(components.client.connected)
        self.assertFalse(components.board.connected)
        self.assertFalse(components.client.connected)

    def test_run_empty(self):
        components = self.make_components()
        components.conductor.connect()
        with close_many(components.conductor, components.client):
            t = threading_utils.daemon_thread(components.conductor.run)
            t.start()
            components.conductor.stop()
            self.assertTrue(
                components.conductor.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(components.conductor.dispatching)
            t.join()

    def test_run(self):
        components = self.make_components()
        components.conductor.connect()
        consumed_event = threading.Event()
        job_consumed_event = threading.Event()
        job_abandoned_event = threading.Event()

        def on_consume(state, details):
            consumed_event.set()

        def on_job_consumed(event, details):
            if event == 'job_consumed':
                job_consumed_event.set()

        def on_job_abandoned(event, details):
            if event == 'job_abandoned':
                job_abandoned_event.set()

        components.board.notifier.register(base.REMOVAL, on_consume)
        components.conductor.notifier.register("job_consumed",
                                               on_job_consumed)
        components.conductor.notifier.register("job_abandoned",
                                               on_job_abandoned)
        with close_many(components.conductor, components.client):
            t = threading_utils.daemon_thread(components.conductor.run)
            t.start()
            lb, fd = pu.temporary_flow_detail(components.persistence)
            engines.save_factory_details(fd, test_factory,
                                         [False], {},
                                         backend=components.persistence)
            components.board.post('poke', lb,
                                  details={'flow_uuid': fd.uuid})
            self.assertTrue(consumed_event.wait(test_utils.WAIT_TIMEOUT))
            self.assertTrue(job_consumed_event.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(job_abandoned_event.wait(1))
            components.conductor.stop()
            self.assertTrue(components.conductor.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(components.conductor.dispatching)

        persistence = components.persistence
        with contextlib.closing(persistence.get_connection()) as conn:
            lb = conn.get_logbook(lb.uuid)
            fd = lb.find(fd.uuid)
        self.assertIsNotNone(fd)
        self.assertEqual(st.SUCCESS, fd.state)

    def test_run_max_dispatches(self):
        components = self.make_components()
        components.conductor.connect()
        consumed_event = threading.Event()

        def on_consume(state, details):
            consumed_event.set()

        components.board.notifier.register(base.REMOVAL, on_consume)
        with close_many(components.client, components.conductor):
            t = threading_utils.daemon_thread(
                lambda: components.conductor.run(max_dispatches=5))
            t.start()
            lb, fd = pu.temporary_flow_detail(components.persistence)
            engines.save_factory_details(fd, test_factory,
                                         [False], {},
                                         backend=components.persistence)
            for _ in range(5):
                components.board.post('poke', lb,
                                      details={'flow_uuid': fd.uuid})
                self.assertTrue(consumed_event.wait(
                    test_utils.WAIT_TIMEOUT))
            components.board.post('poke', lb,
                                  details={'flow_uuid': fd.uuid})
            components.conductor.stop()
            self.assertTrue(components.conductor.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(components.conductor.dispatching)

    def test_fail_run(self):
        components = self.make_components()
        components.conductor.connect()
        consumed_event = threading.Event()
        job_consumed_event = threading.Event()
        job_abandoned_event = threading.Event()

        def on_consume(state, details):
            consumed_event.set()

        def on_job_consumed(event, details):
            if event == 'job_consumed':
                job_consumed_event.set()

        def on_job_abandoned(event, details):
            if event == 'job_abandoned':
                job_abandoned_event.set()

        components.board.notifier.register(base.REMOVAL, on_consume)
        components.conductor.notifier.register("job_consumed",
                                               on_job_consumed)
        components.conductor.notifier.register("job_abandoned",
                                               on_job_abandoned)
        with close_many(components.conductor, components.client):
            t = threading_utils.daemon_thread(components.conductor.run)
            t.start()
            lb, fd = pu.temporary_flow_detail(components.persistence)
            engines.save_factory_details(fd, test_factory,
                                         [True], {},
                                         backend=components.persistence)
            components.board.post('poke', lb,
                                  details={'flow_uuid': fd.uuid})
            self.assertTrue(consumed_event.wait(test_utils.WAIT_TIMEOUT))
            self.assertTrue(job_consumed_event.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(job_abandoned_event.wait(1))
            components.conductor.stop()
            self.assertTrue(components.conductor.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(components.conductor.dispatching)

        persistence = components.persistence
        with contextlib.closing(persistence.get_connection()) as conn:
            lb = conn.get_logbook(lb.uuid)
            fd = lb.find(fd.uuid)
        self.assertIsNotNone(fd)
        self.assertEqual(st.REVERTED, fd.state)

    def test_missing_store(self):
        components = self.make_components()
        components.conductor.connect()
        consumed_event = threading.Event()

        def on_consume(state, details):
            consumed_event.set()

        components.board.notifier.register(base.REMOVAL, on_consume)
        with close_many(components.conductor, components.client):
            t = threading_utils.daemon_thread(components.conductor.run)
            t.start()
            lb, fd = pu.temporary_flow_detail(components.persistence)
            engines.save_factory_details(fd, test_store_factory,
                                         [], {},
                                         backend=components.persistence)
            components.board.post('poke', lb,
                                  details={'flow_uuid': fd.uuid})
            self.assertTrue(consumed_event.wait(test_utils.WAIT_TIMEOUT))
            components.conductor.stop()
            self.assertTrue(components.conductor.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(components.conductor.dispatching)

        persistence = components.persistence
        with contextlib.closing(persistence.get_connection()) as conn:
            lb = conn.get_logbook(lb.uuid)
            fd = lb.find(fd.uuid)
        self.assertIsNotNone(fd)
        self.assertIsNone(fd.state)

    def test_job_store(self):
        components = self.make_components()
        components.conductor.connect()
        consumed_event = threading.Event()

        def on_consume(state, details):
            consumed_event.set()

        store = {'x': True, 'y': False, 'z': None}

        components.board.notifier.register(base.REMOVAL, on_consume)
        with close_many(components.conductor, components.client):
            t = threading_utils.daemon_thread(components.conductor.run)
            t.start()
            lb, fd = pu.temporary_flow_detail(components.persistence)
            engines.save_factory_details(fd, test_store_factory,
                                         [], {},
                                         backend=components.persistence)
            components.board.post('poke', lb,
                                  details={'flow_uuid': fd.uuid,
                                           'store': store})
            self.assertTrue(consumed_event.wait(test_utils.WAIT_TIMEOUT))
            components.conductor.stop()
            self.assertTrue(components.conductor.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(components.conductor.dispatching)

        persistence = components.persistence
        with contextlib.closing(persistence.get_connection()) as conn:
            lb = conn.get_logbook(lb.uuid)
            fd = lb.find(fd.uuid)
        self.assertIsNotNone(fd)
        self.assertEqual(st.SUCCESS, fd.state)

    def test_flowdetails_store(self):
        components = self.make_components()
        components.conductor.connect()
        consumed_event = threading.Event()

        def on_consume(state, details):
            consumed_event.set()

        store = {'x': True, 'y': False, 'z': None}

        components.board.notifier.register(base.REMOVAL, on_consume)
        with close_many(components.conductor, components.client):
            t = threading_utils.daemon_thread(components.conductor.run)
            t.start()
            lb, fd = pu.temporary_flow_detail(components.persistence,
                                              meta={'store': store})
            engines.save_factory_details(fd, test_store_factory,
                                         [], {},
                                         backend=components.persistence)
            components.board.post('poke', lb,
                                  details={'flow_uuid': fd.uuid})
            self.assertTrue(consumed_event.wait(test_utils.WAIT_TIMEOUT))
            components.conductor.stop()
            self.assertTrue(components.conductor.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(components.conductor.dispatching)

        persistence = components.persistence
        with contextlib.closing(persistence.get_connection()) as conn:
            lb = conn.get_logbook(lb.uuid)
            fd = lb.find(fd.uuid)
        self.assertIsNotNone(fd)
        self.assertEqual(st.SUCCESS, fd.state)

    def test_combined_store(self):
        components = self.make_components()
        components.conductor.connect()
        consumed_event = threading.Event()

        def on_consume(state, details):
            consumed_event.set()

        flow_store = {'x': True, 'y': False}
        job_store = {'z': None}

        components.board.notifier.register(base.REMOVAL, on_consume)
        with close_many(components.conductor, components.client):
            t = threading_utils.daemon_thread(components.conductor.run)
            t.start()
            lb, fd = pu.temporary_flow_detail(components.persistence,
                                              meta={'store': flow_store})
            engines.save_factory_details(fd, test_store_factory,
                                         [], {},
                                         backend=components.persistence)
            components.board.post('poke', lb,
                                  details={'flow_uuid': fd.uuid,
                                           'store': job_store})
            self.assertTrue(consumed_event.wait(test_utils.WAIT_TIMEOUT))
            components.conductor.stop()
            self.assertTrue(components.conductor.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(components.conductor.dispatching)

        persistence = components.persistence
        with contextlib.closing(persistence.get_connection()) as conn:
            lb = conn.get_logbook(lb.uuid)
            fd = lb.find(fd.uuid)
        self.assertIsNotNone(fd)
        self.assertEqual(st.SUCCESS, fd.state)

    def test_stop_aborts_engine(self):
        components = self.make_components()
        components.conductor.connect()
        consumed_event = threading.Event()
        job_consumed_event = threading.Event()
        job_abandoned_event = threading.Event()
        running_start_event = threading.Event()

        def on_running_start(event, details):
            running_start_event.set()

        def on_consume(state, details):
            consumed_event.set()

        def on_job_consumed(event, details):
            if event == 'job_consumed':
                job_consumed_event.set()

        def on_job_abandoned(event, details):
            if event == 'job_abandoned':
                job_abandoned_event.set()

        components.board.notifier.register(base.REMOVAL, on_consume)
        components.conductor.notifier.register("job_consumed",
                                               on_job_consumed)
        components.conductor.notifier.register("job_abandoned",
                                               on_job_abandoned)
        components.conductor.notifier.register("running_start",
                                               on_running_start)
        with close_many(components.conductor, components.client):
            t = threading_utils.daemon_thread(components.conductor.run)
            t.start()
            lb, fd = pu.temporary_flow_detail(components.persistence)
            engines.save_factory_details(fd, sleep_factory,
                                         [], {},
                                         backend=components.persistence)
            components.board.post('poke', lb,
                                  details={'flow_uuid': fd.uuid,
                                           'store': {'duration': 2}})
            running_start_event.wait(test_utils.WAIT_TIMEOUT)
            components.conductor.stop()
            job_abandoned_event.wait(test_utils.WAIT_TIMEOUT)
            self.assertTrue(job_abandoned_event.is_set())
            self.assertFalse(job_consumed_event.is_set())
            self.assertFalse(consumed_event.is_set())


class NonBlockingExecutorTest(test.TestCase):
    def test_bad_wait_timeout(self):
        persistence = impl_memory.MemoryBackend()
        client = fake_client.FakeClient()
        board = impl_zookeeper.ZookeeperJobBoard('testing', {},
                                                 client=client,
                                                 persistence=persistence)
        self.assertRaises(ValueError,
                          backends.fetch,
                          'nonblocking', 'testing', board,
                          persistence=persistence,
                          wait_timeout='testing')

    def test_bad_factory(self):
        persistence = impl_memory.MemoryBackend()
        client = fake_client.FakeClient()
        board = impl_zookeeper.ZookeeperJobBoard('testing', {},
                                                 client=client,
                                                 persistence=persistence)
        self.assertRaises(ValueError,
                          backends.fetch,
                          'nonblocking', 'testing', board,
                          persistence=persistence,
                          executor_factory='testing')
