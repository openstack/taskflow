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

from zake import fake_client

from taskflow.conductors import single_threaded as stc
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


class SingleThreadedConductorTest(test_utils.EngineTestBase, test.TestCase):
    ComponentBundle = collections.namedtuple('ComponentBundle',
                                             ['board', 'client',
                                              'persistence', 'conductor'])

    def make_components(self, name='testing', wait_timeout=0.1):
        client = fake_client.FakeClient()
        persistence = impl_memory.MemoryBackend()
        board = impl_zookeeper.ZookeeperJobBoard(name, {},
                                                 client=client,
                                                 persistence=persistence)
        conductor = stc.SingleThreadedConductor(name, board, persistence,
                                                wait_timeout=wait_timeout)
        return self.ComponentBundle(board, client, persistence, conductor)

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
        consumed_event = threading_utils.Event()

        def on_consume(state, details):
            consumed_event.set()

        components.board.notifier.register(base.REMOVAL, on_consume)
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
            components.conductor.stop()
            self.assertTrue(components.conductor.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(components.conductor.dispatching)

        persistence = components.persistence
        with contextlib.closing(persistence.get_connection()) as conn:
            lb = conn.get_logbook(lb.uuid)
            fd = lb.find(fd.uuid)
        self.assertIsNotNone(fd)
        self.assertEqual(st.SUCCESS, fd.state)

    def test_fail_run(self):
        components = self.make_components()
        components.conductor.connect()
        consumed_event = threading_utils.Event()

        def on_consume(state, details):
            consumed_event.set()

        components.board.notifier.register(base.REMOVAL, on_consume)
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
            components.conductor.stop()
            self.assertTrue(components.conductor.wait(test_utils.WAIT_TIMEOUT))
            self.assertFalse(components.conductor.dispatching)

        persistence = components.persistence
        with contextlib.closing(persistence.get_connection()) as conn:
            lb = conn.get_logbook(lb.uuid)
            fd = lb.find(fd.uuid)
        self.assertIsNotNone(fd)
        self.assertEqual(st.REVERTED, fd.state)
