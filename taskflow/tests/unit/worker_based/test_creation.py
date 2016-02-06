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

from taskflow.engines.worker_based import engine
from taskflow.engines.worker_based import executor
from taskflow.patterns import linear_flow as lf
from taskflow.persistence import backends
from taskflow import test
from taskflow.test import mock
from taskflow.tests import utils
from taskflow.utils import persistence_utils as pu


class TestWorkerBasedActionEngine(test.MockTestCase):
    @staticmethod
    def _create_engine(**kwargs):
        flow = lf.Flow('test-flow').add(utils.DummyTask())
        backend = backends.fetch({'connection': 'memory'})
        flow_detail = pu.create_flow_detail(flow, backend=backend)
        options = kwargs.copy()
        return engine.WorkerBasedActionEngine(flow, flow_detail,
                                              backend, options)

    def _patch_in_executor(self):
        executor_mock, executor_inst_mock = self.patchClass(
            engine.executor, 'WorkerTaskExecutor', attach_as='executor')
        return executor_mock, executor_inst_mock

    def test_creation_default(self):
        executor_mock, executor_inst_mock = self._patch_in_executor()
        eng = self._create_engine()
        expected_calls = [
            mock.call.executor_class(uuid=eng.storage.flow_uuid,
                                     url=None,
                                     exchange='default',
                                     topics=[],
                                     transport=None,
                                     transport_options=None,
                                     transition_timeout=mock.ANY,
                                     retry_options=None,
                                     worker_expiry=mock.ANY)
        ]
        self.assertEqual(expected_calls, self.master_mock.mock_calls)

    def test_creation_custom(self):
        executor_mock, executor_inst_mock = self._patch_in_executor()
        topics = ['test-topic1', 'test-topic2']
        exchange = 'test-exchange'
        broker_url = 'test-url'
        eng = self._create_engine(
            url=broker_url,
            exchange=exchange,
            transport='memory',
            transport_options={},
            transition_timeout=200,
            topics=topics,
            retry_options={},
            worker_expiry=1)
        expected_calls = [
            mock.call.executor_class(uuid=eng.storage.flow_uuid,
                                     url=broker_url,
                                     exchange=exchange,
                                     topics=topics,
                                     transport='memory',
                                     transport_options={},
                                     transition_timeout=200,
                                     retry_options={},
                                     worker_expiry=1)
        ]
        self.assertEqual(expected_calls, self.master_mock.mock_calls)

    def test_creation_custom_executor(self):
        ex = executor.WorkerTaskExecutor('a', 'test-exchange', ['test-topic'])
        eng = self._create_engine(executor=ex)
        self.assertIs(eng._task_executor, ex)
        self.assertIsInstance(eng._task_executor, executor.WorkerTaskExecutor)

    def test_creation_invalid_custom_executor(self):
        self.assertRaises(TypeError, self._create_engine, executor=2)
        self.assertRaises(TypeError, self._create_engine, executor='blah')
