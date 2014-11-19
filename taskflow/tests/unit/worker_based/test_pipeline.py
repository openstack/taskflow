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

import threading

from concurrent import futures

from taskflow.engines.worker_based import endpoint
from taskflow.engines.worker_based import executor as worker_executor
from taskflow.engines.worker_based import server as worker_server
from taskflow.openstack.common import uuidutils
from taskflow import test
from taskflow.tests import utils as test_utils
from taskflow.types import failure


TEST_EXCHANGE, TEST_TOPIC = ('test-exchange', 'test-topic')
WAIT_TIMEOUT = 1.0
POLLING_INTERVAL = 0.01


class TestPipeline(test.TestCase):
    def _fetch_server(self, task_classes):
        endpoints = []
        for cls in task_classes:
            endpoints.append(endpoint.Endpoint(cls))
        server = worker_server.Server(
            TEST_TOPIC, TEST_EXCHANGE,
            futures.ThreadPoolExecutor(1), endpoints,
            transport='memory',
            transport_options={
                'polling_interval': POLLING_INTERVAL,
            })
        server_thread = threading.Thread(target=server.start)
        server_thread.daemon = True
        return (server, server_thread)

    def _fetch_executor(self):
        executor = worker_executor.WorkerTaskExecutor(
            uuidutils.generate_uuid(),
            TEST_EXCHANGE,
            [TEST_TOPIC],
            transport='memory',
            transport_options={
                'polling_interval': POLLING_INTERVAL,
            })
        return executor

    def _start_components(self, task_classes):
        server, server_thread = self._fetch_server(task_classes)
        executor = self._fetch_executor()
        self.addCleanup(executor.stop)
        self.addCleanup(server_thread.join)
        self.addCleanup(server.stop)
        executor.start()
        server_thread.start()
        server.wait()
        return (executor, server)

    def test_execution_pipeline(self):
        executor, server = self._start_components([test_utils.TaskOneReturn])
        self.assertEqual(0, executor.wait_for_workers(timeout=WAIT_TIMEOUT))

        t = test_utils.TaskOneReturn()
        f = executor.execute_task(t, uuidutils.generate_uuid(), {})
        executor.wait_for_any([f])

        t2, _action, result = f.result()

        self.assertEqual(1, result)
        self.assertEqual(t, t2)

    def test_execution_failure_pipeline(self):
        task_classes = [
            test_utils.TaskWithFailure,
        ]
        executor, server = self._start_components(task_classes)

        t = test_utils.TaskWithFailure()
        f = executor.execute_task(t, uuidutils.generate_uuid(), {})
        executor.wait_for_any([f])

        _t2, _action, result = f.result()
        self.assertIsInstance(result, failure.Failure)
        self.assertEqual(RuntimeError, result.check(RuntimeError))
