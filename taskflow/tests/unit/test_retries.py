# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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

import testtools

from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf

import taskflow.engines

from taskflow import test
from taskflow.tests import utils

from taskflow.utils import eventlet_utils as eu


class RetryTest(utils.EngineTestBase):

    def test_run_empty_linear_flow(self):
        flow = lf.Flow('flow-1', utils.OneReturnRetry(provides='x'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'x': 1})

    def test_run_empty_unordered_flow(self):
        flow = uf.Flow('flow-1', utils.OneReturnRetry(provides='x'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'x': 1})

    def test_run_empty_graph_flow(self):
        flow = gf.Flow('flow-1', utils.OneReturnRetry(provides='x'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'x': 1})


class SingleThreadedEngineTest(RetryTest,
                               test.TestCase):
    def _make_engine(self, flow, flow_detail=None):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine_conf='serial',
                                     backend=self.backend)


class MultiThreadedEngineTest(RetryTest,
                              test.TestCase):
    def _make_engine(self, flow, flow_detail=None, executor=None):
        engine_conf = dict(engine='parallel',
                           executor=executor)
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     engine_conf=engine_conf,
                                     backend=self.backend)


@testtools.skipIf(not eu.EVENTLET_AVAILABLE, 'eventlet is not available')
class ParallelEngineWithEventletTest(RetryTest,
                                     test.TestCase):

    def _make_engine(self, flow, flow_detail=None, executor=None):
        if executor is None:
            executor = eu.GreenExecutor()
        engine_conf = dict(engine='parallel',
                           executor=executor)
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     engine_conf=engine_conf,
                                     backend=self.backend)
