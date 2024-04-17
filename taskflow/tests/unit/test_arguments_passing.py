# -*- coding: utf-8 -*-

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

import futurist
import testtools

import taskflow.engines
from taskflow import exceptions as exc
from taskflow import test
from taskflow.tests import utils
from taskflow.utils import eventlet_utils as eu

try:
    from taskflow.engines.action_engine import process_executor as pe
except ImportError:
    pe = None


class ArgumentsPassingTest(utils.EngineTestBase):

    def test_save_as(self):
        flow = utils.TaskOneReturn(name='task1', provides='first_data')
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual({'first_data': 1}, engine.storage.fetch_all())

    def test_save_all_in_one(self):
        flow = utils.TaskMultiReturn(provides='all_data')
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual({'all_data': (1, 3, 5)},
                         engine.storage.fetch_all())

    def test_save_several_values(self):
        flow = utils.TaskMultiReturn(provides=('badger', 'mushroom', 'snake'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual({
            'badger': 1,
            'mushroom': 3,
            'snake': 5
        }, engine.storage.fetch_all())

    def test_save_dict(self):
        flow = utils.TaskMultiDict(provides=set(['badger',
                                                 'mushroom',
                                                 'snake']))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual({
            'badger': 0,
            'mushroom': 1,
            'snake': 2,
        }, engine.storage.fetch_all())

    def test_bad_save_as_value(self):
        self.assertRaises(TypeError,
                          utils.TaskOneReturn,
                          name='task1', provides=object())

    def test_arguments_passing(self):
        flow = utils.TaskMultiArgOneReturn(provides='result')
        engine = self._make_engine(flow)
        engine.storage.inject({'x': 1, 'y': 4, 'z': 9, 'a': 17})
        engine.run()
        self.assertEqual({
            'x': 1, 'y': 4, 'z': 9, 'a': 17,
            'result': 14,
        }, engine.storage.fetch_all())

    def test_arguments_missing(self):
        flow = utils.TaskMultiArg()
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'x': 17})
        self.assertRaises(exc.MissingDependencies, engine.run)

    def test_partial_arguments_mapping(self):
        flow = utils.TaskMultiArgOneReturn(provides='result',
                                           rebind={'x': 'a'})
        engine = self._make_engine(flow)
        engine.storage.inject({'x': 1, 'y': 4, 'z': 9, 'a': 17})
        engine.run()
        self.assertEqual({
            'x': 1, 'y': 4, 'z': 9, 'a': 17,
            'result': 30,
        }, engine.storage.fetch_all())

    def test_argument_injection(self):
        flow = utils.TaskMultiArgOneReturn(provides='result',
                                           inject={'x': 1, 'y': 4, 'z': 9})
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual({
            'result': 14,
        }, engine.storage.fetch_all())

    def test_argument_injection_rebind(self):
        flow = utils.TaskMultiArgOneReturn(provides='result',
                                           rebind=['a', 'b', 'c'],
                                           inject={'a': 1, 'b': 4, 'c': 9})
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual({
            'result': 14,
        }, engine.storage.fetch_all())

    def test_argument_injection_required(self):
        flow = utils.TaskMultiArgOneReturn(provides='result',
                                           requires=['a', 'b', 'c'],
                                           inject={'x': 1, 'y': 4, 'z': 9,
                                                   'a': 0, 'b': 0, 'c': 0})
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual({
            'result': 14,
        }, engine.storage.fetch_all())

    def test_all_arguments_mapping(self):
        flow = utils.TaskMultiArgOneReturn(provides='result',
                                           rebind=['a', 'b', 'c'])
        engine = self._make_engine(flow)
        engine.storage.inject({
            'a': 1, 'b': 2, 'c': 3, 'x': 4, 'y': 5, 'z': 6
        })
        engine.run()
        self.assertEqual({
            'a': 1, 'b': 2, 'c': 3, 'x': 4, 'y': 5, 'z': 6,
            'result': 6,
        }, engine.storage.fetch_all())

    def test_invalid_argument_name_map(self):
        flow = utils.TaskMultiArg(rebind={'z': 'b'})
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'y': 4, 'c': 9, 'x': 17})
        self.assertRaises(exc.MissingDependencies, engine.run)

    def test_invalid_argument_name_list(self):
        flow = utils.TaskMultiArg(rebind=['a', 'z', 'b'])
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'c': 9, 'x': 17})
        self.assertRaises(exc.MissingDependencies, engine.run)

    def test_bad_rebind_args_value(self):
        self.assertRaises(TypeError,
                          utils.TaskOneArg,
                          rebind=object())

    def test_long_arg_name(self):
        flow = utils.LongArgNameTask(requires='long_arg_name',
                                     provides='result')
        engine = self._make_engine(flow)
        engine.storage.inject({'long_arg_name': 1})
        engine.run()
        self.assertEqual({
            'long_arg_name': 1, 'result': 1
        }, engine.storage.fetch_all())

    def test_revert_rebound_args_required(self):
        flow = utils.TaskMultiArg(revert_rebind={'z': 'b'})
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'y': 4, 'c': 9, 'x': 17})
        self.assertRaises(exc.MissingDependencies, engine.run)

    def test_revert_required_args_required(self):
        flow = utils.TaskMultiArg(revert_requires=['a'])
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 4, 'z': 9, 'x': 17})
        self.assertRaises(exc.MissingDependencies, engine.run)

    def test_derived_revert_args_required(self):
        flow = utils.TaskRevertExtraArgs()
        engine = self._make_engine(flow)
        engine.storage.inject({'y': 4, 'z': 9, 'x': 17})
        self.assertRaises(exc.MissingDependencies, engine.run)
        engine.storage.inject({'revert_arg': None})
        self.assertRaises(exc.ExecutionFailure, engine.run)


class SerialEngineTest(ArgumentsPassingTest, test.TestCase):

    def _make_engine(self, flow, flow_detail=None):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine='serial',
                                     backend=self.backend)


class ParallelEngineWithThreadsTest(ArgumentsPassingTest, test.TestCase):
    _EXECUTOR_WORKERS = 2

    def _make_engine(self, flow, flow_detail=None, executor=None):
        if executor is None:
            executor = 'threads'
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine='parallel',
                                     backend=self.backend,
                                     executor=executor,
                                     max_workers=self._EXECUTOR_WORKERS)


@testtools.skipIf(not eu.EVENTLET_AVAILABLE, 'eventlet is not available')
class ParallelEngineWithEventletTest(ArgumentsPassingTest, test.TestCase):

    def _make_engine(self, flow, flow_detail=None, executor=None):
        if executor is None:
            executor = futurist.GreenThreadPoolExecutor()
            self.addCleanup(executor.shutdown)
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     backend=self.backend,
                                     engine='parallel',
                                     executor=executor)


@testtools.skipIf(pe is None, 'process_executor is not available')
class ParallelEngineWithProcessTest(ArgumentsPassingTest, test.TestCase):
    _EXECUTOR_WORKERS = 2

    def _make_engine(self, flow, flow_detail=None, executor=None):
        if executor is None:
            executor = 'processes'
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     backend=self.backend,
                                     engine='parallel',
                                     executor=executor,
                                     max_workers=self._EXECUTOR_WORKERS)
