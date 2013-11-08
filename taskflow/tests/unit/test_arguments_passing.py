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

import taskflow.engines

from taskflow import exceptions as exc
from taskflow import test
from taskflow.tests import utils


class ArgumentsPassingTest(utils.EngineTestBase):

    def test_save_as(self):
        flow = utils.TaskOneReturn(name='task1', provides='first_data')
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {'first_data': 1})

    def test_save_all_in_one(self):
        flow = utils.TaskMultiReturn(provides='all_data')
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(engine.storage.fetch_all(),
                         {'all_data': (1, 3, 5)})

    def test_save_several_values(self):
        flow = utils.TaskMultiReturn(provides=('badger', 'mushroom', 'snake'))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {
            'badger': 1,
            'mushroom': 3,
            'snake': 5
        })

    def test_save_dict(self):
        flow = utils.TaskMultiDictk(provides=set(['badger',
                                                  'mushroom',
                                                  'snake']))
        engine = self._make_engine(flow)
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {
            'badger': 0,
            'mushroom': 1,
            'snake': 2,
        })

    def test_bad_save_as_value(self):
        with self.assertRaises(TypeError):
            utils.TaskOneReturn(name='task1', provides=object())

    def test_arguments_passing(self):
        flow = utils.TaskMultiArgOneReturn(provides='result')
        engine = self._make_engine(flow)
        engine.storage.inject({'x': 1, 'y': 4, 'z': 9, 'a': 17})
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {
            'x': 1, 'y': 4, 'z': 9, 'a': 17,
            'result': 14,
        })

    def test_arguments_missing(self):
        flow = utils.TaskMultiArg()
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'x': 17})
        with self.assertRaises(exc.MissingDependencies):
            engine.run()

    def test_partial_arguments_mapping(self):
        flow = utils.TaskMultiArgOneReturn(provides='result',
                                           rebind={'x': 'a'})
        engine = self._make_engine(flow)
        engine.storage.inject({'x': 1, 'y': 4, 'z': 9, 'a': 17})
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {
            'x': 1, 'y': 4, 'z': 9, 'a': 17,
            'result': 30,
        })

    def test_all_arguments_mapping(self):
        flow = utils.TaskMultiArgOneReturn(provides='result',
                                           rebind=['a', 'b', 'c'])
        engine = self._make_engine(flow)
        engine.storage.inject({
            'a': 1, 'b': 2, 'c': 3, 'x': 4, 'y': 5, 'z': 6
        })
        engine.run()
        self.assertEqual(engine.storage.fetch_all(), {
            'a': 1, 'b': 2, 'c': 3, 'x': 4, 'y': 5, 'z': 6,
            'result': 6,
        })

    def test_invalid_argument_name_map(self):
        flow = utils.TaskMultiArg(rebind={'z': 'b'})
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'y': 4, 'c': 9, 'x': 17})
        with self.assertRaises(exc.MissingDependencies):
            engine.run()

    def test_invalid_argument_name_list(self):
        flow = utils.TaskMultiArg(rebind=['a', 'z', 'b'])
        engine = self._make_engine(flow)
        engine.storage.inject({'a': 1, 'b': 4, 'c': 9, 'x': 17})
        with self.assertRaises(exc.MissingDependencies):
            engine.run()

    def test_bad_rebind_args_value(self):
        with self.assertRaises(TypeError):
            utils.TaskOneArg(rebind=object())


class SingleThreadedEngineTest(ArgumentsPassingTest,
                               test.TestCase):
    def _make_engine(self, flow, flow_detail=None):
        return taskflow.engines.load(flow,
                                     flow_detail=flow_detail,
                                     engine_conf='serial',
                                     backend=self.backend)


class MultiThreadedEngineTest(ArgumentsPassingTest,
                              test.TestCase):
    def _make_engine(self, flow, flow_detail=None, executor=None):
        engine_conf = dict(engine='parallel',
                           executor=executor)
        return taskflow.engines.load(flow, flow_detail=flow_detail,
                                     engine_conf=engine_conf,
                                     backend=self.backend)
