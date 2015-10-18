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

import taskflow.engines
from taskflow import exceptions as exc
from taskflow.patterns import linear_flow
from taskflow import test
from taskflow.test import mock
from taskflow.tests import utils as test_utils
from taskflow.utils import persistence_utils as p_utils


class EngineLoadingTestCase(test.TestCase):
    def _make_dummy_flow(self):
        f = linear_flow.Flow('test')
        f.add(test_utils.TaskOneReturn("run-1"))
        return f

    def test_default_load(self):
        f = self._make_dummy_flow()
        e = taskflow.engines.load(f)
        self.assertIsNotNone(e)

    def test_unknown_load(self):
        f = self._make_dummy_flow()
        self.assertRaises(exc.NotFound, taskflow.engines.load, f,
                          engine='not_really_any_engine')

    def test_options_empty(self):
        f = self._make_dummy_flow()
        e = taskflow.engines.load(f)
        self.assertEqual({}, e.options)

    def test_options_passthrough(self):
        f = self._make_dummy_flow()
        e = taskflow.engines.load(f, pass_1=1, pass_2=2)
        self.assertEqual({'pass_1': 1, 'pass_2': 2}, e.options)


class FlowFromDetailTestCase(test.TestCase):
    def test_no_meta(self):
        _lb, flow_detail = p_utils.temporary_flow_detail()
        self.assertEqual({}, flow_detail.meta)
        self.assertRaisesRegexp(ValueError,
                                '^Cannot .* no factory information saved.$',
                                taskflow.engines.flow_from_detail,
                                flow_detail)

    def test_no_factory_in_meta(self):
        _lb, flow_detail = p_utils.temporary_flow_detail()
        self.assertRaisesRegexp(ValueError,
                                '^Cannot .* no factory information saved.$',
                                taskflow.engines.flow_from_detail,
                                flow_detail)

    def test_no_importable_function(self):
        _lb, flow_detail = p_utils.temporary_flow_detail()
        flow_detail.meta = dict(factory=dict(
            name='you can not import me, i contain spaces'
        ))
        self.assertRaisesRegexp(ImportError,
                                '^Could not import factory',
                                taskflow.engines.flow_from_detail,
                                flow_detail)

    def test_no_arg_factory(self):
        name = 'some.test.factory'
        _lb, flow_detail = p_utils.temporary_flow_detail()
        flow_detail.meta = dict(factory=dict(name=name))

        with mock.patch('oslo_utils.importutils.import_class',
                        return_value=lambda: 'RESULT') as mock_import:
            result = taskflow.engines.flow_from_detail(flow_detail)
            mock_import.assert_called_once_with(name)
        self.assertEqual('RESULT', result)

    def test_factory_with_arg(self):
        name = 'some.test.factory'
        _lb, flow_detail = p_utils.temporary_flow_detail()
        flow_detail.meta = dict(factory=dict(name=name, args=['foo']))

        with mock.patch('oslo_utils.importutils.import_class',
                        return_value=lambda x: 'RESULT %s' % x) as mock_import:
            result = taskflow.engines.flow_from_detail(flow_detail)
            mock_import.assert_called_once_with(name)
        self.assertEqual('RESULT foo', result)


def my_flow_factory(task_name):
    return test_utils.DummyTask(name=task_name)


class LoadFromFactoryTestCase(test.TestCase):

    def test_non_reimportable(self):

        def factory():
            pass

        self.assertRaisesRegexp(ValueError,
                                'Flow factory .* is not reimportable',
                                taskflow.engines.load_from_factory,
                                factory)

    def test_it_works(self):
        engine = taskflow.engines.load_from_factory(
            my_flow_factory, factory_kwargs={'task_name': 'test1'})
        self.assertIsInstance(engine._flow, test_utils.DummyTask)

        fd = engine.storage._flowdetail
        self.assertEqual('test1', fd.name)
        self.assertEqual({
            'name': '%s.my_flow_factory' % __name__,
            'args': [],
            'kwargs': {'task_name': 'test1'},
        }, fd.meta.get('factory'))

    def test_it_works_by_name(self):
        factory_name = '%s.my_flow_factory' % __name__
        engine = taskflow.engines.load_from_factory(
            factory_name, factory_kwargs={'task_name': 'test1'})
        self.assertIsInstance(engine._flow, test_utils.DummyTask)

        fd = engine.storage._flowdetail
        self.assertEqual('test1', fd.name)
        self.assertEqual({
            'name': factory_name,
            'args': [],
            'kwargs': {'task_name': 'test1'},
        }, fd.meta.get('factory'))
