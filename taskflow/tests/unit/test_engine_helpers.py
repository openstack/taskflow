# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import mock

from taskflow import test
from taskflow.tests import utils as test_utils
from taskflow.utils import persistence_utils as p_utils

import taskflow.engines


class FlowFromDetailTestCase(test.TestCase):
    def test_no_meta(self):
        _lb, flow_detail = p_utils.temporary_flow_detail()
        self.assertIs(flow_detail.meta, None)
        expected_msg = '^Cannot .* no factory information saved.$'
        with self.assertRaisesRegexp(ValueError, expected_msg):
            taskflow.engines.flow_from_detail(flow_detail)

    def test_no_factory_in_meta(self):
        _lb, flow_detail = p_utils.temporary_flow_detail()
        flow_detail.meta = {}
        expected_msg = '^Cannot .* no factory information saved.$'
        with self.assertRaisesRegexp(ValueError, expected_msg):
            taskflow.engines.flow_from_detail(flow_detail)

    def test_no_importable_function(self):
        _lb, flow_detail = p_utils.temporary_flow_detail()
        flow_detail.meta = dict(factory=dict(
            name='you can not import me, i contain spaces'
        ))
        expected_msg = '^Could not import factory'
        with self.assertRaisesRegexp(ImportError, expected_msg):
            taskflow.engines.flow_from_detail(flow_detail)

    def test_no_arg_factory(self):
        name = 'some.test.factory'
        _lb, flow_detail = p_utils.temporary_flow_detail()
        flow_detail.meta = dict(factory=dict(name=name))

        with mock.patch('taskflow.openstack.common.importutils.import_class',
                        return_value=lambda: 'RESULT') as mock_import:
            result = taskflow.engines.flow_from_detail(flow_detail)
            mock_import.assert_called_onec_with(name)
        self.assertEqual(result, 'RESULT')

    def test_factory_with_arg(self):
        name = 'some.test.factory'
        _lb, flow_detail = p_utils.temporary_flow_detail()
        flow_detail.meta = dict(factory=dict(name=name, args=['foo']))

        with mock.patch('taskflow.openstack.common.importutils.import_class',
                        return_value=lambda x: 'RESULT %s' % x) as mock_import:
            result = taskflow.engines.flow_from_detail(flow_detail)
            mock_import.assert_called_onec_with(name)
        self.assertEqual(result, 'RESULT foo')


def my_flow_factory(task_name):
    return test_utils.DummyTask(name=task_name)


class LoadFromFactoryTestCase(test.TestCase):

    def test_non_reimportable(self):
        def factory():
            pass
        with self.assertRaisesRegexp(ValueError,
                                     'Flow factory .* is not reimportable'):
            taskflow.engines.load_from_factory(factory)

    def test_it_works(self):
        engine = taskflow.engines.load_from_factory(
            my_flow_factory, factory_kwargs={'task_name': 'test1'})
        self.assertIsInstance(engine._flow, test_utils.DummyTask)

        fd = engine.storage._flowdetail
        self.assertEqual(fd.name, 'test1')
        self.assertEqual(fd.meta.get('factory'), {
            'name': '%s.my_flow_factory' % __name__,
            'args': [],
            'kwargs': {'task_name': 'test1'},
        })

    def test_it_works_by_name(self):
        factory_name = '%s.my_flow_factory' % __name__
        engine = taskflow.engines.load_from_factory(
            factory_name, factory_kwargs={'task_name': 'test1'})
        self.assertIsInstance(engine._flow, test_utils.DummyTask)

        fd = engine.storage._flowdetail
        self.assertEqual(fd.name, 'test1')
        self.assertEqual(fd.meta.get('factory'), {
            'name': factory_name,
            'args': [],
            'kwargs': {'task_name': 'test1'},
        })
