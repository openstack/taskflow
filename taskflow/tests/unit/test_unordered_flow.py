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

from taskflow.patterns import unordered_flow as uf
from taskflow import task
from taskflow import test

from taskflow.tests import utils


class UnorderedFlowTest(test.TestCase):
    def _make_engine(self, flow):
        return taskflow.engines.load(flow, store={'context': {}})

    def test_result_access(self):

        class DoApply(task.Task):
            default_provides = ('a', 'b')

            def execute(self):
                return [1, 2]

        wf = uf.Flow("the-test-action")
        wf.add(DoApply())

        e = self._make_engine(wf)
        e.run()
        data = e.storage.fetch_all()
        self.assertIn('a', data)
        self.assertIn('b', data)
        self.assertEqual(2, data['b'])
        self.assertEqual(1, data['a'])

    def test_reverting_flow(self):
        wf = uf.Flow("the-test-action")
        wf.add(utils.make_reverting_task('1'))
        wf.add(utils.make_reverting_task('2', blowup=True))
        e = self._make_engine(wf)
        self.assertRaises(Exception, e.run)

    def test_functor_flow(self):

        class DoApply1(task.Task):
            default_provides = ('a', 'b', 'c')

            def execute(self, context):
                context['1'] = True
                return ['a', 'b', 'c']

        class DoApply2(task.Task):
            def execute(self, context):
                context['2'] = True

        wf = uf.Flow("the-test-action")
        wf.add(DoApply1())
        wf.add(DoApply2())
        e = self._make_engine(wf)
        e.run()
        self.assertEqual(2, len(e.storage.fetch('context')))
