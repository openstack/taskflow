# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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

from taskflow import functor_task
from taskflow.patterns import linear_flow
from taskflow import test


def add(a, b):
    return a + b


class BunchOfFunctions(object):

    def __init__(self, values):
        self.values = values

    def run_one(self, *args, **kwargs):
        self.values.append('one')

    def revert_one(self, *args, **kwargs):
        self.values.append('revert one')

    def run_fail(self, *args, **kwargs):
        self.values.append('fail')
        raise RuntimeError('Woot!')


class FunctorTaskTest(test.TestCase):

    def test_simple(self):
        task = functor_task.FunctorTask(add)
        self.assertEquals(task.name, __name__ + '.add')

    def test_other_name(self):
        task = functor_task.FunctorTask(add, name='my task')
        self.assertEquals(task.name, 'my task')

    def test_it_runs(self):
        values = []
        bof = BunchOfFunctions(values)
        t = functor_task.FunctorTask

        flow = linear_flow.Flow('test')
        flow.add_many((
            t(bof.run_one, revert_with=bof.revert_one),
            t(bof.run_fail)
        ))
        with self.assertRaisesRegexp(RuntimeError, '^Woot'):
            flow.run(None)
        self.assertEquals(values, ['one', 'fail', 'revert one'])
