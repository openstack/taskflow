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

import contextlib
import mock
import time

from taskflow import task
from taskflow import test

import taskflow.engines
from taskflow import exceptions as exc
from taskflow.listeners import timing
from taskflow.patterns import linear_flow as lf
from taskflow.persistence.backends import impl_memory
from taskflow.tests import utils as t_utils
from taskflow.utils import persistence_utils as p_utils


class SleepyTask(task.Task):
    def __init__(self, name, sleep_for=0.0):
        super(SleepyTask, self).__init__(name=name)
        self._sleep_for = float(sleep_for)

    def execute(self):
        if self._sleep_for <= 0:
            return
        else:
            time.sleep(self._sleep_for)


class TestDuration(test.TestCase):
    def make_engine(self, flow, flow_detail, backend):
        e = taskflow.engines.load(flow,
                                  flow_detail=flow_detail,
                                  backend=backend)
        e.compile()
        return e

    def test_duration(self):
        with contextlib.closing(impl_memory.MemoryBackend({})) as be:
            flow = lf.Flow("test")
            flow.add(SleepyTask("test-1", sleep_for=0.1))
            (lb, fd) = p_utils.temporary_flow_detail(be)
            e = self.make_engine(flow, fd, be)
            with timing.TimingListener(e):
                e.run()
            t_uuid = e.storage.get_atom_uuid("test-1")
            td = fd.find(t_uuid)
            self.assertIsNotNone(td)
            self.assertIsNotNone(td.meta)
            self.assertIn('duration', td.meta)
            self.assertGreaterEqual(0.1, td.meta['duration'])

    @mock.patch.object(timing.LOG, 'warn')
    def test_record_ending_exception(self, mocked_warn):
        with contextlib.closing(impl_memory.MemoryBackend({})) as be:
            flow = lf.Flow("test")
            flow.add(t_utils.TaskNoRequiresNoReturns("test-1"))
            (lb, fd) = p_utils.temporary_flow_detail(be)
            e = self.make_engine(flow, fd, be)
            timing_listener = timing.TimingListener(e)
            with mock.patch.object(timing_listener._engine.storage,
                                   'update_atom_metadata') as mocked_uam:
                mocked_uam.side_effect = exc.StorageFailure('Woot!')
                with timing_listener:
                    e.run()
        mocked_warn.assert_called_once_with(mock.ANY, mock.ANY, 'test-1',
                                            exc_info=True)
