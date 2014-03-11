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

from taskflow.engines.action_engine import engine
from taskflow.engines.worker_based import executor
from taskflow import storage as t_storage


class WorkerBasedActionEngine(engine.ActionEngine):
    _storage_cls = t_storage.SingleThreadedStorage

    def _task_executor_cls(self):
        return executor.WorkerTaskExecutor(**self._executor_config)

    def __init__(self, flow, flow_detail, backend, conf):
        self._executor_config = {
            'uuid': flow_detail.uuid,
            'url': conf.get('url'),
            'exchange': conf.get('exchange', 'default'),
            'topics': conf.get('topics', []),
            'transport': conf.get('transport'),
            'transport_options': conf.get('transport_options')
        }
        super(WorkerBasedActionEngine, self).__init__(
            flow, flow_detail, backend, conf)
