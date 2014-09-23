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
from taskflow.engines.worker_based import protocol as pr
from taskflow import storage as t_storage


class WorkerBasedActionEngine(engine.ActionEngine):
    """Worker based action engine.

    Specific backend configuration:

    :param exchange: broker exchange exchange name in which executor / worker
                     communication is performed
    :param url: broker connection url (see format in kombu documentation)
    :param topics: list of workers topics to communicate with (this will also
                   be learned by listening to the notifications that workers
                   emit).
    :param transport: transport to be used (e.g. amqp, memory, etc.)
    :param transport_options: transport specific options
    :param transition_timeout: numeric value (or None for infinite) to wait
                               for submitted remote requests to transition out
                               of the (PENDING, WAITING) request states. When
                               expired the associated task the request was made
                               for will have its result become a
                               `RequestTimeout` exception instead of its
                               normally returned value (or raised exception).
    """

    _storage_factory = t_storage.SingleThreadedStorage

    def _task_executor_factory(self):
        if self._executor is not None:
            return self._executor
        return executor.WorkerTaskExecutor(
            uuid=self._flow_detail.uuid,
            url=self._conf.get('url'),
            exchange=self._conf.get('exchange', 'default'),
            topics=self._conf.get('topics', []),
            transport=self._conf.get('transport'),
            transport_options=self._conf.get('transport_options'),
            transition_timeout=self._conf.get('transition_timeout',
                                              pr.REQUEST_TIMEOUT))

    def __init__(self, flow, flow_detail, backend, conf, **kwargs):
        super(WorkerBasedActionEngine, self).__init__(
            flow, flow_detail, backend, conf)
        self._executor = kwargs.get('executor')
