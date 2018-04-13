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


class WorkerBasedActionEngine(engine.ActionEngine):
    """Worker based action engine.

    Specific backend options (extracted from provided engine options):

    :param exchange: broker exchange exchange name in which executor / worker
                     communication is performed
    :param url: broker connection url (see format in kombu documentation)
    :param topics: list of workers topics to communicate with (this will also
                   be learned by listening to the notifications that workers
                   emit).
    :param transport: transport to be used (e.g. amqp, memory, etc.)
    :param transition_timeout: numeric value (or None for infinite) to wait
                               for submitted remote requests to transition out
                               of the (PENDING, WAITING) request states. When
                               expired the associated task the request was made
                               for will have its result become a
                               :py:class:`~taskflow.exceptions.RequestTimeout`
                               exception instead of its normally returned
                               value (or raised exception).
    :param transport_options: transport specific options (see:
                              http://kombu.readthedocs.org/ for what these
                              options imply and are expected to be)
    :param retry_options: retry specific options
                          (see: :py:attr:`~.proxy.Proxy.DEFAULT_RETRY_OPTIONS`)
    :param worker_expiry: numeric value (or negative/zero/None for
                          infinite) that defines the number of seconds to
                          continue to send messages to workers that
                          have **not** responded back to a prior
                          notification/ping request (this defaults
                          to 60 seconds).
    """

    def __init__(self, flow, flow_detail, backend, options):
        super(WorkerBasedActionEngine, self).__init__(flow, flow_detail,
                                                      backend, options)
        # This ensures that any provided executor will be validated before
        # we get to far in the compilation/execution pipeline...
        self._task_executor = self._fetch_task_executor(self._options,
                                                        self._flow_detail)

    @classmethod
    def _fetch_task_executor(cls, options, flow_detail):
        try:
            e = options['executor']
            if not isinstance(e, executor.WorkerTaskExecutor):
                raise TypeError("Expected an instance of type '%s' instead of"
                                " type '%s' for 'executor' option"
                                % (executor.WorkerTaskExecutor, type(e)))
            return e
        except KeyError:
            return executor.WorkerTaskExecutor(
                uuid=flow_detail.uuid,
                url=options.get('url'),
                exchange=options.get('exchange', 'default'),
                retry_options=options.get('retry_options'),
                topics=options.get('topics', []),
                transport=options.get('transport'),
                transport_options=options.get('transport_options'),
                transition_timeout=options.get('transition_timeout',
                                               pr.REQUEST_TIMEOUT),
                worker_expiry=options.get('worker_expiry',
                                          pr.EXPIRES_AFTER),
            )
