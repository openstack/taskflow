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

import logging
import os
import platform
import socket
import string
import sys

from concurrent import futures

from taskflow.engines.worker_based import endpoint
from taskflow.engines.worker_based import server
from taskflow import task as t_task
from taskflow.utils import reflection
from taskflow.utils import threading_utils as tu
from taskflow import version

BANNER_TEMPLATE = string.Template("""
TaskFlow v${version} WBE worker.
Connection details:
  Driver = $transport_driver
  Exchange = $exchange
  Topic = $topic
  Transport = $transport_type
  Uri = $connection_uri
Powered by:
  Executor = $executor_type
  Thread count = $executor_thread_count
Supported endpoints:$endpoints
System details:
  Hostname = $hostname
  Pid = $pid
  Platform = $platform
  Python = $python
  Thread id = $thread_id
""".strip())
BANNER_TEMPLATE.defaults = {
    # These values may not be possible to fetch/known, default to unknown...
    'pid': '???',
    'hostname': '???',
    'executor_thread_count': '???',
    'endpoints': ' %s' % ([]),
    # These are static (avoid refetching...)
    'version': version.version_string(),
    'python': sys.version.split("\n", 1)[0].strip(),
}

LOG = logging.getLogger(__name__)


class Worker(object):
    """Worker that can be started on a remote host for handling tasks requests.

    :param url: broker url
    :param exchange: broker exchange name
    :param topic: topic name under which worker is stated
    :param tasks: task list that worker is capable of performing, items in
        the list can be one of the following types; 1, a string naming the
        python module name to search for tasks in or the task class name; 2, a
        python module  to search for tasks in; 3, a task class object that
        will be used to create tasks from.
    :param executor: custom executor object that can used for processing
        requests in separate threads (if not provided one will be created)
    :param threads_count: threads count to be passed to the default executor
    :param transport: transport to be used (e.g. amqp, memory, etc.)
    :param transport_options: transport specific options
    """

    def __init__(self, exchange, topic, tasks, executor=None, **kwargs):
        self._topic = topic
        self._executor = executor
        self._owns_executor = False
        self._threads_count = -1
        if self._executor is None:
            if 'threads_count' in kwargs:
                self._threads_count = int(kwargs.pop('threads_count'))
                if self._threads_count <= 0:
                    raise ValueError("threads_count provided must be > 0")
            else:
                self._threads_count = tu.get_optimal_thread_count()
            self._executor = futures.ThreadPoolExecutor(self._threads_count)
            self._owns_executor = True
        self._endpoints = self._derive_endpoints(tasks)
        self._exchange = exchange
        self._server = server.Server(topic, exchange, self._executor,
                                     self._endpoints, **kwargs)

    @staticmethod
    def _derive_endpoints(tasks):
        """Derive endpoints from list of strings, classes or packages."""
        derived_tasks = reflection.find_subclasses(tasks, t_task.BaseTask)
        return [endpoint.Endpoint(task) for task in derived_tasks]

    def _generate_banner(self):
        """Generates a banner that can be useful to display before running."""
        tpl_params = {}
        connection_details = self._server.connection_details
        transport = connection_details.transport
        if transport.driver_version:
            transport_driver = "%s v%s" % (transport.driver_name,
                                           transport.driver_version)
        else:
            transport_driver = transport.driver_name
        tpl_params['transport_driver'] = transport_driver
        tpl_params['exchange'] = self._exchange
        tpl_params['topic'] = self._topic
        tpl_params['transport_type'] = transport.driver_type
        tpl_params['connection_uri'] = connection_details.uri
        tpl_params['executor_type'] = reflection.get_class_name(self._executor)
        if self._threads_count != -1:
            tpl_params['executor_thread_count'] = self._threads_count
        if self._endpoints:
            pretty_endpoints = []
            for ep in self._endpoints:
                pretty_endpoints.append("  - %s" % ep)
            # This ensures there is a newline before the list...
            tpl_params['endpoints'] = "\n" + "\n".join(pretty_endpoints)
        try:
            tpl_params['hostname'] = socket.getfqdn()
        except socket.error:
            pass
        try:
            tpl_params['pid'] = os.getpid()
        except OSError:
            pass
        tpl_params['platform'] = platform.platform()
        tpl_params['thread_id'] = tu.get_ident()
        return BANNER_TEMPLATE.substitute(BANNER_TEMPLATE.defaults,
                                          **tpl_params)

    def run(self, display_banner=True):
        """Runs the worker."""
        if display_banner:
            for line in self._generate_banner().splitlines():
                LOG.info(line)
        self._server.start()

    def wait(self):
        """Wait until worker is started."""
        self._server.wait()

    def stop(self):
        """Stop worker."""
        self._server.stop()
        if self._owns_executor:
            self._executor.shutdown()
