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

import os
import platform
import socket
import sys

import futurist
from oslo_utils import reflection

from taskflow.engines.worker_based import endpoint
from taskflow.engines.worker_based import server
from taskflow import logging
from taskflow import task as t_task
from taskflow.utils import banner
from taskflow.utils import misc
from taskflow.utils import threading_utils as tu

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
    :param threads_count: threads count to be passed to the
                          default executor (used only if an executor is not
                          passed in)
    :param transport: transport to be used (e.g. amqp, memory, etc.)
    :param transport_options: transport specific options (see:
                              http://kombu.readthedocs.org/ for what these
                              options imply and are expected to be)
    :param retry_options: retry specific options
                          (see: :py:attr:`~.proxy.Proxy.DEFAULT_RETRY_OPTIONS`)
    """

    def __init__(self, exchange, topic, tasks,
                 executor=None, threads_count=None, url=None,
                 transport=None, transport_options=None,
                 retry_options=None):
        self._topic = topic
        self._executor = executor
        self._owns_executor = False
        if self._executor is None:
            self._executor = futurist.ThreadPoolExecutor(
                max_workers=threads_count)
            self._owns_executor = True
        self._endpoints = self._derive_endpoints(tasks)
        self._exchange = exchange
        self._server = server.Server(topic, exchange, self._executor,
                                     self._endpoints, url=url,
                                     transport=transport,
                                     transport_options=transport_options,
                                     retry_options=retry_options)

    @staticmethod
    def _derive_endpoints(tasks):
        """Derive endpoints from list of strings, classes or packages."""
        derived_tasks = misc.find_subclasses(tasks, t_task.Task)
        return [endpoint.Endpoint(task) for task in derived_tasks]

    @misc.cachedproperty
    def banner(self):
        """A banner that can be useful to display before running."""
        connection_details = self._server.connection_details
        transport = connection_details.transport
        if transport.driver_version:
            transport_driver = "%s v%s" % (transport.driver_name,
                                           transport.driver_version)
        else:
            transport_driver = transport.driver_name
        try:
            hostname = socket.getfqdn()
        except socket.error:
            hostname = "???"
        try:
            pid = os.getpid()
        except OSError:
            pid = "???"
        chapters = {
            'Connection details': {
                'Driver': transport_driver,
                'Exchange': self._exchange,
                'Topic': self._topic,
                'Transport': transport.driver_type,
                'Uri': connection_details.uri,
            },
            'Powered by': {
                'Executor': reflection.get_class_name(self._executor),
                'Thread count': getattr(self._executor, 'max_workers', "???"),
            },
            'Supported endpoints': [str(ep) for ep in self._endpoints],
            'System details': {
                'Hostname': hostname,
                'Pid': pid,
                'Platform': platform.platform(),
                'Python': sys.version.split("\n", 1)[0].strip(),
                'Thread id': tu.get_ident(),
            },
        }
        return banner.make_banner('WBE worker', chapters)

    def run(self, display_banner=True, banner_writer=None):
        """Runs the worker."""
        if display_banner:
            if banner_writer is None:
                for line in self.banner.splitlines():
                    LOG.info(line)
            else:
                banner_writer(self.banner)
        self._server.start()

    def wait(self):
        """Wait until worker is started."""
        self._server.wait()

    def stop(self):
        """Stop worker."""
        self._server.stop()
        if self._owns_executor:
            self._executor.shutdown()


if __name__ == '__main__':
    import argparse
    import logging as log
    parser = argparse.ArgumentParser()
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--connection-url", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--task", action='append',
                        metavar="TASK", default=[])
    parser.add_argument("-v", "--verbose", action='store_true')
    args = parser.parse_args()
    if args.verbose:
        log.basicConfig(level=logging.DEBUG, format="")
    w = Worker(args.exchange, args.topic, args.task, url=args.connection_url)
    w.run()
