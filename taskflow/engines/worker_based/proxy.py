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

import collections
import logging
import socket

import kombu
import six

from taskflow.engines.worker_based import dispatcher
from taskflow.utils import threading_utils

LOG = logging.getLogger(__name__)

# NOTE(skudriashev): A timeout of 1 is often used in environments where
# the socket can get "stuck", and is a best practice for Kombu consumers.
DRAIN_EVENTS_PERIOD = 1

# Helper objects returned when requested to get connection details, used
# instead of returning the raw results from the kombu connection objects
# themselves so that a person can not mutate those objects (which would be
# bad).
_ConnectionDetails = collections.namedtuple('_ConnectionDetails',
                                            ['uri', 'transport'])
_TransportDetails = collections.namedtuple('_TransportDetails',
                                           ['options', 'driver_type',
                                            'driver_name', 'driver_version'])


class Proxy(object):
    """A proxy processes messages from/to the named exchange."""

    def __init__(self, topic, exchange_name, type_handlers, on_wait=None,
                 **kwargs):
        self._topic = topic
        self._exchange_name = exchange_name
        self._on_wait = on_wait
        self._running = threading_utils.Event()
        self._dispatcher = dispatcher.TypeDispatcher(type_handlers)
        self._dispatcher.add_requeue_filter(
            # NOTE(skudriashev): Process all incoming messages only if proxy is
            # running, otherwise requeue them.
            lambda data, message: not self.is_running)

        url = kwargs.get('url')
        transport = kwargs.get('transport')
        transport_opts = kwargs.get('transport_options')

        self._drain_events_timeout = DRAIN_EVENTS_PERIOD
        if transport == 'memory' and transport_opts:
            polling_interval = transport_opts.get('polling_interval')
            if polling_interval is not None:
                self._drain_events_timeout = polling_interval

        # create connection
        self._conn = kombu.Connection(url, transport=transport,
                                      transport_options=transport_opts)

        # create exchange
        self._exchange = kombu.Exchange(name=self._exchange_name,
                                        durable=False,
                                        auto_delete=True)

    @property
    def connection_details(self):
        # The kombu drivers seem to use 'N/A' when they don't have a version...
        driver_version = self._conn.transport.driver_version()
        if driver_version and driver_version.lower() == 'n/a':
            driver_version = None
        if self._conn.transport_options:
            transport_options = self._conn.transport_options.copy()
        else:
            transport_options = {}
        transport = _TransportDetails(
            options=transport_options,
            driver_type=self._conn.transport.driver_type,
            driver_name=self._conn.transport.driver_name,
            driver_version=driver_version)
        return _ConnectionDetails(
            uri=self._conn.as_uri(include_password=False),
            transport=transport)

    @property
    def is_running(self):
        """Return whether the proxy is running."""
        return self._running.is_set()

    def _make_queue(self, name, exchange, **kwargs):
        """Make named queue for the given exchange."""
        return kombu.Queue(name="%s_%s" % (self._exchange_name, name),
                           exchange=exchange,
                           routing_key=name,
                           durable=False,
                           auto_delete=True,
                           **kwargs)

    def publish(self, msg, routing_key, **kwargs):
        """Publish message to the named exchange with given routing key."""
        if isinstance(routing_key, six.string_types):
            routing_keys = [routing_key]
        else:
            routing_keys = routing_key
        LOG.debug("Sending '%s' using routing keys %s", msg, routing_keys)
        with kombu.producers[self._conn].acquire(block=True) as producer:
            for routing_key in routing_keys:
                queue = self._make_queue(routing_key, self._exchange)
                producer.publish(body=msg.to_dict(),
                                 routing_key=routing_key,
                                 exchange=self._exchange,
                                 declare=[queue],
                                 type=msg.TYPE,
                                 **kwargs)

    def start(self):
        """Start proxy."""
        LOG.info("Starting to consume from the '%s' exchange.",
                 self._exchange_name)
        with kombu.connections[self._conn].acquire(block=True) as conn:
            queue = self._make_queue(self._topic, self._exchange, channel=conn)
            with conn.Consumer(queues=queue,
                               callbacks=[self._dispatcher.on_message]):
                self._running.set()
                while self.is_running:
                    try:
                        conn.drain_events(timeout=self._drain_events_timeout)
                    except socket.timeout:
                        pass
                    if self._on_wait is not None:
                        self._on_wait()

    def wait(self):
        """Wait until proxy is started."""
        self._running.wait()

    def stop(self):
        """Stop proxy."""
        self._running.clear()
