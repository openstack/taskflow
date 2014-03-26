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

import kombu
import logging
import socket
import threading

import six

LOG = logging.getLogger(__name__)

# NOTE(skudriashev): A timeout of 1 is often used in environments where
# the socket can get "stuck", and is a best practice for Kombu consumers.
DRAIN_EVENTS_PERIOD = 1


class Proxy(object):
    """Proxy picks up messages from the named exchange, calls on_message
    callback when new message received and is used to publish messages.
    """

    def __init__(self, topic, exchange_name, on_message, on_wait=None,
                 **kwargs):
        self._topic = topic
        self._exchange_name = exchange_name
        self._on_message = on_message
        self._on_wait = on_wait
        self._running = threading.Event()
        self._url = kwargs.get('url')
        self._transport = kwargs.get('transport')
        self._transport_opts = kwargs.get('transport_options')

        self._drain_events_timeout = DRAIN_EVENTS_PERIOD
        if self._transport == 'memory' and self._transport_opts:
            polling_interval = self._transport_opts.get('polling_interval')
            if polling_interval:
                self._drain_events_timeout = polling_interval

        # create connection
        self._conn = kombu.Connection(self._url, transport=self._transport,
                                      transport_options=self._transport_opts)

        # create exchange
        self._exchange = kombu.Exchange(name=self._exchange_name,
                                        durable=False,
                                        auto_delete=True)

    @property
    def is_running(self):
        """Return whether proxy is running."""
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
        """Publish message to the named exchange with routing key."""
        LOG.debug("Sending %s", msg)
        if isinstance(routing_key, six.string_types):
            routing_keys = [routing_key]
        else:
            routing_keys = routing_key
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
                               callbacks=[self._on_message]):
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
