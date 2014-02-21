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

from amqp import exceptions as amqp_exc

from taskflow.engines.worker_based import protocol as pr

LOG = logging.getLogger(__name__)

# NOTE(skudriashev): A timeout of 1 is often used in environments where
# the socket can get "stuck", and is a best practice for Kombu consumers.
DRAIN_EVENTS_PERIOD = 1


class Proxy(object):
    """Proxy picks up messages from the named exchange, calls on_message
    callback when new message received and is used to publish messages.
    """

    def __init__(self, uuid, exchange_name, on_message, on_wait=None,
                 **kwargs):
        self._uuid = uuid
        self._exchange_name = exchange_name
        self._on_message = on_message
        self._on_wait = on_wait
        self._running = threading.Event()
        self._url = kwargs.get('url')
        self._transport = kwargs.get('transport')
        self._transport_opts = kwargs.get('transport_options')

        # create connection
        self._conn = kombu.Connection(self._url, transport=self._transport,
                                      transport_options=self._transport_opts)

        # create exchange
        self._exchange = kombu.Exchange(name=self._exchange_name,
                                        channel=self._conn,
                                        durable=False,
                                        auto_delete=True)

    @property
    def is_running(self):
        """Return whether proxy is running."""
        return self._running.is_set()

    def _make_queue(self, name, exchange, **kwargs):
        """Make named queue for the given exchange."""
        queue_arguments = {'x-expires': pr.QUEUE_EXPIRE_TIMEOUT * 1000}
        return kombu.Queue(name="%s_%s" % (self._exchange_name, name),
                           exchange=exchange,
                           routing_key=name,
                           durable=False,
                           queue_arguments=queue_arguments,
                           **kwargs)

    def publish(self, msg, task_uuid, routing_key, **kwargs):
        """Publish message to the named exchange with routing key."""
        with kombu.producers[self._conn].acquire(block=True) as producer:
            queue = self._make_queue(routing_key, self._exchange)
            producer.publish(body=msg,
                             routing_key=routing_key,
                             exchange=self._exchange,
                             correlation_id=task_uuid,
                             declare=[queue],
                             **kwargs)

    def start(self):
        """Start proxy."""
        LOG.info("Starting to consume from the '%s' exchange." %
                 self._exchange_name)
        with kombu.connections[self._conn].acquire(block=True) as conn:
            queue = self._make_queue(self._uuid, self._exchange, channel=conn)
            try:
                with conn.Consumer(queues=queue,
                                   callbacks=[self._on_message]):
                    self._running.set()
                    while self.is_running:
                        try:
                            conn.drain_events(timeout=DRAIN_EVENTS_PERIOD)
                        except socket.timeout:
                            pass
                        if self._on_wait is not None:
                            self._on_wait()
            finally:
                try:
                    queue.delete(if_unused=True)
                except (amqp_exc.PreconditionFailed, amqp_exc.NotFound):
                    pass
                except Exception as e:
                    LOG.error("Failed to delete the '%s' queue: %s" %
                              (queue.name, e))
                try:
                    self._exchange.delete(if_unused=True)
                except (amqp_exc.PreconditionFailed, amqp_exc.NotFound):
                    pass
                except Exception as e:
                    LOG.error("Failed to delete the '%s' exchange: %s" %
                              (self._exchange.name, e))

    def wait(self):
        """Wait until proxy is started."""
        self._running.wait()

    def stop(self):
        """Stop proxy."""
        self._running.clear()
