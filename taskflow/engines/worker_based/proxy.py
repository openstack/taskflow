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
import threading

import kombu
from kombu import exceptions as kombu_exceptions
import six

from taskflow.engines.worker_based import dispatcher
from taskflow import logging

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
    """A proxy processes messages from/to the named exchange.

    For **internal** usage only (not for public consumption).
    """

    DEFAULT_RETRY_OPTIONS = {
        # The number of seconds we start sleeping for.
        'interval_start': 1,
        # How many seconds added to the interval for each retry.
        'interval_step': 1,
        # Maximum number of seconds to sleep between each retry.
        'interval_max': 1,
        # Maximum number of times to retry.
        'max_retries': 3,
    }
    """Settings used (by default) to reconnect under transient failures.

    See: http://kombu.readthedocs.org/ (and connection ``ensure_options``) for
    what these values imply/mean...
    """

    # This is the only provided option that should be an int, the others
    # are allowed to be floats; used when we check that the user-provided
    # value is valid...
    _RETRY_INT_OPTS = frozenset(['max_retries'])

    def __init__(self, topic, exchange,
                 type_handlers=None, on_wait=None, url=None,
                 transport=None, transport_options=None,
                 retry_options=None):
        self._topic = topic
        self._exchange_name = exchange
        self._on_wait = on_wait
        self._running = threading.Event()
        self._dispatcher = dispatcher.TypeDispatcher(
            # NOTE(skudriashev): Process all incoming messages only if proxy is
            # running, otherwise requeue them.
            requeue_filters=[lambda data, message: not self.is_running],
            type_handlers=type_handlers)

        ensure_options = self.DEFAULT_RETRY_OPTIONS.copy()
        if retry_options is not None:
            # Override the defaults with any user provided values...
            for k in set(six.iterkeys(ensure_options)):
                if k in retry_options:
                    # Ensure that the right type is passed in...
                    val = retry_options[k]
                    if k in self._RETRY_INT_OPTS:
                        tmp_val = int(val)
                    else:
                        tmp_val = float(val)
                    if tmp_val < 0:
                        raise ValueError("Expected value greater or equal to"
                                         " zero for 'retry_options' %s; got"
                                         " %s instead" % (k, val))
                    ensure_options[k] = tmp_val
        self._ensure_options = ensure_options

        self._drain_events_timeout = DRAIN_EVENTS_PERIOD
        if transport == 'memory' and transport_options:
            polling_interval = transport_options.get('polling_interval')
            if polling_interval is not None:
                self._drain_events_timeout = polling_interval

        # create connection
        self._conn = kombu.Connection(url, transport=transport,
                                      transport_options=transport_options)

        # create exchange
        self._exchange = kombu.Exchange(name=self._exchange_name,
                                        durable=False, auto_delete=True)

    @property
    def dispatcher(self):
        """Dispatcher internally used to dispatch message(s) that match."""
        return self._dispatcher

    @property
    def connection_details(self):
        """Details about the connection (read-only)."""
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

    def _make_queue(self, routing_key, exchange, channel=None):
        """Make a named queue for the given exchange."""
        queue_name = "%s_%s" % (self._exchange_name, routing_key)
        return kombu.Queue(name=queue_name,
                           routing_key=routing_key, durable=False,
                           exchange=exchange, auto_delete=True,
                           channel=channel)

    def publish(self, msg, routing_key, reply_to=None, correlation_id=None):
        """Publish message to the named exchange with given routing key."""
        if isinstance(routing_key, six.string_types):
            routing_keys = [routing_key]
        else:
            routing_keys = routing_key

        # Filter out any empty keys...
        routing_keys = [r_k for r_k in routing_keys if r_k]
        if not routing_keys:
            LOG.warning("No routing key/s specified; unable to send '%s'"
                        " to any target queue on exchange '%s'", msg,
                        self._exchange_name)
            return

        def _publish(producer, routing_key):
            queue = self._make_queue(routing_key, self._exchange)
            producer.publish(body=msg.to_dict(),
                             routing_key=routing_key,
                             exchange=self._exchange,
                             declare=[queue],
                             type=msg.TYPE,
                             reply_to=reply_to,
                             correlation_id=correlation_id)

        def _publish_errback(exc, interval):
            LOG.exception('Publishing error: %s', exc)
            LOG.info('Retry triggering in %s seconds', interval)

        LOG.debug("Sending '%s' message using routing keys %s",
                  msg, routing_keys)
        with kombu.connections[self._conn].acquire(block=True) as conn:
            with conn.Producer() as producer:
                ensure_kwargs = self._ensure_options.copy()
                ensure_kwargs['errback'] = _publish_errback
                safe_publish = conn.ensure(producer, _publish, **ensure_kwargs)
                for routing_key in routing_keys:
                    safe_publish(producer, routing_key)

    def start(self):
        """Start proxy."""

        def _drain(conn, timeout):
            try:
                conn.drain_events(timeout=timeout)
            except kombu_exceptions.TimeoutError:
                pass

        def _drain_errback(exc, interval):
            LOG.exception('Draining error: %s', exc)
            LOG.info('Retry triggering in %s seconds', interval)

        LOG.info("Starting to consume from the '%s' exchange.",
                 self._exchange_name)
        with kombu.connections[self._conn].acquire(block=True) as conn:
            queue = self._make_queue(self._topic, self._exchange, channel=conn)
            callbacks = [self._dispatcher.on_message]
            with conn.Consumer(queues=queue, callbacks=callbacks) as consumer:
                ensure_kwargs = self._ensure_options.copy()
                ensure_kwargs['errback'] = _drain_errback
                safe_drain = conn.ensure(consumer, _drain, **ensure_kwargs)
                self._running.set()
                try:
                    while self._running.is_set():
                        safe_drain(conn, self._drain_events_timeout)
                        if self._on_wait is not None:
                            self._on_wait()
                finally:
                    self._running.clear()

    def wait(self):
        """Wait until proxy is started."""
        self._running.wait()

    def stop(self):
        """Stop proxy."""
        self._running.clear()
