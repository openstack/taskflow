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

import functools
import logging

from kombu import exceptions as kombu_exc

from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import proxy
from taskflow.utils import misc
from taskflow.utils import persistence_utils as pu

LOG = logging.getLogger(__name__)


class Server(object):
    """Server implementation that waits for incoming tasks requests."""

    def __init__(self, topic, exchange, executor, endpoints, **kwargs):
        self._proxy = proxy.Proxy(topic, exchange, self._on_message, **kwargs)
        self._executor = executor
        self._endpoints = dict([(endpoint.name, endpoint)
                                for endpoint in endpoints])

    def _on_message(self, request, message):
        """This method is called on incoming request."""
        LOG.debug("Got request: %s", request)
        # NOTE(skudriashev): Process all incoming requests only if proxy is
        # running, otherwise reject and requeue them.
        if self._proxy.is_running:
            # NOTE(skudriashev): Process request only if message has been
            # acknowledged successfully.
            try:
                # acknowledge message
                message.ack()
            except kombu_exc.MessageStateError:
                LOG.exception("Failed to acknowledge AMQP message")
            else:
                LOG.debug("AMQP message acknowledged")
                # spawn new thread to process request
                self._executor.submit(self._process_request, request, message)
        else:
            try:
                # reject and requeue message
                message.reject(requeue=True)
            except kombu_exc.MessageStateError:
                LOG.exception("Failed to reject/requeue AMQP message")
            else:
                LOG.debug("AMQP message rejected and requeued")

    @staticmethod
    def _parse_request(task, task_name, action, arguments, result=None,
                       failures=None, **kwargs):
        """Parse request before it can be processed. All `misc.Failure` objects
        that have been converted to dict on the remote side to be serializable
        are now converted back to objects.
        """
        action_args = dict(arguments=arguments, task_name=task_name)
        if result is not None:
            data_type, data = result
            if data_type == 'failure':
                action_args['result'] = pu.failure_from_dict(data)
            else:
                action_args['result'] = data
        if failures is not None:
            action_args['failures'] = {}
            for k, v in failures.items():
                action_args['failures'][k] = pu.failure_from_dict(v)
        return task, action, action_args

    @staticmethod
    def _parse_message(message):
        """Parse broker message to get the `reply_to` and the `correlation_id`
        properties. If required properties are missing - the `ValueError` is
        raised.
        """
        properties = []
        for prop in ('reply_to', 'correlation_id'):
            try:
                properties.append(message.properties[prop])
            except KeyError:
                raise ValueError("The '%s' message property is missing" %
                                 prop)

        return properties

    def _reply(self, reply_to, task_uuid, state=pr.FAILURE, **kwargs):
        """Send reply to the `reply_to` queue."""
        response = dict(state=state, **kwargs)
        LOG.debug("Sending reply: %s", response)
        try:
            self._proxy.publish(response, task_uuid, reply_to)
        except Exception:
            LOG.exception("Failed to send reply")

    def _on_update_progress(self, reply_to, task_uuid, task, event_data,
                            progress):
        """Send task update progress notification."""
        self._reply(reply_to, task_uuid, pr.PROGRESS, event_data=event_data,
                    progress=progress)

    def _process_request(self, request, message):
        """Process request in separate thread and reply back."""
        # NOTE(skudriashev): parse broker message first to get the `reply_to`
        # and the `task_uuid` parameters to have possibility to reply back.
        try:
            reply_to, task_uuid = self._parse_message(message)
        except ValueError:
            LOG.exception("Failed to parse broker message")
            return
        else:
            # prepare task progress callback
            progress_callback = functools.partial(
                self._on_update_progress, reply_to, task_uuid)
            # prepare reply callback
            reply_callback = functools.partial(
                self._reply, reply_to, task_uuid)

        # parse request to get task name, action and action arguments
        try:
            task, action, action_args = self._parse_request(**request)
            action_args.update(task_uuid=task_uuid,
                               progress_callback=progress_callback)
        except ValueError:
            with misc.capture_failure() as failure:
                LOG.exception("Failed to parse request")
                reply_callback(result=pu.failure_to_dict(failure))
                return

        # get task endpoint
        try:
            endpoint = self._endpoints[task]
        except KeyError:
            with misc.capture_failure() as failure:
                LOG.exception("The '%s' task endpoint does not exist", task)
                reply_callback(result=pu.failure_to_dict(failure))
                return
        else:
            reply_callback(state=pr.RUNNING)

        # perform task action
        try:
            result = getattr(endpoint, action)(**action_args)
        except Exception:
            with misc.capture_failure() as failure:
                LOG.exception("The %s task execution failed", endpoint)
                reply_callback(result=pu.failure_to_dict(failure))
        else:
            if isinstance(result, misc.Failure):
                reply_callback(result=pu.failure_to_dict(result))
            else:
                reply_callback(state=pr.SUCCESS, result=result)

    def start(self):
        """Start processing incoming requests."""
        self._proxy.start()

    def wait(self):
        """Wait until server is started."""
        self._proxy.wait()

    def stop(self):
        """Stop processing incoming requests."""
        self._proxy.stop()
        self._executor.shutdown()
