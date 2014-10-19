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

import six

from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import proxy
from taskflow.types import failure as ft
from taskflow.utils import misc

LOG = logging.getLogger(__name__)


def delayed(executor):
    """Wraps & runs the function using a futures compatible executor."""

    def decorator(f):

        @six.wraps(f)
        def wrapper(*args, **kwargs):
            return executor.submit(f, *args, **kwargs)

        return wrapper

    return decorator


class Server(object):
    """Server implementation that waits for incoming tasks requests."""

    def __init__(self, topic, exchange, executor, endpoints, **kwargs):
        handlers = {
            pr.NOTIFY: [
                delayed(executor)(self._process_notify),
                functools.partial(pr.Notify.validate, response=False),
            ],
            pr.REQUEST: [
                delayed(executor)(self._process_request),
                pr.Request.validate,
            ],
        }
        self._proxy = proxy.Proxy(topic, exchange, handlers,
                                  on_wait=None, **kwargs)
        self._topic = topic
        self._endpoints = dict([(endpoint.name, endpoint)
                                for endpoint in endpoints])

    @property
    def connection_details(self):
        return self._proxy.connection_details

    @staticmethod
    def _parse_request(task_cls, task_name, action, arguments, result=None,
                       failures=None, **kwargs):
        """Parse request before it can be further processed.

        All `failure.Failure` objects that have been converted to dict on the
        remote side will now converted back to `failure.Failure` objects.
        """
        action_args = dict(arguments=arguments, task_name=task_name)
        if result is not None:
            data_type, data = result
            if data_type == 'failure':
                action_args['result'] = ft.Failure.from_dict(data)
            else:
                action_args['result'] = data
        if failures is not None:
            action_args['failures'] = {}
            for key, data in six.iteritems(failures):
                action_args['failures'][key] = ft.Failure.from_dict(data)
        return task_cls, action, action_args

    @staticmethod
    def _parse_message(message):
        """Extracts required attributes out of the messages properties.

        This extracts the `reply_to` and the `correlation_id` properties. If
        any of these required properties are missing a `ValueError` is raised.
        """
        properties = []
        for prop in ('reply_to', 'correlation_id'):
            try:
                properties.append(message.properties[prop])
            except KeyError:
                raise ValueError("The '%s' message property is missing" %
                                 prop)
        return properties

    def _reply(self, capture, reply_to, task_uuid, state=pr.FAILURE, **kwargs):
        """Send a reply to the `reply_to` queue with the given information.

        Can capture failures to publish and if capturing will log associated
        critical errors on behalf of the caller, and then returns whether the
        publish worked out or did not.
        """
        response = pr.Response(state, **kwargs)
        published = False
        try:
            self._proxy.publish(response, reply_to, correlation_id=task_uuid)
            published = True
        except Exception:
            if not capture:
                raise
            LOG.critical("Failed to send reply to '%s' for task '%s' with"
                         " response %s", reply_to, task_uuid, response,
                         exc_info=True)
        return published

    def _on_update_progress(self, reply_to, task_uuid, task, event_data,
                            progress):
        """Send task update progress notification."""
        # NOTE(harlowja): the executor that will trigger this using the
        # task notification/listener mechanism will handle logging if this
        # fails, so thats why capture is 'False' is used here.
        self._reply(False, reply_to, task_uuid, pr.PROGRESS,
                    event_data=event_data, progress=progress)

    def _process_notify(self, notify, message):
        """Process notify message and reply back."""
        LOG.debug("Started processing notify message %r", message.delivery_tag)
        try:
            reply_to = message.properties['reply_to']
        except KeyError:
            LOG.warn("The 'reply_to' message property is missing"
                     " in received notify message %r", message.delivery_tag,
                     exc_info=True)
        else:
            response = pr.Notify(topic=self._topic,
                                 tasks=self._endpoints.keys())
            try:
                self._proxy.publish(response, routing_key=reply_to)
            except Exception:
                LOG.critical("Failed to send reply to '%s' with notify"
                             " response %s", reply_to, response,
                             exc_info=True)

    def _process_request(self, request, message):
        """Process request message and reply back."""
        LOG.debug("Started processing request message %r",
                  message.delivery_tag)
        try:
            # NOTE(skudriashev): parse broker message first to get
            # the `reply_to` and the `task_uuid` parameters to have
            # possibility to reply back (if we can't parse, we can't respond
            # in the first place...).
            reply_to, task_uuid = self._parse_message(message)
        except ValueError:
            LOG.warn("Failed to parse request attributes from message %r",
                     message.delivery_tag, exc_info=True)
            return
        else:
            # prepare task progress callback
            progress_callback = functools.partial(self._on_update_progress,
                                                  reply_to, task_uuid)
            # prepare reply callback
            reply_callback = functools.partial(self._reply, True, reply_to,
                                               task_uuid)

        # parse request to get task name, action and action arguments
        try:
            task_cls, action, action_args = self._parse_request(**request)
            action_args.update(task_uuid=task_uuid,
                               progress_callback=progress_callback)
        except ValueError:
            with misc.capture_failure() as failure:
                LOG.warn("Failed to parse request contents from message %r",
                         message.delivery_tag, exc_info=True)
                reply_callback(result=failure.to_dict())
                return

        # get task endpoint
        try:
            endpoint = self._endpoints[task_cls]
        except KeyError:
            with misc.capture_failure() as failure:
                LOG.warn("The '%s' task endpoint does not exist, unable"
                         " to continue processing request message %r",
                         task_cls, message.delivery_tag, exc_info=True)
                reply_callback(result=failure.to_dict())
                return
        else:
            try:
                handler = getattr(endpoint, action)
            except AttributeError:
                with misc.capture_failure() as failure:
                    LOG.warn("The '%s' handler does not exist on task endpoint"
                             " '%s', unable to continue processing request"
                             " message %r", action, endpoint,
                             message.delivery_tag, exc_info=True)
                    reply_callback(result=failure.to_dict())
                    return
            else:
                if not reply_callback(state=pr.RUNNING):
                    return

        # perform task action
        try:
            result = handler(**action_args)
        except Exception:
            with misc.capture_failure() as failure:
                LOG.warn("The '%s' endpoint '%s' execution for request"
                         " message %r failed", endpoint, action,
                         message.delivery_tag, exc_info=True)
                reply_callback(result=failure.to_dict())
        else:
            if isinstance(result, ft.Failure):
                reply_callback(result=result.to_dict())
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
