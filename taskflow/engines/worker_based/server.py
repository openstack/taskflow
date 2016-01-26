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

from oslo_utils import reflection
from oslo_utils import timeutils

from taskflow.engines.worker_based import dispatcher
from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import proxy
from taskflow import logging
from taskflow.types import failure as ft
from taskflow.types import notifier as nt
from taskflow.utils import kombu_utils as ku
from taskflow.utils import misc

LOG = logging.getLogger(__name__)


class Server(object):
    """Server implementation that waits for incoming tasks requests."""

    def __init__(self, topic, exchange, executor, endpoints,
                 url=None, transport=None, transport_options=None,
                 retry_options=None):
        type_handlers = {
            pr.NOTIFY: dispatcher.Handler(
                self._delayed_process(self._process_notify),
                validator=functools.partial(pr.Notify.validate,
                                            response=False)),
            pr.REQUEST: dispatcher.Handler(
                self._delayed_process(self._process_request),
                validator=pr.Request.validate),
        }
        self._executor = executor
        self._proxy = proxy.Proxy(topic, exchange,
                                  type_handlers=type_handlers,
                                  url=url, transport=transport,
                                  transport_options=transport_options,
                                  retry_options=retry_options)
        self._topic = topic
        self._endpoints = dict([(endpoint.name, endpoint)
                                for endpoint in endpoints])

    def _delayed_process(self, func):
        """Runs the function using the instances executor (eventually).

        This adds a *nice* benefit on showing how long it took for the
        function to finally be executed from when the message was received
        to when it was finally ran (which can be a nice thing to know
        to determine bottle-necks...).
        """
        func_name = reflection.get_callable_name(func)

        def _on_run(watch, content, message):
            LOG.trace("It took %s seconds to get around to running"
                      " function/method '%s' with"
                      " message '%s'", watch.elapsed(), func_name,
                      ku.DelayedPretty(message))
            return func(content, message)

        def _on_receive(content, message):
            LOG.debug("Submitting message '%s' for execution in the"
                      " future to '%s'", ku.DelayedPretty(message), func_name)
            watch = timeutils.StopWatch()
            watch.start()
            try:
                self._executor.submit(_on_run, watch, content, message)
            except RuntimeError:
                LOG.error("Unable to continue processing message '%s',"
                          " submission to instance executor (with later"
                          " execution by '%s') was unsuccessful",
                          ku.DelayedPretty(message), func_name,
                          exc_info=True)

        return _on_receive

    @property
    def connection_details(self):
        return self._proxy.connection_details

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

    def _on_event(self, reply_to, task_uuid, event_type, details):
        """Send out a task event notification."""
        # NOTE(harlowja): the executor that will trigger this using the
        # task notification/listener mechanism will handle logging if this
        # fails, so thats why capture is 'False' is used here.
        self._reply(False, reply_to, task_uuid, pr.EVENT,
                    event_type=event_type, details=details)

    def _process_notify(self, notify, message):
        """Process notify message and reply back."""
        try:
            reply_to = message.properties['reply_to']
        except KeyError:
            LOG.warn("The 'reply_to' message property is missing"
                     " in received notify message '%s'",
                     ku.DelayedPretty(message), exc_info=True)
        else:
            response = pr.Notify(topic=self._topic,
                                 tasks=self._endpoints.keys())
            try:
                self._proxy.publish(response, routing_key=reply_to)
            except Exception:
                LOG.critical("Failed to send reply to '%s' with notify"
                             " response '%s'", reply_to, response,
                             exc_info=True)

    def _process_request(self, request, message):
        """Process request message and reply back."""
        try:
            # NOTE(skudriashev): parse broker message first to get
            # the `reply_to` and the `task_uuid` parameters to have
            # possibility to reply back (if we can't parse, we can't respond
            # in the first place...).
            reply_to, task_uuid = self._parse_message(message)
        except ValueError:
            LOG.warn("Failed to parse request attributes from message '%s'",
                     ku.DelayedPretty(message), exc_info=True)
            return
        else:
            # prepare reply callback
            reply_callback = functools.partial(self._reply, True, reply_to,
                                               task_uuid)

        # Parse the request to get the activity/work to perform.
        try:
            work = pr.Request.from_dict(request, task_uuid=task_uuid)
        except ValueError:
            with misc.capture_failure() as failure:
                LOG.warn("Failed to parse request contents from message '%s'",
                         ku.DelayedPretty(message), exc_info=True)
                reply_callback(result=pr.failure_to_dict(failure))
                return

        # Now fetch the task endpoint (and action handler on it).
        try:
            endpoint = self._endpoints[work.task_cls]
        except KeyError:
            with misc.capture_failure() as failure:
                LOG.warn("The '%s' task endpoint does not exist, unable"
                         " to continue processing request message '%s'",
                         work.task_cls, ku.DelayedPretty(message),
                         exc_info=True)
                reply_callback(result=pr.failure_to_dict(failure))
                return
        else:
            try:
                handler = getattr(endpoint, work.action)
            except AttributeError:
                with misc.capture_failure() as failure:
                    LOG.warn("The '%s' handler does not exist on task endpoint"
                             " '%s', unable to continue processing request"
                             " message '%s'", work.action, endpoint,
                             ku.DelayedPretty(message), exc_info=True)
                    reply_callback(result=pr.failure_to_dict(failure))
                    return
            else:
                try:
                    task = endpoint.generate(name=work.task_name)
                except Exception:
                    with misc.capture_failure() as failure:
                        LOG.warn("The '%s' task '%s' generation for request"
                                 " message '%s' failed", endpoint, work.action,
                                 ku.DelayedPretty(message), exc_info=True)
                        reply_callback(result=pr.failure_to_dict(failure))
                        return
                else:
                    if not reply_callback(state=pr.RUNNING):
                        return

        # Associate *any* events this task emits with a proxy that will
        # emit them back to the engine... for handling at the engine side
        # of things...
        if task.notifier.can_be_registered(nt.Notifier.ANY):
            task.notifier.register(nt.Notifier.ANY,
                                   functools.partial(self._on_event,
                                                     reply_to, task_uuid))
        elif isinstance(task.notifier, nt.RestrictedNotifier):
            # Only proxy the allowable events then...
            for event_type in task.notifier.events_iter():
                task.notifier.register(event_type,
                                       functools.partial(self._on_event,
                                                         reply_to, task_uuid))

        # Perform the task action.
        try:
            result = handler(task, **work.arguments)
        except Exception:
            with misc.capture_failure() as failure:
                LOG.warn("The '%s' endpoint '%s' execution for request"
                         " message '%s' failed", endpoint, work.action,
                         ku.DelayedPretty(message), exc_info=True)
                reply_callback(result=pr.failure_to_dict(failure))
        else:
            # And be done with it!
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
