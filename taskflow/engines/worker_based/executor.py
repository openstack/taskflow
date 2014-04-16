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

from kombu import exceptions as kombu_exc

from taskflow.engines.action_engine import executor
from taskflow.engines.worker_based import cache
from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import proxy
from taskflow import exceptions as exc
from taskflow.utils import async_utils
from taskflow.utils import misc
from taskflow.utils import reflection
from taskflow.utils import threading_utils as tu

LOG = logging.getLogger(__name__)


def _is_alive(thread):
    if not thread:
        return False
    return thread.is_alive()


class PeriodicWorker(object):
    """Calls a set of functions when activated periodically.

    NOTE(harlowja): the provided timeout object determines the periodicity.
    """
    def __init__(self, timeout, functors):
        self._timeout = timeout
        self._functors = []
        for f in functors:
            self._functors.append((f, reflection.get_callable_name(f)))

    def start(self):
        while not self._timeout.is_stopped():
            for (f, f_name) in self._functors:
                LOG.debug("Calling periodic function '%s'", f_name)
                try:
                    f()
                except Exception:
                    LOG.warn("Failed to call periodic function '%s'", f_name,
                             exc_info=True)
            self._timeout.wait()

    def stop(self):
        self._timeout.interrupt()

    def reset(self):
        self._timeout.reset()


class WorkerTaskExecutor(executor.TaskExecutorBase):
    """Executes tasks on remote workers."""

    def __init__(self, uuid, exchange, topics, **kwargs):
        self._uuid = uuid
        self._topics = topics
        self._requests_cache = cache.RequestsCache()
        self._workers_cache = cache.WorkersCache()
        self._proxy = proxy.Proxy(uuid, exchange, self._on_message,
                                  self._on_wait, **kwargs)
        self._proxy_thread = None
        self._periodic = PeriodicWorker(misc.Timeout(pr.NOTIFY_PERIOD),
                                        [self._notify_topics])
        self._periodic_thread = None

    def _on_message(self, data, message):
        """This method is called on incoming message."""
        LOG.debug("Got message: %s", data)
        try:
            # acknowledge message before processing
            message.ack()
        except kombu_exc.MessageStateError:
            LOG.exception("Failed to acknowledge AMQP message.")
        else:
            LOG.debug("AMQP message acknowledged.")
            try:
                msg_type = message.properties['type']
            except KeyError:
                LOG.warning("The 'type' message property is missing.")
            else:
                if msg_type == pr.NOTIFY:
                    self._process_notify(data)
                elif msg_type == pr.RESPONSE:
                    self._process_response(data, message)
                else:
                    LOG.warning("Unexpected message type: %s", msg_type)

    def _process_notify(self, notify):
        """Process notify message from remote side."""
        LOG.debug("Start processing notify message.")
        topic = notify['topic']
        tasks = notify['tasks']

        # add worker info to the cache
        self._workers_cache.set(topic, tasks)

        # publish waiting requests
        for request in self._requests_cache.get_waiting_requests(tasks):
            request.set_pending()
            self._publish_request(request, topic)

    def _process_response(self, response, message):
        """Process response from remote side."""
        LOG.debug("Start processing response message.")
        try:
            task_uuid = message.properties['correlation_id']
        except KeyError:
            LOG.warning("The 'correlation_id' message property is missing.")
        else:
            LOG.debug("Task uuid: '%s'", task_uuid)
            request = self._requests_cache.get(task_uuid)
            if request is not None:
                response = pr.Response.from_dict(response)
                if response.state == pr.RUNNING:
                    request.set_running()
                elif response.state == pr.PROGRESS:
                    request.on_progress(**response.data)
                elif response.state in (pr.FAILURE, pr.SUCCESS):
                    # NOTE(imelnikov): request should not be in cache when
                    # another thread can see its result and schedule another
                    # request with same uuid; so we remove it, then set result
                    self._requests_cache.delete(request.uuid)
                    request.set_result(**response.data)
                else:
                    LOG.warning("Unexpected response status: '%s'",
                                response.state)
            else:
                LOG.debug("Request with id='%s' not found.", task_uuid)

    @staticmethod
    def _handle_expired_request(request):
        """Handle expired request.

        When request has expired it is removed from the requests cache and
        the `RequestTimeout` exception is set as a request result.
        """
        LOG.debug("Request '%r' has expired.", request)
        LOG.debug("The '%r' request has expired.", request)
        request.set_result(misc.Failure.from_exception(
            exc.RequestTimeout("The '%r' request has expired" % request)))

    def _on_wait(self):
        """This function is called cyclically between draining events."""
        self._requests_cache.cleanup(self._handle_expired_request)

    def _submit_task(self, task, task_uuid, action, arguments,
                     progress_callback, timeout=pr.REQUEST_TIMEOUT, **kwargs):
        """Submit task request to a worker."""
        request = pr.Request(task, task_uuid, action, arguments,
                             progress_callback, timeout, **kwargs)

        # Get task's topic and publish request if topic was found.
        topic = self._workers_cache.get_topic_by_task(request.task_cls)
        if topic is not None:
            # NOTE(skudriashev): Make sure request is set to the PENDING state
            # before putting it into the requests cache to prevent the notify
            # processing thread get list of waiting requests and publish it
            # before it is published here, so it wouldn't be published twice.
            request.set_pending()
            self._requests_cache.set(request.uuid, request)
            self._publish_request(request, topic)
        else:
            self._requests_cache.set(request.uuid, request)

        return request.result

    def _publish_request(self, request, topic):
        """Publish request to a given topic."""
        try:
            self._proxy.publish(msg=request,
                                routing_key=topic,
                                reply_to=self._uuid,
                                correlation_id=request.uuid)
        except Exception:
            with misc.capture_failure() as failure:
                LOG.exception("Failed to submit the '%s' request." %
                              request)
                self._requests_cache.delete(request.uuid)
                request.set_result(failure)

    def _notify_topics(self):
        """Cyclically called to publish notify message to each topic."""
        self._proxy.publish(pr.Notify(), self._topics, reply_to=self._uuid)

    def execute_task(self, task, task_uuid, arguments,
                     progress_callback=None):
        return self._submit_task(task, task_uuid, pr.EXECUTE, arguments,
                                 progress_callback)

    def revert_task(self, task, task_uuid, arguments, result, failures,
                    progress_callback=None):
        return self._submit_task(task, task_uuid, pr.REVERT, arguments,
                                 progress_callback, result=result,
                                 failures=failures)

    def wait_for_any(self, fs, timeout=None):
        """Wait for futures returned by this executor to complete."""
        return async_utils.wait_for_any(fs, timeout)

    def start(self):
        """Start proxy thread (and associated topic notification thread)."""
        if not _is_alive(self._proxy_thread):
            self._proxy_thread = tu.daemon_thread(self._proxy.start)
            self._proxy_thread.start()
            self._proxy.wait()
        if not _is_alive(self._periodic_thread):
            self._periodic.reset()
            self._periodic_thread = tu.daemon_thread(self._periodic.start)
            self._periodic_thread.start()

    def stop(self):
        """Stop proxy thread (and associated topic notification thread), so
        those threads will be gracefully terminated.
        """
        if self._periodic_thread is not None:
            self._periodic.stop()
            self._periodic_thread.join()
            self._periodic_thread = None
        if self._proxy_thread is not None:
            self._proxy.stop()
            self._proxy_thread.join()
            self._proxy_thread = None
