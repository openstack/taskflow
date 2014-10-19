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
import threading

from oslo.utils import timeutils

from taskflow.engines.action_engine import executor
from taskflow.engines.worker_based import cache
from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import proxy
from taskflow import exceptions as exc
from taskflow.types import timing as tt
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

    def __init__(self, uuid, exchange, topics,
                 transition_timeout=pr.REQUEST_TIMEOUT, **kwargs):
        self._uuid = uuid
        self._topics = topics
        self._requests_cache = cache.RequestsCache()
        self._transition_timeout = transition_timeout
        self._workers_cache = cache.WorkersCache()
        self._workers_arrival = threading.Condition()
        handlers = {
            pr.NOTIFY: [
                self._process_notify,
                functools.partial(pr.Notify.validate, response=True),
            ],
            pr.RESPONSE: [
                self._process_response,
                pr.Response.validate,
            ],
        }
        self._proxy = proxy.Proxy(uuid, exchange, handlers,
                                  self._on_wait, **kwargs)
        self._proxy_thread = None
        self._periodic = PeriodicWorker(tt.Timeout(pr.NOTIFY_PERIOD),
                                        [self._notify_topics])
        self._periodic_thread = None

    def _process_notify(self, notify, message):
        """Process notify message from remote side."""
        LOG.debug("Started processing notify message '%s'",
                  message.delivery_tag)
        topic = notify['topic']
        tasks = notify['tasks']

        # Add worker info to the cache
        LOG.debug("Received that tasks %s can be processed by topic '%s'",
                  tasks, topic)
        self._workers_arrival.acquire()
        try:
            self._workers_cache[topic] = tasks
            self._workers_arrival.notify_all()
        finally:
            self._workers_arrival.release()

        # Publish waiting requests
        for request in self._requests_cache.get_waiting_requests(tasks):
            if request.transition_and_log_error(pr.PENDING, logger=LOG):
                self._publish_request(request, topic)

    def _process_response(self, response, message):
        """Process response from remote side."""
        LOG.debug("Started processing response message '%s'",
                  message.delivery_tag)
        try:
            task_uuid = message.properties['correlation_id']
        except KeyError:
            LOG.warning("The 'correlation_id' message property is missing")
        else:
            request = self._requests_cache.get(task_uuid)
            if request is not None:
                response = pr.Response.from_dict(response)
                LOG.debug("Response with state '%s' received for '%s'",
                          response.state, request)
                if response.state == pr.RUNNING:
                    request.transition_and_log_error(pr.RUNNING, logger=LOG)
                elif response.state == pr.PROGRESS:
                    request.on_progress(**response.data)
                elif response.state in (pr.FAILURE, pr.SUCCESS):
                    moved = request.transition_and_log_error(response.state,
                                                             logger=LOG)
                    if moved:
                        # NOTE(imelnikov): request should not be in the
                        # cache when another thread can see its result and
                        # schedule another request with the same uuid; so
                        # we remove it, then set the result...
                        del self._requests_cache[request.uuid]
                        request.set_result(**response.data)
                else:
                    LOG.warning("Unexpected response status: '%s'",
                                response.state)
            else:
                LOG.debug("Request with id='%s' not found", task_uuid)

    @staticmethod
    def _handle_expired_request(request):
        """Handle expired request.

        When request has expired it is removed from the requests cache and
        the `RequestTimeout` exception is set as a request result.
        """
        if request.transition_and_log_error(pr.FAILURE, logger=LOG):
            # Raise an exception (and then catch it) so we get a nice
            # traceback that the request will get instead of it getting
            # just an exception with no traceback...
            try:
                request_age = timeutils.delta_seconds(request.created_on,
                                                      timeutils.utcnow())
                raise exc.RequestTimeout(
                    "Request '%s' has expired after waiting for %0.2f"
                    " seconds for it to transition out of (%s) states"
                    % (request, request_age, ", ".join(pr.WAITING_STATES)))
            except exc.RequestTimeout:
                with misc.capture_failure() as failure:
                    LOG.debug(failure.exception_str)
                    request.set_result(failure)

    def _on_wait(self):
        """This function is called cyclically between draining events."""
        self._requests_cache.cleanup(self._handle_expired_request)

    def _submit_task(self, task, task_uuid, action, arguments,
                     progress_callback, **kwargs):
        """Submit task request to a worker."""
        request = pr.Request(task, task_uuid, action, arguments,
                             progress_callback, self._transition_timeout,
                             **kwargs)

        # Get task's topic and publish request if topic was found.
        topic = self._workers_cache.get_topic_by_task(request.task_cls)
        if topic is not None:
            # NOTE(skudriashev): Make sure request is set to the PENDING state
            # before putting it into the requests cache to prevent the notify
            # processing thread get list of waiting requests and publish it
            # before it is published here, so it wouldn't be published twice.
            if request.transition_and_log_error(pr.PENDING, logger=LOG):
                self._requests_cache[request.uuid] = request
                self._publish_request(request, topic)
        else:
            LOG.debug("Delaying submission of '%s', no currently known"
                      " worker/s available to process it", request)
            self._requests_cache[request.uuid] = request

        return request.result

    def _publish_request(self, request, topic):
        """Publish request to a given topic."""
        LOG.debug("Submitting execution of '%s' to topic '%s' (expecting"
                  " response identified by reply_to=%s and"
                  " correlation_id=%s)", request, topic, self._uuid,
                  request.uuid)
        try:
            self._proxy.publish(msg=request,
                                routing_key=topic,
                                reply_to=self._uuid,
                                correlation_id=request.uuid)
        except Exception:
            with misc.capture_failure() as failure:
                LOG.critical("Failed to submit '%s' (transitioning it to"
                             " %s)", request, pr.FAILURE, exc_info=True)
                if request.transition_and_log_error(pr.FAILURE, logger=LOG):
                    del self._requests_cache[request.uuid]
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

    def wait_for_workers(self, workers=1, timeout=None):
        """Waits for geq workers to notify they are ready to do work.

        NOTE(harlowja): if a timeout is provided this function will wait
        until that timeout expires, if the amount of workers does not reach
        the desired amount of workers before the timeout expires then this will
        return how many workers are still needed, otherwise it will
        return zero.
        """
        if workers <= 0:
            raise ValueError("Worker amount must be greater than zero")
        w = None
        if timeout is not None:
            w = tt.StopWatch(timeout).start()
        self._workers_arrival.acquire()
        try:
            while len(self._workers_cache) < workers:
                if w is not None and w.expired():
                    return workers - len(self._workers_cache)
                timeout = None
                if w is not None:
                    timeout = w.leftover()
                self._workers_arrival.wait(timeout)
            return 0
        finally:
            self._workers_arrival.release()

    def start(self):
        """Starts proxy thread and associated topic notification thread."""
        if not _is_alive(self._proxy_thread):
            self._proxy_thread = tu.daemon_thread(self._proxy.start)
            self._proxy_thread.start()
            self._proxy.wait()
        if not _is_alive(self._periodic_thread):
            self._periodic.reset()
            self._periodic_thread = tu.daemon_thread(self._periodic.start)
            self._periodic_thread.start()

    def stop(self):
        """Stops proxy thread and associated topic notification thread."""
        if self._periodic_thread is not None:
            self._periodic.stop()
            self._periodic_thread.join()
            self._periodic_thread = None
        if self._proxy_thread is not None:
            self._proxy.stop()
            self._proxy_thread.join()
            self._proxy_thread = None
