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

from oslo_utils import timeutils

from taskflow.engines.action_engine import executor
from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import proxy
from taskflow.engines.worker_based import types as wt
from taskflow import exceptions as exc
from taskflow import logging
from taskflow import task as task_atom
from taskflow.types import periodic
from taskflow.utils import kombu_utils as ku
from taskflow.utils import misc
from taskflow.utils import threading_utils as tu

LOG = logging.getLogger(__name__)


class WorkerTaskExecutor(executor.TaskExecutor):
    """Executes tasks on remote workers."""

    def __init__(self, uuid, exchange, topics,
                 transition_timeout=pr.REQUEST_TIMEOUT,
                 url=None, transport=None, transport_options=None,
                 retry_options=None):
        self._uuid = uuid
        self._requests_cache = wt.RequestsCache()
        self._transition_timeout = transition_timeout
        type_handlers = {
            pr.RESPONSE: [
                self._process_response,
                pr.Response.validate,
            ],
        }
        self._proxy = proxy.Proxy(uuid, exchange,
                                  type_handlers=type_handlers,
                                  on_wait=self._on_wait, url=url,
                                  transport=transport,
                                  transport_options=transport_options,
                                  retry_options=retry_options)
        # NOTE(harlowja): This is the most simplest finder impl. that
        # doesn't have external dependencies (outside of what this engine
        # already requires); it though does create periodic 'polling' traffic
        # to workers to 'learn' of the tasks they can perform (and requires
        # pre-existing knowledge of the topics those workers are on to gather
        # and update this information).
        self._finder = wt.ProxyWorkerFinder(uuid, self._proxy, topics)
        self._finder.notifier.register(wt.WorkerFinder.WORKER_ARRIVED,
                                       self._on_worker)
        self._helpers = tu.ThreadBundle()
        self._helpers.bind(lambda: tu.daemon_thread(self._proxy.start),
                           after_start=lambda t: self._proxy.wait(),
                           before_join=lambda t: self._proxy.stop())
        p_worker = periodic.PeriodicWorker.create([self._finder])
        if p_worker:
            self._helpers.bind(lambda: tu.daemon_thread(p_worker.start),
                               before_join=lambda t: p_worker.stop(),
                               after_join=lambda t: p_worker.reset(),
                               before_start=lambda t: p_worker.reset())

    def _on_worker(self, event_type, details):
        """Process new worker that has arrived (and fire off any work)."""
        worker = details['worker']
        for request in self._requests_cache.get_waiting_requests(worker):
            if request.transition_and_log_error(pr.PENDING, logger=LOG):
                self._publish_request(request, worker)

    def _process_response(self, response, message):
        """Process response from remote side."""
        LOG.debug("Started processing response message '%s'",
                  ku.DelayedPretty(message))
        try:
            task_uuid = message.properties['correlation_id']
        except KeyError:
            LOG.warning("The 'correlation_id' message property is"
                        " missing in message '%s'",
                        ku.DelayedPretty(message))
        else:
            request = self._requests_cache.get(task_uuid)
            if request is not None:
                response = pr.Response.from_dict(response)
                LOG.debug("Response with state '%s' received for '%s'",
                          response.state, request)
                if response.state == pr.RUNNING:
                    request.transition_and_log_error(pr.RUNNING, logger=LOG)
                elif response.state == pr.EVENT:
                    # Proxy the event + details to the task/request notifier...
                    event_type = response.data['event_type']
                    details = response.data['details']
                    request.notifier.notify(event_type, details)
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
                    LOG.warning("Unexpected response status '%s'",
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
                     progress_callback=None, **kwargs):
        """Submit task request to a worker."""
        request = pr.Request(task, task_uuid, action, arguments,
                             self._transition_timeout, **kwargs)

        # Register the callback, so that we can proxy the progress correctly.
        if (progress_callback is not None and
                request.notifier.can_be_registered(
                    task_atom.EVENT_UPDATE_PROGRESS)):
            request.notifier.register(task_atom.EVENT_UPDATE_PROGRESS,
                                      progress_callback)
            cleaner = functools.partial(request.notifier.deregister,
                                        task_atom.EVENT_UPDATE_PROGRESS,
                                        progress_callback)
            request.result.add_done_callback(lambda fut: cleaner())

        # Get task's worker and publish request if worker was found.
        worker = self._finder.get_worker_for_task(task)
        if worker is not None:
            # NOTE(skudriashev): Make sure request is set to the PENDING state
            # before putting it into the requests cache to prevent the notify
            # processing thread get list of waiting requests and publish it
            # before it is published here, so it wouldn't be published twice.
            if request.transition_and_log_error(pr.PENDING, logger=LOG):
                self._requests_cache[request.uuid] = request
                self._publish_request(request, worker)
        else:
            LOG.debug("Delaying submission of '%s', no currently known"
                      " worker/s available to process it", request)
            self._requests_cache[request.uuid] = request

        return request.result

    def _publish_request(self, request, worker):
        """Publish request to a given topic."""
        LOG.debug("Submitting execution of '%s' to worker '%s' (expecting"
                  " response identified by reply_to=%s and"
                  " correlation_id=%s)", request, worker, self._uuid,
                  request.uuid)
        try:
            self._proxy.publish(request, worker.topic,
                                reply_to=self._uuid,
                                correlation_id=request.uuid)
        except Exception:
            with misc.capture_failure() as failure:
                LOG.critical("Failed to submit '%s' (transitioning it to"
                             " %s)", request, pr.FAILURE, exc_info=True)
                if request.transition_and_log_error(pr.FAILURE, logger=LOG):
                    del self._requests_cache[request.uuid]
                    request.set_result(failure)

    def execute_task(self, task, task_uuid, arguments,
                     progress_callback=None):
        return self._submit_task(task, task_uuid, pr.EXECUTE, arguments,
                                 progress_callback=progress_callback)

    def revert_task(self, task, task_uuid, arguments, result, failures,
                    progress_callback=None):
        return self._submit_task(task, task_uuid, pr.REVERT, arguments,
                                 progress_callback=progress_callback,
                                 result=result, failures=failures)

    def wait_for_workers(self, workers=1, timeout=None):
        """Waits for geq workers to notify they are ready to do work.

        NOTE(harlowja): if a timeout is provided this function will wait
        until that timeout expires, if the amount of workers does not reach
        the desired amount of workers before the timeout expires then this will
        return how many workers are still needed, otherwise it will
        return zero.
        """
        return self._finder.wait_for_workers(workers=workers,
                                             timeout=timeout)

    def start(self):
        """Starts proxy thread and associated topic notification thread."""
        self._helpers.start()

    def stop(self):
        """Stops proxy thread and associated topic notification thread."""
        self._helpers.stop()
        self._requests_cache.clear(self._handle_expired_request)
        self._finder.clear()
