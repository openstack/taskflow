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

import threading

from futurist import periodics
from oslo_utils import timeutils
import six

from taskflow.engines.action_engine import executor
from taskflow.engines.worker_based import dispatcher
from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import proxy
from taskflow import exceptions as exc
from taskflow import logging
from taskflow.task import EVENT_UPDATE_PROGRESS  # noqa
from taskflow.utils import kombu_utils as ku
from taskflow.utils import misc
from taskflow.utils import threading_utils as tu

LOG = logging.getLogger(__name__)


class WorkerTaskExecutor(executor.TaskExecutor):
    """Executes tasks on remote workers."""

    def __init__(self, topic, exchange, finder_factory,
                 transition_timeout=pr.REQUEST_TIMEOUT,
                 url=None, transport=None, transport_options=None,
                 retry_options=None):
        self._topic = topic
        self._ongoing_requests = {}
        self._ongoing_requests_lock = threading.RLock()
        self._transition_timeout = transition_timeout
        type_handlers = {
            pr.RESPONSE: dispatcher.Handler(self._process_response,
                                            validator=pr.Response.validate),
        }
        self._proxy = proxy.Proxy(topic, exchange,
                                  type_handlers=type_handlers,
                                  on_wait=self._on_wait, url=url,
                                  transport=transport,
                                  transport_options=transport_options,
                                  retry_options=retry_options)

        if not six.callable(finder_factory):
            raise ValueError("Provided factory used to build worker finders"
                             " must be callable")
        self._finder = finder_factory(topic, self._proxy)
        self._finder.notifier.register(pr.WORKER_LOST, self._reassign_worker_tasks)

        self._helpers = tu.ThreadBundle()
        self._helpers.bind(lambda: tu.daemon_thread(self._proxy.start),
                           after_start=lambda t: self._proxy.wait(),
                           before_join=lambda t: self._proxy.stop())
        p_worker = periodics.PeriodicWorker.create([self._finder])
        if p_worker:
            self._helpers.bind(lambda: tu.daemon_thread(p_worker.start),
                               before_join=lambda t: p_worker.stop(),
                               after_join=lambda t: p_worker.reset(),
                               before_start=lambda t: p_worker.reset())
        self._activator = misc.Activator([self._finder, self._helpers])
        self._messages_processed = {
            'finder': self._finder.messages_processed,
        }

    def _reassign_worker_tasks(self, state, details):
        worker_id = details['identity']
        worker_requests = []
        with self._ongoing_requests_lock:
            for request in six.itervalues(self._ongoing_requests):
                if request.worker and request.worker.identity == worker_id:
                    worker_requests.append(request)

        for request in worker_requests:
            request.detach_worker()
            if request.transition_and_log_error(pr.WAITING, logger=LOG):
                self._assign_worker_to_request(request, request.task)
                if request.worker is not None:
                    LOG.debug("Request %s for task %s has moved from "
                              "worker %s to worker %s", request.uuid,
                              request.task, worker_id,
                              request.worker.identity)
                else:
                    LOG.debug("Request %s for task %s has been abandoned "
                              "from worker %s - no new worker could be "
                              "found", request.uuid, request.task,
                              worker_id)

    def _process_response(self, response, message):
        """Process response from remote side."""
        LOG.debug("Started processing response message '%s'",
                  ku.DelayedPretty(message))
        try:
            request_uuid = message.properties['correlation_id']
        except KeyError:
            LOG.warning("The 'correlation_id' message property is"
                        " missing in message '%s'",
                        ku.DelayedPretty(message))
        else:
            request = self._ongoing_requests.get(request_uuid)
            if request is not None:
                response = pr.Response.from_dict(response)
                LOG.debug("Extracted response '%s' and matched it to"
                          " request '%s'", response, request)
                if response.state == pr.RUNNING:
                    request.transition_and_log_error(pr.RUNNING, logger=LOG)
                elif response.state == pr.EVENT:
                    # Proxy the event + details to the task notifier so
                    # that it shows up in the local process (and activates
                    # any local callbacks...); thus making it look like
                    # the task is running locally (in some regards).
                    event_type = response.data['event_type']
                    details = response.data['details']
                    request.task.notifier.notify(event_type, details)
                elif response.state in (pr.FAILURE, pr.SUCCESS):
                    if request.transition_and_log_error(response.state,
                                                        logger=LOG):
                        with self._ongoing_requests_lock:
                            del self._ongoing_requests[request.uuid]
                        request.set_result(result=response.data['result'])
                else:
                    LOG.warning("Unexpected response status '%s'",
                                response.state)
            else:
                LOG.debug("Request with id='%s' not found", request_uuid)

    @staticmethod
    def _handle_expired_request(request):
        """Handle a expired request.

        When a request has expired it is removed from the ongoing requests
        dictionary and a ``RequestTimeout`` exception is set as a
        request result.
        """
        if request.transition_and_log_error(pr.FAILURE, logger=LOG):
            # Raise an exception (and then catch it) so we get a nice
            # traceback that the request will get instead of it getting
            # just an exception with no traceback...
            try:
                request_age = timeutils.now() - request.created_on
                raise exc.RequestTimeout(
                    "Request '%s' has expired after waiting for %0.2f"
                    " seconds for it to transition out of (%s) states"
                    % (request, request_age, ", ".join(pr.WAITING_STATES)))
            except exc.RequestTimeout:
                with misc.capture_failure() as failure:
                    LOG.debug(failure.exception_str)
                    request.set_result(failure)
            return True
        return False

    def _clean(self):
        if not self._ongoing_requests:
            return
        with self._ongoing_requests_lock:
            ongoing_requests_uuids = set(six.iterkeys(self._ongoing_requests))
        waiting_requests = {}
        expired_requests = {}
        for request_uuid in ongoing_requests_uuids:
            try:
                request = self._ongoing_requests[request_uuid]
            except KeyError:
                # Guess it got removed before we got to it...
                pass
            else:
                if request.expired:
                    expired_requests[request_uuid] = request
                elif request.current_state == pr.WAITING:
                    waiting_requests[request_uuid] = request
        if expired_requests:
            with self._ongoing_requests_lock:
                while expired_requests:
                    request_uuid, request = expired_requests.popitem()
                    if self._handle_expired_request(request):
                        del self._ongoing_requests[request_uuid]
        if waiting_requests:
            finder = self._finder
            new_messages_processed = finder.messages_processed
            last_messages_processed = self._messages_processed['finder']
            if new_messages_processed > last_messages_processed:
                # Some new message got to the finder, so we can see
                # if any new workers match (if no new messages have been
                # processed we might as well not do anything).
                while waiting_requests:
                    _request_uuid, request = waiting_requests.popitem()
                    worker = finder.get_worker_for_task(request.task)
                    if (worker is not None and
                            request.transition_and_log_error(pr.PENDING,
                                                             logger=LOG)):
                        self._publish_request(request, worker)
                self._messages_processed['finder'] = new_messages_processed

    def _on_wait(self):
        """This function is called cyclically between draining events."""
        # Discover any new workers
        self._finder.discover()
        # Process any expired requests or requests that have no current
        # worker located (publish messages for those if we now do have
        # a worker located).
        self._clean()

    def _submit_task(self, task, task_uuid, action, arguments,
                     progress_callback=None, result=pr.NO_RESULT,
                     failures=None):
        """Submit task request to a worker."""
        request = pr.Request(task, task_uuid, action, arguments,
                             timeout=self._transition_timeout,
                             result=result, failures=failures)

        # Register the callback, so that we can proxy the progress correctly.
        if (progress_callback is not None and
                task.notifier.can_be_registered(EVENT_UPDATE_PROGRESS)):
            task.notifier.register(EVENT_UPDATE_PROGRESS, progress_callback)
            request.future.add_done_callback(
                lambda _fut: task.notifier.deregister(EVENT_UPDATE_PROGRESS,
                                                      progress_callback))
        return self._assign_worker_to_request(request, task)

    def _assign_worker_to_request(self, request, task):
        # Get task's worker and publish request if worker was found.
        worker = self._finder.get_worker_for_task(task)
        if worker is not None:
            if request.transition_and_log_error(pr.PENDING, logger=LOG):
                request.attach_worker(worker)
                with self._ongoing_requests_lock:
                    self._ongoing_requests[request.uuid] = request
                self._publish_request(request, worker)
        else:
            LOG.debug("Delaying submission of '%s', no currently known"
                      " worker/s available to process it", request)
            with self._ongoing_requests_lock:
                self._ongoing_requests[request.uuid] = request
        return request.future

    def _publish_request(self, request, worker):
        """Publish request to a given topic."""
        LOG.debug("Submitting execution of '%s' to worker '%s' (expecting"
                  " response identified by reply_to=%s and"
                  " correlation_id=%s) - waited %0.3f seconds to"
                  " get published", request, worker, self._topic,
                  request.uuid, timeutils.now() - request.created_on)
        try:
            self._proxy.publish(request, worker.topic,
                                reply_to=self._topic,
                                correlation_id=request.uuid)
        except Exception:
            with misc.capture_failure() as failure:
                LOG.critical("Failed to submit '%s' (transitioning it to"
                             " %s)", request, pr.FAILURE, exc_info=True)
                if request.transition_and_log_error(pr.FAILURE, logger=LOG):
                    with self._ongoing_requests_lock:
                        del self._ongoing_requests[request.uuid]
                    request.set_result(failure)

    def execute_task(self, task, task_uuid, arguments,
                     progress_callback=None):
        return self._submit_task(task, task_uuid, pr.EXECUTE, arguments,
                                 progress_callback=progress_callback)

    def revert_task(self, task, task_uuid, arguments, result, failures,
                    progress_callback=None):
        return self._submit_task(task, task_uuid, pr.REVERT, arguments,
                                 result=result, failures=failures,
                                 progress_callback=progress_callback)

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
        if self._activator.need_to_be_stopped:
            raise RuntimeError("Worker executor must be stopped before"
                               " it can be started")

        self._activator.start()

    def stop(self):
        """Stops proxy thread and associated topic notification thread."""
        self._activator.stop()
        self._finder.clear()
        self._ongoing_requests = {}
