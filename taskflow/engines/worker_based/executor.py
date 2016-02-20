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
import threading

from oslo_utils import timeutils
import six

from taskflow.engines.action_engine import executor
from taskflow.engines.worker_based import dispatcher
from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import proxy
from taskflow.engines.worker_based import types as wt
from taskflow import exceptions as exc
from taskflow import logging
from taskflow.task import EVENT_UPDATE_PROGRESS  # noqa
from taskflow.utils import kombu_utils as ku
from taskflow.utils import misc
from taskflow.utils import threading_utils as tu

LOG = logging.getLogger(__name__)


class WorkerTaskExecutor(executor.TaskExecutor):
    """Executes tasks on remote workers."""

    def __init__(self, uuid, exchange, topics,
                 transition_timeout=pr.REQUEST_TIMEOUT,
                 url=None, transport=None, transport_options=None,
                 retry_options=None, worker_expiry=pr.EXPIRES_AFTER):
        self._uuid = uuid
        self._ongoing_requests = {}
        self._ongoing_requests_lock = threading.RLock()
        self._transition_timeout = transition_timeout
        self._proxy = proxy.Proxy(uuid, exchange,
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
        self._finder = wt.ProxyWorkerFinder(uuid, self._proxy, topics,
                                            worker_expiry=worker_expiry)
        self._proxy.dispatcher.type_handlers.update({
            pr.RESPONSE: dispatcher.Handler(self._process_response,
                                            validator=pr.Response.validate),
            pr.NOTIFY: dispatcher.Handler(
                self._finder.process_response,
                validator=functools.partial(pr.Notify.validate,
                                            response=True)),
        })
        # Thread that will run the message dispatching (and periodically
        # call the on_wait callback to do various things) loop...
        self._helper = None
        self._messages_processed = {
            'finder': self._finder.messages_processed,
        }

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
        # Publish any finding messages (used to locate workers).
        self._finder.maybe_publish()
        # If the finder hasn't heard from workers in a given amount
        # of time, then those workers are likely dead, so clean them out...
        self._finder.clean()
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
        # Get task's worker and publish request if worker was found.
        worker = self._finder.get_worker_for_task(task)
        if worker is not None:
            if request.transition_and_log_error(pr.PENDING, logger=LOG):
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
                  " get published", request, worker, self._uuid,
                  request.uuid, timeutils.now() - request.created_on)
        try:
            self._proxy.publish(request, worker.topic,
                                reply_to=self._uuid,
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
        """Starts message processing thread."""
        if self._helper is not None:
            raise RuntimeError("Worker executor must be stopped before"
                               " it can be started")
        self._helper = tu.daemon_thread(self._proxy.start)
        self._helper.start()
        self._proxy.wait()

    def stop(self):
        """Stops message processing thread."""
        if self._helper is not None:
            self._proxy.stop()
            self._helper.join()
            self._helper = None
        with self._ongoing_requests_lock:
            while self._ongoing_requests:
                _request_uuid, request = self._ongoing_requests.popitem()
                self._handle_expired_request(request)
        self._finder.reset()
        self._messages_processed['finder'] = self._finder.messages_processed
