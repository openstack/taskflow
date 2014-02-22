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
import six
import threading

from kombu import exceptions as kombu_exc

from taskflow.engines.action_engine import executor
from taskflow.engines.worker_based import protocol as pr
from taskflow.engines.worker_based import proxy
from taskflow.engines.worker_based import remote_task as rt
from taskflow import exceptions as exc
from taskflow.utils import async_utils
from taskflow.utils import misc
from taskflow.utils import persistence_utils as pu

LOG = logging.getLogger(__name__)


class WorkerTaskExecutor(executor.TaskExecutorBase):
    """Executes tasks on remote workers."""

    def __init__(self, uuid, exchange, workers_info, **kwargs):
        self._uuid = uuid
        self._proxy = proxy.Proxy(uuid, exchange, self._on_message,
                                  self._on_wait, **kwargs)
        self._proxy_thread = None
        self._remote_tasks = {}

        # TODO(skudriashev): This data should be collected from workers
        # using broadcast messages directly.
        self._workers_info = {}
        for topic, tasks in six.iteritems(workers_info):
            for task in tasks:
                self._workers_info[task] = topic

    def _get_proxy_thread(self):
        proxy_thread = threading.Thread(target=self._proxy.start)
        # NOTE(skudriashev): When the main thread is terminated unexpectedly
        # and proxy thread is still alive - it will prevent main thread from
        # exiting unless the daemon property is set to True.
        proxy_thread.daemon = True
        return proxy_thread

    def _on_message(self, response, message):
        """This method is called on incoming response."""
        LOG.debug("Got response: %s", response)
        try:
            # acknowledge message before processing.
            message.ack()
        except kombu_exc.MessageStateError:
            LOG.exception("Failed to acknowledge AMQP message")
        else:
            LOG.debug("AMQP message acknowledged.")
            # get task uuid from message correlation id parameter
            try:
                task_uuid = message.properties['correlation_id']
            except KeyError:
                LOG.warning("Got message with no 'correlation_id' property.")
            else:
                LOG.debug("Task uuid: '%s'", task_uuid)
                self._process_response(task_uuid, response)

    def _process_response(self, task_uuid, response):
        """Process response from remote side."""
        try:
            task = self._remote_tasks[task_uuid]
        except KeyError:
            LOG.debug("Task with id='%s' not found.", task_uuid)
        else:
            state = response.pop('state')
            if state == pr.RUNNING:
                task.set_running()
            elif state == pr.PROGRESS:
                task.on_progress(**response)
            elif state == pr.FAILURE:
                response['result'] = pu.failure_from_dict(response['result'])
                task.set_result(**response)
                self._remove_remote_task(task)
            elif state == pr.SUCCESS:
                task.set_result(**response)
                self._remove_remote_task(task)
            else:
                LOG.warning("Unexpected response status: '%s'", state)

    def _on_wait(self):
        """This function is called cyclically between draining events
        iterations to clean-up expired task requests.
        """
        expired_tasks = [task for task in six.itervalues(self._remote_tasks)
                         if task.expired]
        for task in expired_tasks:
            LOG.debug("Task request '%s' has expired.", task)
            task.set_result(misc.Failure.from_exception(
                exc.Timeout("Task request '%s' has expired" % task)))
            del self._remote_tasks[task.uuid]

    def _store_remote_task(self, task):
        """Store task in the remote tasks map."""
        self._remote_tasks[task.uuid] = task
        return task

    def _remove_remote_task(self, task):
        """Remove remote task from the tasks map."""
        if task.uuid in self._remote_tasks:
            del self._remote_tasks[task.uuid]

    def _submit_task(self, task, task_uuid, action, arguments,
                     progress_callback, timeout=pr.REQUEST_TIMEOUT, **kwargs):
        """Submit task request to workers."""
        remote_task = self._store_remote_task(
            rt.RemoteTask(task, task_uuid, action, arguments,
                          progress_callback, timeout, **kwargs)
        )
        try:
            # get task's workers topic to send request to
            try:
                topic = self._workers_info[remote_task.name]
            except KeyError:
                raise exc.NotFound("Workers topic not found for the '%s'"
                                   " task" % remote_task.name)
            else:
                # publish request
                request = remote_task.request
                LOG.debug("Sending request: %s", request)
                self._proxy.publish(request, remote_task.uuid,
                                    routing_key=topic, reply_to=self._uuid)
        except Exception:
            with misc.capture_failure() as failure:
                LOG.exception("Failed to submit the '%s' task", remote_task)
                self._remove_remote_task(remote_task)
                remote_task.set_result(failure)
        return remote_task.result

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
        """Start proxy thread."""
        if self._proxy_thread is None:
            self._proxy_thread = self._get_proxy_thread()
            self._proxy_thread.start()
            self._proxy.wait()

    def stop(self):
        """Stop proxy, so its thread would be gracefully terminated."""
        if self._proxy_thread is not None:
            if self._proxy_thread.is_alive():
                self._proxy.stop()
                self._proxy_thread.join()
            self._proxy_thread = None
