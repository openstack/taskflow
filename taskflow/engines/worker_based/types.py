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

import abc
import functools
import itertools
import random
import threading

from oslo_utils import reflection
import six

from taskflow.engines.worker_based import protocol as pr
from taskflow import logging
from taskflow.types import cache as base
from taskflow.types import notifier
from taskflow.types import periodic
from taskflow.types import timing as tt
from taskflow.utils import kombu_utils as ku

LOG = logging.getLogger(__name__)


class RequestsCache(base.ExpiringCache):
    """Represents a thread-safe requests cache."""

    def get_waiting_requests(self, worker):
        """Get list of waiting requests that the given worker can satisfy."""
        waiting_requests = []
        with self._lock:
            for request in six.itervalues(self._data):
                if request.state == pr.WAITING \
                   and worker.performs(request.task):
                    waiting_requests.append(request)
        return waiting_requests


# TODO(harlowja): this needs to be made better, once
# https://blueprints.launchpad.net/taskflow/+spec/wbe-worker-info is finally
# implemented we can go about using that instead.
class TopicWorker(object):
    """A (read-only) worker and its relevant information + useful methods."""

    _NO_IDENTITY = object()

    def __init__(self, topic, tasks, identity=_NO_IDENTITY):
        self.tasks = []
        for task in tasks:
            if not isinstance(task, six.string_types):
                task = reflection.get_class_name(task)
            self.tasks.append(task)
        self.topic = topic
        self.identity = identity

    def performs(self, task):
        if not isinstance(task, six.string_types):
            task = reflection.get_class_name(task)
        return task in self.tasks

    def __eq__(self, other):
        if not isinstance(other, TopicWorker):
            return NotImplemented
        if len(other.tasks) != len(self.tasks):
            return False
        if other.topic != self.topic:
            return False
        for task in other.tasks:
            if not self.performs(task):
                return False
        # If one of the identity equals _NO_IDENTITY, then allow it to match...
        if self._NO_IDENTITY in (self.identity, other.identity):
            return True
        else:
            return other.identity == self.identity

    def __repr__(self):
        r = reflection.get_class_name(self, fully_qualified=False)
        if self.identity is not self._NO_IDENTITY:
            r += "(identity=%s, tasks=%s, topic=%s)" % (self.identity,
                                                        self.tasks, self.topic)
        else:
            r += "(identity=*, tasks=%s, topic=%s)" % (self.tasks, self.topic)
        return r


@six.add_metaclass(abc.ABCMeta)
class WorkerFinder(object):
    """Base class for worker finders..."""

    #: Event type emitted when a new worker arrives.
    WORKER_ARRIVED = 'worker_arrived'

    def __init__(self):
        self._cond = threading.Condition()
        self.notifier = notifier.RestrictedNotifier([self.WORKER_ARRIVED])

    @abc.abstractmethod
    def _total_workers(self):
        """Returns how many workers are known."""

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
        watch = tt.StopWatch(duration=timeout)
        watch.start()
        with self._cond:
            while self._total_workers() < workers:
                if watch.expired():
                    return max(0, workers - self._total_workers())
                self._cond.wait(watch.leftover(return_none=True))
            return 0

    @staticmethod
    def _match_worker(task, available_workers):
        """Select a worker (from geq 1 workers) that can best perform the task.

        NOTE(harlowja): this method will be activated when there exists
        one one greater than one potential workers that can perform a task,
        the arguments provided will be the potential workers located and the
        task that is being requested to perform and the result should be one
        of those workers using whatever best-fit algorithm is possible (or
        random at the least).
        """
        if len(available_workers) == 1:
            return available_workers[0]
        else:
            return random.choice(available_workers)

    @abc.abstractmethod
    def get_worker_for_task(self, task):
        """Gets a worker that can perform a given task."""

    def clear(self):
        pass


class ProxyWorkerFinder(WorkerFinder):
    """Requests and receives responses about workers topic+task details."""

    def __init__(self, uuid, proxy, topics):
        super(ProxyWorkerFinder, self).__init__()
        self._proxy = proxy
        self._topics = topics
        self._workers = {}
        self._uuid = uuid
        self._proxy.dispatcher.type_handlers.update({
            pr.NOTIFY: [
                self._process_response,
                functools.partial(pr.Notify.validate, response=True),
            ],
        })
        self._counter = itertools.count()

    def _next_worker(self, topic, tasks, temporary=False):
        if not temporary:
            return TopicWorker(topic, tasks,
                               identity=six.next(self._counter))
        else:
            return TopicWorker(topic, tasks)

    @periodic.periodic(pr.NOTIFY_PERIOD)
    def beat(self):
        """Cyclically called to publish notify message to each topic."""
        self._proxy.publish(pr.Notify(), self._topics, reply_to=self._uuid)

    def _total_workers(self):
        return len(self._workers)

    def _add(self, topic, tasks):
        """Adds/updates a worker for the topic for the given tasks."""
        try:
            worker = self._workers[topic]
            # Check if we already have an equivalent worker, if so just
            # return it...
            if worker == self._next_worker(topic, tasks, temporary=True):
                return (worker, False)
            # This *fall through* is done so that if someone is using an
            # active worker object that already exists that we just create
            # a new one; so that the existing object doesn't get
            # affected (workers objects are supposed to be immutable).
        except KeyError:
            pass
        worker = self._next_worker(topic, tasks)
        self._workers[topic] = worker
        return (worker, True)

    def _process_response(self, response, message):
        """Process notify message from remote side."""
        LOG.debug("Started processing notify message '%s'",
                  ku.DelayedPretty(message))
        topic = response['topic']
        tasks = response['tasks']
        with self._cond:
            worker, new_or_updated = self._add(topic, tasks)
            if new_or_updated:
                LOG.debug("Received notification about worker '%s' (%s"
                          " total workers are currently known)", worker,
                          self._total_workers())
                self._cond.notify_all()
        if new_or_updated:
            self.notifier.notify(self.WORKER_ARRIVED, {'worker': worker})

    def clear(self):
        with self._cond:
            self._workers.clear()
            self._cond.notify_all()

    def get_worker_for_task(self, task):
        available_workers = []
        with self._cond:
            for worker in six.itervalues(self._workers):
                if worker.performs(task):
                    available_workers.append(worker)
        if available_workers:
            return self._match_worker(task, available_workers)
        else:
            return None
