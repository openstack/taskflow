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

import itertools
import logging
import random
import threading

from oslo.utils import reflection
import six

from taskflow.engines.worker_based import protocol as pr
from taskflow.types import cache as base
from taskflow.types import timing as tt

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


class TopicWorkers(object):
    """A collection of topic based workers."""

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

    def __init__(self):
        self._workers = {}
        self._cond = threading.Condition()
        # Used to name workers with more useful identities...
        self._counter = itertools.count()

    def __len__(self):
        return len(self._workers)

    def _next_worker(self, topic, tasks, temporary=False):
        if not temporary:
            return TopicWorker(topic, tasks,
                               identity=six.next(self._counter))
        else:
            return TopicWorker(topic, tasks)

    def add(self, topic, tasks):
        """Adds/updates a worker for the topic for the given tasks."""
        with self._cond:
            try:
                worker = self._workers[topic]
                # Check if we already have an equivalent worker, if so just
                # return it...
                if worker == self._next_worker(topic, tasks, temporary=True):
                    return worker
                # This *fall through* is done so that if someone is using an
                # active worker object that already exists that we just create
                # a new one; so that the existing object doesn't get
                # affected (workers objects are supposed to be immutable).
            except KeyError:
                pass
            worker = self._next_worker(topic, tasks)
            self._workers[topic] = worker
            self._cond.notify_all()
        return worker

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
            while len(self._workers) < workers:
                if watch.expired():
                    return max(0, workers - len(self._workers))
                self._cond.wait(watch.leftover(return_none=True))
            return 0

    def get_worker_for_task(self, task):
        """Gets a worker that can perform a given task."""
        available_workers = []
        with self._cond:
            for worker in six.itervalues(self._workers):
                if worker.performs(task):
                    available_workers.append(worker)
        if available_workers:
            return self._match_worker(task, available_workers)
        else:
            return None

    def clear(self):
        with self._cond:
            self._workers.clear()
            self._cond.notify_all()


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
