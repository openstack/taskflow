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

import random
import threading

from oslo_utils import reflection
from oslo_utils import timeutils

from taskflow.engines.worker_based import protocol as pr
from taskflow import logging
from taskflow.utils import kombu_utils as ku

LOG = logging.getLogger(__name__)


# TODO(harlowja): this needs to be made better, once
# https://blueprints.launchpad.net/taskflow/+spec/wbe-worker-info is finally
# implemented we can go about using that instead.
class TopicWorker(object):
    """A (read-only) worker and its relevant information + useful methods."""

    _NO_IDENTITY = object()

    def __init__(self, topic, tasks, identity=_NO_IDENTITY):
        self.tasks = []
        for task in tasks:
            if not isinstance(task, str):
                task = reflection.get_class_name(task)
            self.tasks.append(task)
        self.topic = topic
        self.identity = identity
        self.last_seen = None

    def performs(self, task):
        if not isinstance(task, str):
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

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        r = reflection.get_class_name(self, fully_qualified=False)
        if self.identity is not self._NO_IDENTITY:
            r += "(identity=%s, tasks=%s, topic=%s)" % (self.identity,
                                                        self.tasks, self.topic)
        else:
            r += "(identity=*, tasks=%s, topic=%s)" % (self.tasks, self.topic)
        return r


class ProxyWorkerFinder(object):
    """Requests and receives responses about workers topic+task details."""

    def __init__(self, uuid, proxy, topics,
                 beat_periodicity=pr.NOTIFY_PERIOD,
                 worker_expiry=pr.EXPIRES_AFTER):
        self._cond = threading.Condition()
        self._proxy = proxy
        self._topics = topics
        self._workers = {}
        self._uuid = uuid
        self._seen_workers = 0
        self._messages_processed = 0
        self._messages_published = 0
        self._worker_expiry = worker_expiry
        self._watch = timeutils.StopWatch(duration=beat_periodicity)

    @property
    def total_workers(self):
        """Number of workers currently known."""
        return len(self._workers)

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
        watch = timeutils.StopWatch(duration=timeout)
        watch.start()
        with self._cond:
            while self.total_workers < workers:
                if watch.expired():
                    return max(0, workers - self.total_workers)
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

    @property
    def messages_processed(self):
        """How many notify response messages have been processed."""
        return self._messages_processed

    def _next_worker(self, topic, tasks, temporary=False):
        if not temporary:
            w = TopicWorker(topic, tasks, identity=self._seen_workers)
            self._seen_workers += 1
            return w
        else:
            return TopicWorker(topic, tasks)

    def maybe_publish(self):
        """Periodically called to publish notify message to each topic.

        These messages (especially the responses) are how this find learns
        about workers and what tasks they can perform (so that we can then
        match workers to tasks to run).
        """
        if self._messages_published == 0:
            self._proxy.publish(pr.Notify(),
                                self._topics, reply_to=self._uuid)
            self._messages_published += 1
            self._watch.restart()
        else:
            if self._watch.expired():
                self._proxy.publish(pr.Notify(),
                                    self._topics, reply_to=self._uuid)
                self._messages_published += 1
                self._watch.restart()

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

    def process_response(self, data, message):
        """Process notify message sent from remote side."""
        LOG.debug("Started processing notify response message '%s'",
                  ku.DelayedPretty(message))
        response = pr.Notify(**data)
        LOG.debug("Extracted notify response '%s'", response)
        with self._cond:
            worker, new_or_updated = self._add(response.topic,
                                               response.tasks)
            if new_or_updated:
                LOG.debug("Updated worker '%s' (%s total workers are"
                          " currently known)", worker, self.total_workers)
                self._cond.notify_all()
            worker.last_seen = timeutils.now()
            self._messages_processed += 1

    def clean(self):
        """Cleans out any dead/expired/not responding workers.

        Returns how many workers were removed.
        """
        if (not self._workers or
                (self._worker_expiry is None or self._worker_expiry <= 0)):
            return 0
        dead_workers = {}
        with self._cond:
            now = timeutils.now()
            for topic, worker in self._workers.items():
                if worker.last_seen is None:
                    continue
                secs_since_last_seen = max(0, now - worker.last_seen)
                if secs_since_last_seen >= self._worker_expiry:
                    dead_workers[topic] = (worker, secs_since_last_seen)
            for topic in dead_workers.keys():
                self._workers.pop(topic)
            if dead_workers:
                self._cond.notify_all()
        if dead_workers and LOG.isEnabledFor(logging.INFO):
            for worker, secs_since_last_seen in dead_workers.values():
                LOG.info("Removed worker '%s' as it has not responded to"
                         " notification requests in %0.3f seconds",
                         worker, secs_since_last_seen)
        return len(dead_workers)

    def reset(self):
        """Resets finders internal state."""
        with self._cond:
            self._workers.clear()
            self._messages_processed = 0
            self._messages_published = 0
            self._seen_workers = 0
            self._cond.notify_all()

    def get_worker_for_task(self, task):
        """Gets a worker that can perform a given task."""
        available_workers = []
        with self._cond:
            for worker in self._workers.values():
                if worker.performs(task):
                    available_workers.append(worker)
        if available_workers:
            return self._match_worker(task, available_workers)
        else:
            return None
