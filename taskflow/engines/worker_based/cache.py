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

import six

from taskflow.engines.worker_based import protocol as pr
from taskflow.types import cache as base


class RequestsCache(base.ExpiringCache):
    """Represents a thread-safe requests cache."""

    def get_waiting_requests(self, tasks):
        """Get list of waiting requests by tasks."""
        waiting_requests = []
        with self._lock.read_lock():
            for request in six.itervalues(self._data):
                if request.state == pr.WAITING and request.task_cls in tasks:
                    waiting_requests.append(request)
        return waiting_requests


class WorkersCache(base.ExpiringCache):
    """Represents a thread-safe workers cache."""

    def get_topic_by_task(self, task):
        """Get topic for a given task."""
        available_topics = []
        with self._lock.read_lock():
            for topic, tasks in six.iteritems(self._data):
                if task in tasks:
                    available_topics.append(topic)
        return random.choice(available_topics) if available_topics else None
