# -*- coding: utf-8 -*-

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
import threading

import six

import taskflow.engines
from taskflow import exceptions as excp
from taskflow.utils import lock_utils


@six.add_metaclass(abc.ABCMeta)
class Conductor(object):
    """Conductors act as entities which extract jobs from a jobboard, assign
    there work to some engine (using some desired configuration) and then wait
    for that work to complete. If the work fails then they abandon the claimed
    work (or if the process they are running in crashes or dies this
    abandonment happens automatically) and then another conductor at a later
    period of time will finish up the prior failed conductors work.
    """

    def __init__(self, name, jobboard, engine_conf, persistence):
        self._name = name
        self._jobboard = jobboard
        self._engine_conf = engine_conf
        self._persistence = persistence
        self._lock = threading.RLock()

    def _engine_from_job(self, job):
        try:
            flow_uuid = job.details["flow_uuid"]
        except (KeyError, TypeError):
            raise excp.NotFound("No flow detail uuid found in job")
        else:
            try:
                flow_detail = job.book.find(flow_uuid)
            except (TypeError, AttributeError):
                flow_detail = None
            if flow_detail is None:
                raise excp.NotFound("No matching flow detail found in"
                                    " job for flow detail uuid %s" % flow_uuid)
            try:
                store = dict(job.details["store"])
            except (KeyError, TypeError):
                store = {}
            return taskflow.engines.load_from_detail(
                flow_detail,
                store=store,
                engine_conf=dict(self._engine_conf),
                backend=self._persistence)

    @lock_utils.locked
    def connect(self):
        """Ensures the jobboard is connected (noop if it is already)."""
        if not self._jobboard.connected:
            self._jobboard.connect()

    @lock_utils.locked
    def close(self):
        """Closes the jobboard, disallowing further use."""
        self._jobboard.close()

    @abc.abstractmethod
    def run(self):
        """Continuously claims, runs, and consumes jobs, and waits for more
        jobs when there are none left on the jobboard.
        """

    @abc.abstractmethod
    def _dispatch_job(self, job):
        """Accepts a single (already claimed) job and causes it to be run in
        an engine. The job is consumed upon completion (unless False is
        returned which will signify the job should be abandoned instead)

        :param job: A Job instance that has already been claimed by the
            jobboard.
        """
