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

from taskflow import engines
from taskflow import exceptions as excp
from taskflow.utils import lock_utils


@six.add_metaclass(abc.ABCMeta)
class Conductor(object):
    """Conductors conduct jobs & assist in associated runtime interactions.

    Conductors act as entities which extract jobs from a jobboard, assign
    there work to some engine (using some desired configuration) and then wait
    for that work to complete. If the work fails then they abandon the claimed
    work (or if the process they are running in crashes or dies this
    abandonment happens automatically) and then another conductor at a later
    period of time will finish up the prior failed conductors work.
    """

    def __init__(self, name, jobboard, persistence,
                 engine=None, engine_options=None):
        self._name = name
        self._jobboard = jobboard
        self._engine = engine
        if not engine_options:
            self._engine_options = {}
        else:
            self._engine_options = engine_options.copy()
        self._persistence = persistence
        self._lock = threading.RLock()

    def _flow_detail_from_job(self, job):
        """Extracts a flow detail from a job (via some manner).

        The current mechanism to accomplish this is the following choices:

        * If the job details provide a 'flow_uuid' key attempt to load this
          key from the jobs book and use that as the flow_detail to run.
        * If the job details does not have have a 'flow_uuid' key then attempt
          to examine the size of the book and if it's only one element in the
          book (aka one flow_detail) then just use that.
        * Otherwise if there is no 'flow_uuid' defined or there are > 1
          flow_details in the book raise an error that corresponds to being
          unable to locate the correct flow_detail to run.
        """
        book = job.book
        if book is None:
            raise excp.NotFound("No book found in job")
        if job.details and 'flow_uuid' in job.details:
            flow_uuid = job.details["flow_uuid"]
            flow_detail = book.find(flow_uuid)
            if flow_detail is None:
                raise excp.NotFound("No matching flow detail found in"
                                    " jobs book for flow detail"
                                    " with uuid %s" % flow_uuid)
        else:
            choices = len(book)
            if choices == 1:
                flow_detail = list(book)[0]
            elif choices == 0:
                raise excp.NotFound("No flow detail(s) found in jobs book")
            else:
                raise excp.MultipleChoices("No matching flow detail found (%s"
                                           " choices) in jobs book" % choices)
        return flow_detail

    def _engine_from_job(self, job):
        """Extracts an engine from a job (via some manner)."""
        flow_detail = self._flow_detail_from_job(job)
        if job.details and 'store' in job.details:
            store = dict(job.details["store"])
        else:
            store = {}
        return engines.load_from_detail(flow_detail, store=store,
                                        engine=self._engine,
                                        backend=self._persistence,
                                        **self._engine_options)

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
        """Continuously claims, runs, and consumes jobs (and repeat)."""

    @abc.abstractmethod
    def _dispatch_job(self, job):
        """Dispatches a claimed job for work completion.

        Accepts a single (already claimed) job and causes it to be run in
        an engine. Returns a future object that represented the work to be
        completed sometime in the future. The future should return a single
        boolean from its result() method. This boolean determines whether the
        job will be consumed (true) or whether it should be abandoned (false).

        :param job: A job instance that has already been claimed by the
                    jobboard.
        """
