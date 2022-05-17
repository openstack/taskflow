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
import os
import threading

import fasteners

from taskflow import engines
from taskflow import exceptions as excp
from taskflow.types import entity
from taskflow.types import notifier
from taskflow.utils import misc


class Conductor(object, metaclass=abc.ABCMeta):
    """Base for all conductor implementations.

    Conductors act as entities which extract jobs from a jobboard, assign
    there work to some engine (using some desired configuration) and then wait
    for that work to complete. If the work fails then they abandon the claimed
    work (or if the process they are running in crashes or dies this
    abandonment happens automatically) and then another conductor at a later
    period of time will finish up the prior failed conductors work.
    """

    #: Entity kind used when creating new entity objects
    ENTITY_KIND = 'conductor'

    def __init__(self, name, jobboard,
                 persistence=None, engine=None, engine_options=None):
        self._name = name
        self._jobboard = jobboard
        self._engine = engine
        self._engine_options = misc.safe_copy_dict(engine_options)
        self._persistence = persistence
        self._lock = threading.RLock()
        self._notifier = notifier.Notifier()

    @misc.cachedproperty
    def conductor(self):
        """Entity object that represents this conductor."""
        hostname = misc.get_hostname()
        pid = os.getpid()
        name = '@'.join([self._name, hostname + ":" + str(pid)])
        metadata = {
            'hostname': hostname,
            'pid': pid,
        }
        return entity.Entity(self.ENTITY_KIND, name, metadata)

    @property
    def notifier(self):
        """The conductor actions (or other state changes) notifier.

        NOTE(harlowja): different conductor implementations may emit
        different events + event details at different times, so refer to your
        conductor documentation to know exactly what can and what can not be
        subscribed to.
        """
        return self._notifier

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
        store = {}

        if flow_detail.meta and 'store' in flow_detail.meta:
            store.update(flow_detail.meta['store'])

        if job.details and 'store' in job.details:
            store.update(job.details["store"])

        engine = engines.load_from_detail(flow_detail, store=store,
                                          engine=self._engine,
                                          backend=self._persistence,
                                          **self._engine_options)
        return engine

    def _listeners_from_job(self, job, engine):
        """Returns a list of listeners to be attached to an engine.

        This method should be overridden in order to attach listeners to
        engines. It will be called once for each job, and the list returned
        listeners will be added to the engine for this job.

        :param job: A job instance that is about to be run in an engine.
        :param engine: The engine that listeners will be attached to.
        :returns: a list of (unregistered) listener instances.
        """
        # TODO(dkrause): Create a standard way to pass listeners or
        #                listener factories over the jobboard
        return []

    @fasteners.locked
    def connect(self):
        """Ensures the jobboard is connected (noop if it is already)."""
        if not self._jobboard.connected:
            self._jobboard.connect()

    @fasteners.locked
    def close(self):
        """Closes the contained jobboard, disallowing further use."""
        self._jobboard.close()

    @abc.abstractmethod
    def run(self, max_dispatches=None):
        """Continuously claims, runs, and consumes jobs (and repeat).

        :param max_dispatches: An upper bound on the number of jobs that will
                               be dispatched, if none or negative this implies
                               there is no limit to the number of jobs that
                               will be dispatched, otherwise if positive this
                               run method will return when that amount of jobs
                               has been dispatched (instead of running
                               forever and/or until stopped).
        """

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
