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

import logging

import six

from taskflow.conductors import base
from taskflow import exceptions as excp
from taskflow.listeners import logging as logging_listener
from taskflow.types import timing as tt
from taskflow.utils import async_utils
from taskflow.utils import lock_utils
from taskflow.utils import threading_utils

LOG = logging.getLogger(__name__)
WAIT_TIMEOUT = 0.5
NO_CONSUME_EXCEPTIONS = tuple([
    excp.ExecutionFailure,
    excp.StorageFailure,
])


class SingleThreadedConductor(base.Conductor):
    """A conductor that runs jobs in its own dispatching loop.

    This conductor iterates over jobs in the provided jobboard (waiting for
    the given timeout if no jobs exist) and attempts to claim them, work on
    those jobs in its local thread (blocking further work from being claimed
    and consumed) and then consume those work units after completetion. This
    process will repeat until the conductor has been stopped or other critical
    error occurs.

    NOTE(harlowja): consumption occurs even if a engine fails to run due to
    a task failure. This is only skipped when an execution failure or
    a storage failure occurs which are *usually* correctable by re-running on
    a different conductor (storage failures and execution failures may be
    transient issues that can be worked around by later execution). If a job
    after completing can not be consumed or abandoned the conductor relies
    upon the jobboard capabilities to automatically abandon these jobs.
    """

    def __init__(self, name, jobboard, persistence,
                 engine=None, engine_options=None, wait_timeout=None):
        super(SingleThreadedConductor, self).__init__(
            name, jobboard, persistence,
            engine=engine, engine_options=engine_options)
        if wait_timeout is None:
            wait_timeout = WAIT_TIMEOUT
        if isinstance(wait_timeout, (int, float) + six.string_types):
            self._wait_timeout = tt.Timeout(float(wait_timeout))
        elif isinstance(wait_timeout, tt.Timeout):
            self._wait_timeout = wait_timeout
        else:
            raise ValueError("Invalid timeout literal: %s" % (wait_timeout))
        self._dead = threading_utils.Event()

    @lock_utils.locked
    def stop(self, timeout=None):
        """Requests the conductor to stop dispatching.

        This method can be used to request that a conductor stop its
        consumption & dispatching loop. It returns whether the stop request
        was successfully completed. If the dispatching is still occurring
        then False is returned otherwise True will be returned to signal that
        the conductor is no longer consuming & dispatching job requests.

        NOTE(harlowja): If a timeout is provided the dispatcher loop may
        not have ceased by the timeout reached (the request to cease will
        be honored in the future) and False will be returned indicating this.
        """
        self._wait_timeout.interrupt()
        return self._dead.wait(timeout)

    @property
    def dispatching(self):
        return not self._dead.is_set()

    def _dispatch_job(self, job):
        engine = self._engine_from_job(job)
        consume = True
        with logging_listener.LoggingListener(engine, log=LOG):
            LOG.debug("Dispatching engine %s for job: %s", engine, job)
            try:
                engine.run()
            except excp.WrappedFailure as e:
                if all((f.check(*NO_CONSUME_EXCEPTIONS) for f in e)):
                    consume = False
                if LOG.isEnabledFor(logging.WARNING):
                    if consume:
                        LOG.warn("Job execution failed (consumption being"
                                 " skipped): %s [%s failures]", job, len(e))
                    else:
                        LOG.warn("Job execution failed (consumption"
                                 " proceeding): %s [%s failures]", job, len(e))
                    # Show the failure/s + traceback (if possible)...
                    for i, f in enumerate(e):
                        LOG.warn("%s. %s", i + 1, f.pformat(traceback=True))
            except NO_CONSUME_EXCEPTIONS:
                LOG.warn("Job execution failed (consumption being"
                         " skipped): %s", job, exc_info=True)
                consume = False
            except Exception:
                LOG.warn("Job execution failed (consumption proceeding): %s",
                         job, exc_info=True)
            else:
                LOG.info("Job completed successfully: %s", job)
        return async_utils.make_completed_future(consume)

    def run(self):
        self._dead.clear()
        try:
            while True:
                if self._wait_timeout.is_stopped():
                    break
                dispatched = 0
                for job in self._jobboard.iterjobs():
                    if self._wait_timeout.is_stopped():
                        break
                    LOG.debug("Trying to claim job: %s", job)
                    try:
                        self._jobboard.claim(job, self._name)
                    except (excp.UnclaimableJob, excp.NotFound):
                        LOG.debug("Job already claimed or consumed: %s", job)
                        continue
                    consume = False
                    try:
                        f = self._dispatch_job(job)
                    except Exception:
                        LOG.warn("Job dispatching failed: %s", job,
                                 exc_info=True)
                    else:
                        dispatched += 1
                        consume = f.result()
                    try:
                        if consume:
                            self._jobboard.consume(job, self._name)
                        else:
                            self._jobboard.abandon(job, self._name)
                    except excp.JobFailure:
                        if consume:
                            LOG.warn("Failed job consumption: %s", job,
                                     exc_info=True)
                        else:
                            LOG.warn("Failed job abandonment: %s", job,
                                     exc_info=True)
                if dispatched == 0 and not self._wait_timeout.is_stopped():
                    self._wait_timeout.wait()
        finally:
            self._dead.set()
