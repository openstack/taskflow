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
import functools
import itertools
import threading

try:
    from contextlib import ExitStack  # noqa
except ImportError:
    from contextlib2 import ExitStack  # noqa

from debtcollector import removals
from oslo_utils import excutils
from oslo_utils import timeutils
import six

from taskflow.conductors import base
from taskflow import exceptions as excp
from taskflow.listeners import logging as logging_listener
from taskflow import logging
from taskflow import states
from taskflow.types import timing as tt
from taskflow.utils import iter_utils
from taskflow.utils import misc

LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class ExecutorConductor(base.Conductor):
    """Dispatches jobs from blocking :py:meth:`.run` method to some executor.

    This conductor iterates over jobs in the provided jobboard (waiting for
    the given timeout if no jobs exist) and attempts to claim them, work on
    those jobs using an executor (potentially blocking further work from being
    claimed and consumed) and then consume those work units after
    completion. This process will repeat until the conductor has been stopped
    or other critical error occurs.

    NOTE(harlowja): consumption occurs even if a engine fails to run due to
    a atom failure. This is only skipped when an execution failure or
    a storage failure occurs which are *usually* correctable by re-running on
    a different conductor (storage failures and execution failures may be
    transient issues that can be worked around by later execution). If a job
    after completing can not be consumed or abandoned the conductor relies
    upon the jobboard capabilities to automatically abandon these jobs.
    """

    LOG = None
    """
    Logger that will be used for listening to events (if none then the module
    level logger will be used instead).
    """

    REFRESH_PERIODICITY = 30
    """
    Every 30 seconds the jobboard will be resynced (if for some reason
    a watch or set of watches was not received) using the `ensure_fresh`
    option to ensure this (for supporting jobboard backends only).
    """

    #: Default timeout used to idle/wait when no jobs have been found.
    WAIT_TIMEOUT = 0.5

    MAX_SIMULTANEOUS_JOBS = -1
    """
    Default maximum number of jobs that can be in progress at the same time.

    Negative or zero values imply no limit (do note that if a executor is
    used that is built on a queue, as most are, that this will imply that the
    queue will contain a potentially large & unfinished backlog of
    submitted jobs). This *may* get better someday if
    https://bugs.python.org/issue22737 is ever implemented and released.
    """

    #: Exceptions that will **not** cause consumption to occur.
    NO_CONSUME_EXCEPTIONS = tuple([
        excp.ExecutionFailure,
        excp.StorageFailure,
    ])

    _event_factory = threading.Event
    """This attribute *can* be overridden by subclasses (for example if
       an eventlet *green* event works better for the conductor user)."""

    EVENTS_EMITTED = tuple([
        'compilation_start', 'compilation_end',
        'preparation_start', 'preparation_end',
        'validation_start', 'validation_end',
        'running_start', 'running_end',
        'job_consumed', 'job_abandoned',
    ])
    """Events will be emitted for each of the events above.  The event is
       emitted to listeners registered with the conductor.
    """

    def __init__(self, name, jobboard,
                 persistence=None, engine=None,
                 engine_options=None, wait_timeout=None,
                 log=None, max_simultaneous_jobs=MAX_SIMULTANEOUS_JOBS):
        super(ExecutorConductor, self).__init__(
            name, jobboard, persistence=persistence,
            engine=engine, engine_options=engine_options)
        self._wait_timeout = tt.convert_to_timeout(
            value=wait_timeout, default_value=self.WAIT_TIMEOUT,
            event_factory=self._event_factory)
        self._dead = self._event_factory()
        self._log = misc.pick_first_not_none(log, self.LOG, LOG)
        self._max_simultaneous_jobs = int(
            misc.pick_first_not_none(max_simultaneous_jobs,
                                     self.MAX_SIMULTANEOUS_JOBS))
        self._dispatched = set()

    def _executor_factory(self):
        """Creates an executor to be used during dispatching."""
        raise excp.NotImplementedError("This method must be implemented but"
                                       " it has not been")

    @removals.removed_kwarg('timeout', version="0.8", removal_version="2.0")
    def stop(self, timeout=None):
        """Requests the conductor to stop dispatching.

        This method can be used to request that a conductor stop its
        consumption & dispatching loop.

        The method returns immediately regardless of whether the conductor has
        been stopped.
        """
        self._wait_timeout.interrupt()

    @property
    def dispatching(self):
        """Whether or not the dispatching loop is still dispatching."""
        return not self._dead.is_set()

    def _listeners_from_job(self, job, engine):
        listeners = super(ExecutorConductor, self)._listeners_from_job(
            job, engine)
        listeners.append(logging_listener.LoggingListener(engine,
                                                          log=self._log))
        return listeners

    def _dispatch_job(self, job):
        engine = self._engine_from_job(job)
        listeners = self._listeners_from_job(job, engine)
        with ExitStack() as stack:
            for listener in listeners:
                stack.enter_context(listener)
            self._log.debug("Dispatching engine for job '%s'", job)
            consume = True
            details = {
                'job': job,
                'engine': engine,
                'conductor': self,
            }

            def _run_engine():
                has_suspended = False
                for _state in engine.run_iter():
                    if not has_suspended and self._wait_timeout.is_stopped():
                        self._log.info("Conductor stopped, requesting "
                                       "suspension of engine running "
                                       "job %s", job)
                        engine.suspend()
                        has_suspended = True

            try:
                for stage_func, event_name in [(engine.compile, 'compilation'),
                                               (engine.prepare, 'preparation'),
                                               (engine.validate, 'validation'),
                                               (_run_engine, 'running')]:
                    self._notifier.notify("%s_start" % event_name,  details)
                    stage_func()
                    self._notifier.notify("%s_end" % event_name, details)
            except excp.WrappedFailure as e:
                if all((f.check(*self.NO_CONSUME_EXCEPTIONS) for f in e)):
                    consume = False
                if self._log.isEnabledFor(logging.WARNING):
                    if consume:
                        self._log.warn(
                            "Job execution failed (consumption being"
                            " skipped): %s [%s failures]", job, len(e))
                    else:
                        self._log.warn(
                            "Job execution failed (consumption"
                            " proceeding): %s [%s failures]", job, len(e))
                    # Show the failure/s + traceback (if possible)...
                    for i, f in enumerate(e):
                        self._log.warn("%s. %s", i + 1,
                                       f.pformat(traceback=True))
            except self.NO_CONSUME_EXCEPTIONS:
                self._log.warn("Job execution failed (consumption being"
                               " skipped): %s", job, exc_info=True)
                consume = False
            except Exception:
                self._log.warn(
                    "Job execution failed (consumption proceeding): %s",
                    job, exc_info=True)
            else:
                if engine.storage.get_flow_state() == states.SUSPENDED:
                    self._log.info("Job execution was suspended: %s", job)
                    consume = False
                else:
                    self._log.info("Job completed successfully: %s", job)
            return consume

    def _try_finish_job(self, job, consume):
        try:
            if consume:
                self._jobboard.consume(job, self._name)
                self._notifier.notify("job_consumed", {
                    'job': job,
                    'conductor': self,
                    'persistence': self._persistence,
                })
            else:
                self._jobboard.abandon(job, self._name)
                self._notifier.notify("job_abandoned", {
                    'job': job,
                    'conductor': self,
                    'persistence': self._persistence,
                })
        except (excp.JobFailure, excp.NotFound):
            if consume:
                self._log.warn("Failed job consumption: %s", job,
                               exc_info=True)
            else:
                self._log.warn("Failed job abandonment: %s", job,
                               exc_info=True)

    def _on_job_done(self, job, fut):
        consume = False
        try:
            consume = fut.result()
        except KeyboardInterrupt:
            with excutils.save_and_reraise_exception():
                self._log.warn("Job dispatching interrupted: %s", job)
        except Exception:
            self._log.warn("Job dispatching failed: %s", job, exc_info=True)
        try:
            self._try_finish_job(job, consume)
        finally:
            self._dispatched.discard(fut)

    def _can_claim_more_jobs(self, job):
        if self._wait_timeout.is_stopped():
            return False
        if self._max_simultaneous_jobs <= 0:
            return True
        if len(self._dispatched) >= self._max_simultaneous_jobs:
            return False
        else:
            return True

    def _run_until_dead(self, executor, max_dispatches=None):
        total_dispatched = 0
        if max_dispatches is None:
            # NOTE(TheSriram): if max_dispatches is not set,
            # then the  conductor will run indefinitely, and not
            # stop after 'n' number of dispatches
            max_dispatches = -1
        dispatch_gen = iter_utils.iter_forever(max_dispatches)
        is_stopped = self._wait_timeout.is_stopped
        try:
            # Don't even do any work in the first place...
            if max_dispatches == 0:
                raise StopIteration
            fresh_period = timeutils.StopWatch(
                duration=self.REFRESH_PERIODICITY)
            fresh_period.start()
            while not is_stopped():
                any_dispatched = False
                if fresh_period.expired():
                    ensure_fresh = True
                    fresh_period.restart()
                else:
                    ensure_fresh = False
                job_it = itertools.takewhile(
                    self._can_claim_more_jobs,
                    self._jobboard.iterjobs(ensure_fresh=ensure_fresh))
                for job in job_it:
                    self._log.debug("Trying to claim job: %s", job)
                    try:
                        self._jobboard.claim(job, self._name)
                    except (excp.UnclaimableJob, excp.NotFound):
                        self._log.debug("Job already claimed or"
                                        " consumed: %s", job)
                    else:
                        try:
                            fut = executor.submit(self._dispatch_job, job)
                        except RuntimeError:
                            with excutils.save_and_reraise_exception():
                                self._log.warn("Job dispatch submitting"
                                               " failed: %s", job)
                                self._try_finish_job(job, False)
                        else:
                            fut.job = job
                            self._dispatched.add(fut)
                            any_dispatched = True
                            fut.add_done_callback(
                                functools.partial(self._on_job_done, job))
                            total_dispatched = next(dispatch_gen)
                if not any_dispatched and not is_stopped():
                    self._wait_timeout.wait()
        except StopIteration:
            # This will be raised from 'dispatch_gen' if it reaches its
            # max dispatch number (which implies we should do no more work).
            with excutils.save_and_reraise_exception():
                if max_dispatches >= 0 and total_dispatched >= max_dispatches:
                    self._log.info("Maximum dispatch limit of %s reached",
                                   max_dispatches)

    def run(self, max_dispatches=None):
        self._dead.clear()
        self._dispatched.clear()
        try:
            self._jobboard.register_entity(self.conductor)
            with self._executor_factory() as executor:
                self._run_until_dead(executor,
                                     max_dispatches=max_dispatches)
        except StopIteration:
            pass
        except KeyboardInterrupt:
            with excutils.save_and_reraise_exception():
                self._log.warn("Job dispatching interrupted")
        finally:
            self._dead.set()

    # Inherit the docs, so we can reference them in our class docstring,
    # if we don't do this sphinx gets confused...
    run.__doc__ = base.Conductor.run.__doc__

    def wait(self, timeout=None):
        """Waits for the conductor to gracefully exit.

        This method waits for the conductor to gracefully exit. An optional
        timeout can be provided, which will cause the method to return
        within the specified timeout. If the timeout is reached, the returned
        value will be ``False``, otherwise it will be ``True``.

        :param timeout: Maximum number of seconds that the :meth:`wait` method
                        should block for.
        """
        return self._dead.wait(timeout)
