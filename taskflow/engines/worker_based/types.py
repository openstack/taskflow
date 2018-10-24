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
import random
import threading

from futurist import periodics
from oslo_utils import reflection
from oslo_utils import timeutils
import six
import tooz
from tooz import coordination

from taskflow.engines.worker_based import dispatcher
from taskflow.engines.worker_based import protocol as pr
from taskflow import exceptions as excp
from taskflow import logging
from taskflow.types import notifier
from taskflow.utils import kombu_utils as ku
from taskflow.utils import misc
from taskflow.utils import threading_utils as tu

LOG = logging.getLogger(__name__)


class TopicWorker(object):
    """A immutable worker and its relevant information + useful methods."""

    #: Anonymous/no identity string.
    NO_IDENTITY = object()

    def __init__(self, topic, tasks, identity=NO_IDENTITY):
        extracted_tasks = []
        for task in tasks:
            if not isinstance(task, six.string_types):
                task = reflection.get_class_name(task)
            extracted_tasks.append(task)
        self.tasks = tuple(extracted_tasks)
        self.topic = topic
        self.identity = identity
        self.last_seen = None

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
        # If one of the identity equals NO_IDENTITY, then allow it to match...
        if self.NO_IDENTITY in (self.identity, other.identity):
            return True
        else:
            return other.identity == self.identity

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        r = reflection.get_class_name(self, fully_qualified=False)
        if self.identity is not self.NO_IDENTITY:
            r += "(identity=%s, tasks=%s, topic=%s)" % (self.identity,
                                                        self.tasks, self.topic)
        else:
            r += "(tasks=%s, topic=%s)" % (self.tasks, self.topic)
        return r


@six.add_metaclass(abc.ABCMeta)
class WorkerFinder(object):
    """Base class for worker finders..."""

    def __init__(self):
        self._cond = threading.Condition()
        self.notifier = notifier.Notifier()

    @abc.abstractproperty
    def available_workers(self):
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
            raise ValueError("Worker amount to wait for"
                             " must be greater than zero")
        with timeutils.StopWatch(duration=timeout) as watch:
            with self._cond:
                while self.available_workers < workers:
                    if watch.expired():
                        return max(0, workers - self.available_workers)
                    self._cond.wait(watch.leftover(return_none=True))
                return 0

    @staticmethod
    def _match_worker(task, available_workers):
        """Select a worker (from >= 1 workers) that can best perform the task.

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

    @staticmethod
    def clear():
        """Clears the known workers (subclass and implement as needed)."""

    @staticmethod
    def start():
        """Starts the finding process (subclass and implement as needed)."""

    @staticmethod
    def stop():
        """Stops the finding process (subclass and implement as needed)."""


class ProxyWorkerFinder(WorkerFinder):
    """Requests and receives responses about workers topic + task details.

    NOTE(harlowja): This is one of the simplest finder implementations that
    doesn't have external dependencies (outside of what the engine
    already requires) and is the default for this reason. It though does
    create periodic 'polling' traffic to workers to 'learn' of the tasks they
    can perform (and does require pre-existing knowledge of the topics
    those workers exist at to gather and update this information).
    """

    # TODO(harlowja): make these settings more configurable...

    #: How often we ask workers of there new/updated capabilities.
    NOTIFY_PERIOD = 5

    def __init__(self, uuid, proxy, topics,
                 beat_periodicity=NOTIFY_PERIOD,
                 worker_expiry=pr.EXPIRES_AFTER):
        super(ProxyWorkerFinder, self).__init__()
        self._proxy = proxy
        self._topics = topics
        self._workers = {}
        self._uuid = uuid
        self._seen_workers = 0
        self._messages_published = 0
        self._worker_expiry = worker_expiry
        self._watch = timeutils.StopWatch(duration=beat_periodicity)
        # TODO (jimbobhickville): this needs to be refactored
        self._proxy.dispatcher.type_handlers.update({
            pr.NOTIFY: dispatcher.Handler(
                self.process_response,
                validator=functools.partial(pr.Notify.validate,
                                            response=True)),
        })

    def _next_worker(self, topic, tasks, temporary=False):
        if not temporary:
            w = TopicWorker(topic, tasks, identity=self._seen_workers)
            self._seen_workers += 1
            return w
        else:
            return TopicWorker(topic, tasks)

    @classmethod
    def generate_factory(cls, options):
        """Returns a function that generates finders using provided options."""

        # TODO(harlowja): this should likely be a real entrypoint someday
        # in the future...
        topics = list(options.get('topics', []))
        worker_expiry = options.get('worker_expiry', pr.EXPIRES_AFTER)

        def factory(uuid, proxy):
            return cls(uuid, proxy, topics[:], worker_expiry=worker_expiry)

        return factory

    def discover(self):
        """Publish notify message to each topic to discover new workers

        The response expected from workers is then used to find later
        tasks to fullfill (as the response from a worker contains the
        tasks they can perform).
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

        self.clean()

    @property
    def available_workers(self):
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

    def process_response(self, data, message):
        """Process notify message (response) sent from remote side.

        NOTE(harlowja): the message content should already have had
        basic validation performed on it...
        """
        LOG.debug("Started processing notify response message '%s'",
                  ku.DelayedPretty(message))
        response = pr.Notify(**data)
        LOG.debug("Extracted notify response '%s'", response)
        with self._cond:
            worker, new_or_updated = self._add(response.topic,
                                               response.tasks)
            if new_or_updated:
                LOG.debug("Received notification about worker '%s' (%s"
                          " total workers are currently known)", worker,
                          self.available_workers)
                self._cond.notify_all()
            worker.last_seen = timeutils.now()

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
            for topic, worker in six.iteritems(self._workers):
                if worker.last_seen is None:
                    continue
                secs_since_last_seen = max(0, now - worker.last_seen)
                if secs_since_last_seen >= self._worker_expiry:
                    dead_workers[topic] = (worker, secs_since_last_seen)
            for topic in six.iterkeys(dead_workers):
                self._workers.pop(topic)
            if dead_workers:
                self._cond.notify_all()
        if dead_workers and LOG.isEnabledFor(logging.INFO):
            for worker, secs_since_last_seen in six.itervalues(dead_workers):
                self.notifier.notify(pr.WORKER_LOST, worker.__dict__)
                LOG.info("Removed worker '%s' as it has not responded to"
                         " notification requests in %0.3f seconds",
                         worker, secs_since_last_seen)
        return len(dead_workers)

    def reset(self):
        """Resets finders internal state."""
        with self._cond:
            self._workers.clear()
            self._messages_published = 0
            self._seen_workers = 0
            self._cond.notify_all()

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


class ToozWorkerAdvertiser(object):
    """Advertiser that advertises worker capabilities via `tooz`_ groups.

    .. note::

        It only *advertises* a single workers capabilities (tasks
        the worker can perform and the topic the worker responds to on) via
        tooz groups & membership concepts. It **still** requires using a
        :py:class:`~taskflow.engines.worker_based.proxy.Proxy` for actually
        performing (and responding to) task requests.

    .. warning::

        This worker is still considered **experimental** and it needs to
        be used in conjunction with
        :py:class:`~taskflow.engines.worker_based.types.ToozWorkerFinder`
        finder/s to achieve a working architecture.

    .. _tooz: http://docs.openstack.org/developer/tooz/
    """

    # TODO(harlowja): make these settings more configurable...

    BEAT_PERIOD = 1
    """
    This is how often we want to heartbeat into the coordinator
    backend so that we don't get ejected from any groups we are in
    or lose our connection to that backend; aka this ensures the backend
    knows we are still alive/not dead.
    """

    MEMBER_PREFIX = "a+"
    """
    Prefix used along with the workers topic used to form a tooz member
    id (only used when a member id was not provided via the factory functions
    options).
    """

    def __init__(self, coordinator_url, member_id, join_groups,
                 worker_topic, worker_endpoints):
        self._coordinator = coordination.get_coordinator(coordinator_url,
                                                         member_id)
        self._join_groups = frozenset(join_groups)
        self._helpers = tu.ThreadBundle()
        self._member_id = member_id
        self._capabilities = {
            'topic': worker_topic,
            'tasks': [e.name for e in worker_endpoints],
        }
        p_worker = periodics.PeriodicWorker.create([self])
        if p_worker:
            self._helpers.bind(lambda: tu.daemon_thread(p_worker.start),
                               before_join=lambda t: p_worker.stop(),
                               after_join=lambda t: p_worker.reset(),
                               before_start=lambda t: p_worker.reset())
        self._activator = misc.Activator([self._coordinator, self._helpers])

    @classmethod
    def generate_factory(cls, options):
        """Returns a function that generates finders using provided options."""

        # TODO(harlowja): this should likely be a real entrypoint someday
        # in the future...
        options = options['coordinator']
        url = options['url']
        groups = list(options.get('groups', []))

        def factory(worker):
            member_id = options.get('member_id')
            if not member_id:
                member_id = "".join([cls.MEMBER_PREFIX, worker.topic])
            return cls(url, member_id, groups,
                       worker.topic, worker.endpoints)

        return factory

    @misc.cachedproperty
    def banner_additions(self):
        return {
            'Advertiser details': {
                'kind': reflection.get_class_name(self),
                'coordinator': reflection.get_class_name(self._coordinator),
                'member_id': self._member_id,
                'groups': sorted(self._join_groups),
            }
        }

    def advertise(self):
        # Always try to ensure that the groups we are supposed to be in exist
        # and that we are still a member of those groups (in-case we have a
        # disconnect, we may have been booted out, or those groups may have
        # gone away...); this logic tries to protect against those
        # scenarios...
        caps = self._capabilities
        raw_caps = pr.Capabilities.dumps(**caps)
        for group_id in self._join_groups:
            try:
                self._coordinator.create_group(group_id).get()
            except coordination.GroupAlreadyExist:
                pass
            try:
                self._coordinator.join_group(group_id,
                                             capabilities=raw_caps).get()
            except coordination.MemberAlreadyExist:
                pass
            else:
                LOG.debug("Joined into group '%s' with capabilities %s",
                          group_id, caps)

    def retract(self):
        for group_id in self._join_groups:
            try:
                self._coordinator.delete_group(group_id).get()
            except coordination.GroupAlreadyExist:
                pass

    @periodics.periodic(BEAT_PERIOD, run_immediately=True)
    def beat(self):
        # NOTE(harlowja): if the tooz kazoo driver is used, this is basically
        # a no-op since that driver maintains its own thread that heartbeats
        # to zookeeper (other tooz drivers though are not this lucky)...
        try:
            self._coordinator.heartbeat()
        except tooz.NotImplemented:
            pass

    def start(self):
        self._activator.start()
        self.advertise()

    def stop(self):
        self._activator.stop()
        self.retract()


class ToozWorkerFinder(WorkerFinder):
    """Learns of workers using scanned and watched `tooz`_ groups.

    .. warning::

        This finder is still considered **experimental** and it needs to
        be used in conjunction with
        :py:class:`~taskflow.engines.worker_based.types.ToozWorkerAdvertiser`
        workers to achieve a working architecture.

    .. _tooz: http://docs.openstack.org/developer/tooz/
    """

    # TODO(harlowja): make these settings more configurable...

    BEAT_PERIOD = 1
    """
    This is how often we want to heartbeat into the coordinator
    backend so that we don't get ejected from any groups we are in
    or lose our connection to that backend; aka this ensures the backend
    knows we are still alive/not dead.
    """

    MEMBER_PREFIX = "f+"
    """
    Prefix used along with the engines topic used to form a tooz member
    id (only used when a member id was not provided via the factory functions
    options).
    """

    NOTICE_PERIOD = 10
    """
    This is how often a **partial** membership discovery runs (it will only
    discover workers that have left/joined previously known groups that
    the instance has established watches on).
    """

    def __init__(self, coordination_url, member_id, watch_groups):
        super(ToozWorkerFinder, self).__init__()
        self._coordinator = coordination.get_coordinator(coordination_url,
                                                         member_id)
        self._watch_groups = watch_groups
        self._active_groups = {}
        self._available_workers = 0

        self._helpers = tu.ThreadBundle()
        p_worker = periodics.PeriodicWorker.create([self])
        if p_worker:
            self._helpers.bind(lambda: tu.daemon_thread(p_worker.start),
                               before_join=lambda t: p_worker.stop(),
                               after_join=lambda t: p_worker.reset(),
                               before_start=lambda t: p_worker.reset())
        self._activator = misc.Activator([self._coordinator, self._helpers])

    @classmethod
    def generate_factory(cls, options):
        # TODO(harlowja): this should likely be a real entrypoint someday
        # in the future...
        options = options['coordinator']
        url = options['url']
        groups = list(options.get('groups', []))

        def factory(topic, proxy):
            member_id = options.get('member_id')
            if not member_id:
                member_id = "".join([cls.MEMBER_PREFIX, topic])
            return cls(url, member_id, groups)

        return factory

    def clear(self):
        with self._cond:
            self._active_groups.clear()
            self._recalculate_available_workers()
            self._cond.notify_all()

    def get_worker_for_task(self, task):
        available_workers = []
        with self._cond:
            for workers in six.itervalues(self._active_groups):
                for worker in workers:
                    if worker.performs(task):
                        available_workers.append(worker)
        if available_workers:
            return self._match_worker(task, available_workers)
        else:
            return None

    def start(self):
        self.clear()
        self._activator.start()
        self.discover()

    def stop(self):
        self._activator.stop()
        self.clear()

    @property
    def available_workers(self):
        return self._available_workers

    def _recalculate_available_workers(self):
        # This is *always* called in a locked manner; if it is not then
        # this will likely fail iteration if some other thread mutates these
        # groups at the same time this iteration is ongoing...
        available = 0
        for workers in six.itervalues(self._active_groups):
            available += len(workers)
        self._available_workers = available

    def _on_member(self, group_id, member_id):
        try:
            result = self._coordinator.get_member_capabilities(group_id,
                                                               member_id)
            raw_capabilities = result.get()
        except coordination.ToozError:
            LOG.warn("Failed getting capabilities of '%s' in group '%s'",
                     member_id, group_id, exc_info=True)
            return None
        else:
            try:
                topic, tasks = pr.Capabilities.loads(raw_capabilities)
                worker = TopicWorker(topic, tasks, identity=member_id)
            except (TypeError, excp.InvalidFormat):
                LOG.warn("Unable to construct a topic worker using newly"
                         " discovered member '%s' (of group '%s') with"
                         " capabilities '%s'", member_id, group_id,
                         raw_capabilities, exc_info=True)
                return None
            else:
                self.notifier.notify(pr.WORKER_FOUND, worker.__dict__)
                siblings = self._active_groups[group_id]
                previous_idx = -1
                overwrite = True
                for i, w in enumerate(siblings):
                    if w.identity == worker.identity:
                        previous_idx = i
                        if w == worker:
                            overwrite = False
                        else:
                            overwrite = True
                        # Avoid further searching...
                        break
                new_or_updated = True
                if previous_idx == -1:
                    siblings.append(worker)
                else:
                    if overwrite:
                        siblings[previous_idx] = worker
                    else:
                        new_or_updated = False
                if new_or_updated:
                    return worker
                else:
                    return None

    def _on_group_deleted(self, group_id):
        try:
            self._active_groups.pop(group_id)
        except KeyError:
            return 0
        else:
            LOG.debug("Got notified that group '%s' was deleted", group_id)
            self._purge_watch_group(group_id)
            return 1

    def _purge_watch_group(self, group_id):
        try:
            self._coordinator.unwatch_join_group(group_id,
                                                 self._on_join_member_event)
        except (KeyError, ValueError):
            pass
        try:
            self._coordinator.unwatch_leave_group(group_id,
                                                  self._on_leave_member_event)
        except (KeyError, ValueError):
            pass

    def _on_join_member_event(self, event):
        with self._cond:
            worker = self._on_member(event.group_id, event.member_id)
            if worker is not None:
                LOG.debug("Got notified that '%s' now exists (or was"
                          " updated) in group '%s'", worker, event.group_id)
                self._recalculate_available_workers()
                self._cond.notify_all()

    def _on_leave_member(self, group_id, member_id):
        siblings = self._active_groups.get(group_id, [])
        previous_idx = -1
        for i, worker in enumerate(siblings):
            if member_id == worker.identity:
                previous_idx = i
                break
        if previous_idx != -1:
            worker = siblings.pop(previous_idx)
            self.notifier.notify(pr.WORKER_LOST, worker.__dict__)
            LOG.debug("Got notified that '%s' no longer exists"
                      " in group '%s'", worker, group_id)
            return worker
        return None

    def _on_leave_member_event(self, event):
        with self._cond:
            worker = self._on_leave_member(event.group_id, event.member_id)
            if worker is not None:
                self._recalculate_available_workers()
                self._cond.notify_all()

    def _establish_watch_group(self, group_id):
        expunge = False
        try:
            self._coordinator.watch_join_group(group_id,
                                               self._on_join_member_event)
            self._coordinator.watch_leave_group(group_id,
                                                self._on_leave_member_event)
        except coordination.GroupNotCreated:
            expunge = True
        except coordination.ToozError:
            expunge = True
            LOG.warn("Unexpected failure binding watchers to join/leaves"
                     " of group '%s'", group_id, exc_info=True)
        if expunge:
            self._purge_watch_group(group_id)

    def _on_group_exists(self, group_id):
        if group_id not in self._active_groups:
            LOG.debug("Got notified that group '%s' was created", group_id)
            self._active_groups[group_id] = []
            self._establish_watch_group(group_id)
        new_updated_workers = []
        workers_deleted = 0
        try:
            group_members = self._coordinator.get_members(group_id).get()
            gone_members = set()
            for worker in self._active_groups[group_id]:
                if worker.identity not in group_members:
                    gone_members.add(worker.identity)
            while gone_members:
                worker = self._on_leave_member(group_id, gone_members.pop())
                if worker is not None:
                    workers_deleted += 1
            for member_id in group_members:
                worker = self._on_member(group_id, member_id)
                if worker is not None:
                    new_updated_workers.append(worker)
                    LOG.debug("Got notified that '%s' now exists (or was"
                              " updated) in group '%s'", worker, group_id)
        except coordination.GroupNotCreated:
            pass
        except coordination.ToozError:
            LOG.warn("Unexpected failure getting members of group '%s'"
                     " from coordinator", group_id, exc_info=True)
        return (new_updated_workers, workers_deleted)

    @periodics.periodic(NOTICE_PERIOD, run_immediately=False)
    def notice(self):
        try:
            self._coordinator.run_watchers()
        except tooz.NotImplemented:
            pass
        except coordination.ToozError:
            LOG.warn("Unexpected failure running coordinator watchers",
                     exc_info=True)

    def discover(self):
        try:
            potential_groups = set(self._coordinator.get_groups().get())
        except coordination.ToozError:
            LOG.warn("Unexpected failure getting all groups from"
                     " coordinator", exc_info=True)
        else:
            with self._cond:
                deletes = 0
                for group_id in set(six.iterkeys(self._active_groups)):
                    if group_id not in potential_groups:
                        deletes += self._on_group_deleted(group_id)
                workers = []
                for group_id in potential_groups:
                    if group_id not in self._watch_groups:
                        continue
                    tmp_workers, tmp_deleted = self._on_group_exists(group_id)
                    workers.extend(tmp_workers)
                    deletes += tmp_deleted
                if deletes or workers:
                    self._recalculate_available_workers()
                    self._cond.notify_all()

    @periodics.periodic(BEAT_PERIOD, run_immediately=True)
    def beat(self):
        try:
            self._coordinator.heartbeat()
        except tooz.NotImplemented:
            pass
