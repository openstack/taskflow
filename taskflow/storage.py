# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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
import contextlib
import logging

import six

from taskflow import exceptions
from taskflow.openstack.common import uuidutils
from taskflow.persistence import logbook
from taskflow import states
from taskflow.utils import lock_utils
from taskflow.utils import misc
from taskflow.utils import reflection

LOG = logging.getLogger(__name__)
STATES_WITH_RESULTS = (states.SUCCESS, states.REVERTING, states.FAILURE)


@six.add_metaclass(abc.ABCMeta)
class Storage(object):
    """Interface between engines and logbook.

    This class provides a simple interface to save tasks of a given flow and
    associated activity and results to persistence layer (logbook,
    task_details, flow_details) for use by engines, making it easier to
    interact with the underlying storage & backend mechanism.
    """

    injector_name = '_TaskFlow_INJECTOR'

    def __init__(self, flow_detail, backend=None):
        self._result_mappings = {}
        self._reverse_mapping = {}
        self._backend = backend
        self._flowdetail = flow_detail
        self._lock = self._lock_cls()

        # NOTE(imelnikov): failure serialization looses information,
        # so we cache failures here, in task name -> misc.Failure mapping.
        self._failures = {}
        for td in self._flowdetail:
            if td.failure is not None:
                self._failures[td.name] = td.failure

        self._task_name_to_uuid = dict((td.name, td.uuid)
                                       for td in self._flowdetail)

        try:
            injector_td = self._taskdetail_by_name(self.injector_name)
        except exceptions.NotFound:
            pass
        else:
            names = six.iterkeys(injector_td.results)
            self._set_result_mapping(injector_td.name,
                                     dict((name, name) for name in names))

    @abc.abstractproperty
    def _lock_cls(self):
        """Lock class used to generate reader/writer locks for protecting
        read/write access to the underlying storage backend and internally
        mutating operations.
        """

    def _with_connection(self, functor, *args, **kwargs):
        # NOTE(harlowja): Activate the given function with a backend
        # connection, if a backend is provided in the first place, otherwise
        # don't call the function.
        if self._backend is None:
            LOG.debug("No backend provided, not calling functor '%s'",
                      reflection.get_callable_name(functor))
            return
        with contextlib.closing(self._backend.get_connection()) as conn:
            functor(conn, *args, **kwargs)

    def ensure_task(self, task_name, task_version=None, result_mapping=None):
        """Ensure that there is taskdetail that correspond the task.

        If task does not exist, adds a record for it. Added task will have
        PENDING state. Sets result mapping for the task from result_mapping
        argument.

        Returns uuid for the task details corresponding to the task with
        given name.
        """
        with self._lock.write_lock():
            try:
                task_id = self._task_name_to_uuid[task_name]
            except KeyError:
                task_id = uuidutils.generate_uuid()
                self._add_task(task_id, task_name, task_version)
            self._set_result_mapping(task_name, result_mapping)
        return task_id

    def _add_task(self, uuid, task_name, task_version=None):
        """Add the task to storage.

        Task becomes known to storage by that name and uuid.
        Task state is set to PENDING.
        """

        def save_both(conn, td):
            """Saves the flow and the task detail with the same connection."""
            self._save_flow_detail(conn)
            self._save_task_detail(conn, td)

        # TODO(imelnikov): check that task with same uuid or
        # task name does not exist.
        td = logbook.TaskDetail(name=task_name, uuid=uuid)
        td.state = states.PENDING
        td.version = task_version
        self._flowdetail.add(td)
        self._with_connection(save_both, td)
        self._task_name_to_uuid[task_name] = uuid

    @property
    def flow_name(self):
        # This never changes (so no read locking needed).
        return self._flowdetail.name

    @property
    def flow_uuid(self):
        # This never changes (so no read locking needed).
        return self._flowdetail.uuid

    def _save_flow_detail(self, conn):
        # NOTE(harlowja): we need to update our contained flow detail if
        # the result of the update actually added more (aka another process
        # added item to the flow detail).
        self._flowdetail.update(conn.update_flow_details(self._flowdetail))

    def _taskdetail_by_name(self, task_name):
        try:
            return self._flowdetail.find(self._task_name_to_uuid[task_name])
        except KeyError:
            raise exceptions.NotFound("Unknown task name: %s" % task_name)

    def _save_task_detail(self, conn, task_detail):
        # NOTE(harlowja): we need to update our contained task detail if
        # the result of the update actually added more (aka another process
        # is also modifying the task detail).
        task_detail.update(conn.update_task_details(task_detail))

    def get_task_uuid(self, task_name):
        """Get task uuid by given name."""
        with self._lock.read_lock():
            td = self._taskdetail_by_name(task_name)
            return td.uuid

    def set_task_state(self, task_name, state):
        """Set task state."""
        with self._lock.write_lock():
            td = self._taskdetail_by_name(task_name)
            td.state = state
            self._with_connection(self._save_task_detail, td)

    def get_task_state(self, task_name):
        """Get state of task with given name."""
        with self._lock.read_lock():
            td = self._taskdetail_by_name(task_name)
            return td.state

    def get_tasks_states(self, task_names):
        """Gets all task states."""
        with self._lock.read_lock():
            return dict((name, self.get_task_state(name))
                        for name in task_names)

    def update_task_metadata(self, task_name, update_with):
        """Updates a tasks metadata."""
        if not update_with:
            return
        with self._lock.write_lock():
            td = self._taskdetail_by_name(task_name)
            if not td.meta:
                td.meta = {}
            td.meta.update(update_with)
            self._with_connection(self._save_task_detail, td)

    def set_task_progress(self, task_name, progress, details=None):
        """Set task progress.

        :param task_name: task name
        :param progress: task progress
        :param details: task specific progress information
        """
        metadata_update = {
            'progress': progress,
        }
        if details is not None:
            # NOTE(imelnikov): as we can update progress without
            # updating details (e.g. automatically from engine)
            # we save progress value with details, too.
            if details:
                metadata_update['progress_details'] = {
                    'at_progress': progress,
                    'details': details,
                }
            else:
                metadata_update['progress_details'] = None
        self.update_task_metadata(task_name, metadata_update)

    def get_task_progress(self, task_name):
        """Get progress of task with given name.

        :param task_name: task name
        :returns: current task progress value
        """
        with self._lock.read_lock():
            td = self._taskdetail_by_name(task_name)
            if not td.meta:
                return 0.0
            return td.meta.get('progress', 0.0)

    def get_task_progress_details(self, task_name):
        """Get progress details of task with given name.

        :param task_name: task name
        :returns: None if progress_details not defined, else progress_details
                 dict
        """
        with self._lock.read_lock():
            td = self._taskdetail_by_name(task_name)
            if not td.meta:
                return None
            return td.meta.get('progress_details')

    def _check_all_results_provided(self, task_name, data):
        """Warn if task did not provide some of results.

        This may happen if task returns shorter tuple or list or dict
        without all needed keys. It may also happen if task returns
        result of wrong type.
        """
        result_mapping = self._result_mappings.get(task_name)
        if not result_mapping:
            return
        for name, index in six.iteritems(result_mapping):
            try:
                misc.item_from(data, index, name=name)
            except exceptions.NotFound:
                LOG.warning("Task %s did not supply result "
                            "with index %r (name %s)", task_name, index, name)

    def save(self, task_name, data, state=states.SUCCESS):
        """Put result for task with id 'uuid' to storage."""
        with self._lock.write_lock():
            td = self._taskdetail_by_name(task_name)
            td.state = state
            if state == states.FAILURE and isinstance(data, misc.Failure):
                td.results = None
                td.failure = data
                self._failures[td.name] = data
            else:
                td.results = data
                td.failure = None
                self._check_all_results_provided(td.name, data)
            self._with_connection(self._save_task_detail, td)

    def get(self, task_name):
        """Get result for task with name 'task_name' to storage."""
        with self._lock.read_lock():
            td = self._taskdetail_by_name(task_name)
            if td.failure is not None:
                cached = self._failures.get(task_name)
                if td.failure.matches(cached):
                    return cached
                return td.failure
            if td.state not in STATES_WITH_RESULTS:
                raise exceptions.NotFound("Result for task %s is not known"
                                          % task_name)
            return td.results

    def get_failures(self):
        """Get list of failures that happened with this flow.

        No order guaranteed.
        """
        with self._lock.read_lock():
            return self._failures.copy()

    def has_failures(self):
        """Returns True if there are failed tasks in the storage."""
        with self._lock.read_lock():
            return bool(self._failures)

    def _reset_task(self, td, state):
        if td.name == self.injector_name:
            return False
        if td.state == state:
            return False
        td.results = None
        td.failure = None
        td.state = state
        self._failures.pop(td.name, None)
        return True

    def reset(self, task_name, state=states.PENDING):
        """Remove result for task with id 'uuid' from storage."""
        with self._lock.write_lock():
            td = self._taskdetail_by_name(task_name)
            if self._reset_task(td, state):
                self._with_connection(self._save_task_detail, td)

    def reset_tasks(self):
        """Reset all tasks to PENDING state, removing results.

        Returns list of (name, uuid) tuples for all tasks that were reset.
        """
        reset_results = []

        def do_reset_all(connection):
            for td in self._flowdetail:
                if self._reset_task(td, states.PENDING):
                    self._save_task_detail(connection, td)
                    reset_results.append((td.name, td.uuid))

        with self._lock.write_lock():
            self._with_connection(do_reset_all)

        return reset_results

    def inject(self, pairs):
        """Add values into storage.

        This method should be used to put flow parameters (requirements that
        are not satisfied by any task in the flow) into storage.
        """
        with self._lock.write_lock():
            try:
                td = self._taskdetail_by_name(self.injector_name)
            except exceptions.NotFound:
                self._add_task(uuidutils.generate_uuid(), self.injector_name)
                td = self._taskdetail_by_name(self.injector_name)
                td.results = dict(pairs)
                td.state = states.SUCCESS
            else:
                td.results.update(pairs)
            self._with_connection(self._save_task_detail, td)
            names = six.iterkeys(td.results)
            self._set_result_mapping(self.injector_name,
                                     dict((name, name) for name in names))

    def _set_result_mapping(self, task_name, mapping):
        """Set mapping for naming task results.

        The result saved with given name would be accessible by names
        defined in mapping. Mapping is a dict name => index. If index
        is None, the whole result will have this name; else, only
        part of it, result[index].
        """
        if not mapping:
            return
        self._result_mappings[task_name] = mapping
        for name, index in six.iteritems(mapping):
            entries = self._reverse_mapping.setdefault(name, [])

            # NOTE(imelnikov): We support setting same result mapping for
            # the same task twice (e.g when we are injecting 'a' and then
            # injecting 'a' again), so we should not log warning below in
            # that case and we should have only one item for each pair
            # (task_name, index) in entries. It should be put to the end of
            # entries list because order matters on fetching.
            try:
                entries.remove((task_name, index))
            except ValueError:
                pass

            entries.append((task_name, index))
            if len(entries) > 1:
                LOG.warning("Multiple provider mappings being created for %r",
                            name)

    def fetch(self, name):
        """Fetch named task result."""
        with self._lock.read_lock():
            try:
                indexes = self._reverse_mapping[name]
            except KeyError:
                raise exceptions.NotFound("Name %r is not mapped" % name)
            # Return the first one that is found.
            for (task_name, index) in reversed(indexes):
                try:
                    result = self.get(task_name)
                    return misc.item_from(result, index, name=name)
                except exceptions.NotFound:
                    pass
            raise exceptions.NotFound("Unable to find result %r" % name)

    def fetch_all(self):
        """Fetch all named task results known so far.

        Should be used for debugging and testing purposes mostly.
        """
        with self._lock.read_lock():
            results = {}
            for name in self._reverse_mapping:
                try:
                    results[name] = self.fetch(name)
                except exceptions.NotFound:
                    pass
            return results

    def fetch_mapped_args(self, args_mapping):
        """Fetch arguments for the task using arguments mapping."""
        with self._lock.read_lock():
            return dict((key, self.fetch(name))
                        for key, name in six.iteritems(args_mapping))

    def set_flow_state(self, state):
        """Set flow details state and save it."""
        with self._lock.write_lock():
            self._flowdetail.state = state
            self._with_connection(self._save_flow_detail)

    def get_flow_state(self):
        """Get state from flow details."""
        with self._lock.read_lock():
            state = self._flowdetail.state
            if state is None:
                state = states.PENDING
            return state


class MultiThreadedStorage(Storage):
    """Storage that uses locks to protect against concurrent access."""
    _lock_cls = lock_utils.ReaderWriterLock


class SingleThreadedStorage(Storage):
    """Storage that uses dummy locks when you really don't need locks."""
    _lock_cls = lock_utils.DummyReaderWriterLock
