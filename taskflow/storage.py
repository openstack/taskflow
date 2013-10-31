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

import contextlib
import logging
import six

from taskflow import exceptions
from taskflow.openstack.common import uuidutils
from taskflow.persistence import logbook
from taskflow import states
from taskflow.utils import misc
from taskflow.utils import threading_utils as tu

LOG = logging.getLogger(__name__)
STATES_WITH_RESULTS = (states.SUCCESS, states.REVERTING, states.FAILURE)


def _item_from_result(result, index, name):
    """Attempts to fetch a index/key from a given result."""
    if index is None:
        return result
    try:
        return result[index]
    except (IndexError, KeyError, ValueError, TypeError):
        # NOTE(harlowja): The result that the uuid returned can not be
        # accessed in the manner that the index is requesting. Perhaps
        # the result is a dictionary-like object and that key does
        # not exist (key error), or the result is a tuple/list and a
        # non-numeric key is being requested (index error), or there
        # was no result and an attempt to index into None is being
        # requested (type error).
        raise exceptions.NotFound("Unable to find result %r" % name)


class Storage(object):
    """Interface between engines and logbook

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

        # NOTE(imelnikov): failure serialization looses information,
        # so we cache failures here, in task name -> misc.Failure mapping
        self._failures = {}
        self._reload_failures()

        injector_td = self._flowdetail.find_by_name(self.injector_name)
        if injector_td is not None and injector_td.results is not None:
            names = six.iterkeys(injector_td.results)
            self.set_result_mapping(injector_td.uuid,
                                    dict((name, name) for name in names))

    def _with_connection(self, functor, *args, **kwargs):
        # NOTE(harlowja): Activate the given function with a backend
        # connection, if a backend is provided in the first place, otherwise
        # don't call the function.
        if self._backend is None:
            return
        with contextlib.closing(self._backend.get_connection()) as conn:
            functor(conn, *args, **kwargs)

    def add_task(self, uuid, task_name, task_version=None):
        """Add the task to storage

        Task becomes known to storage by that name and uuid.
        Task state is set to PENDING.
        """
        # TODO(imelnikov): check that task with same uuid or
        # task name does not exist
        td = logbook.TaskDetail(name=task_name, uuid=uuid)
        td.state = states.PENDING
        td.version = task_version
        self._flowdetail.add(td)

        def save_both(conn):
            """Saves the flow and the task detail with the same connection"""
            self._save_flow_detail(conn)
            self._save_task_detail(conn, task_detail=td)

        self._with_connection(save_both)

    @property
    def flow_name(self):
        return self._flowdetail.name

    @property
    def flow_uuid(self):
        return self._flowdetail.uuid

    def _save_flow_detail(self, conn):
        # NOTE(harlowja): we need to update our contained flow detail if
        # the result of the update actually added more (aka another process
        # added item to the flow detail).
        self._flowdetail.update(conn.update_flow_details(self._flowdetail))

    def get_uuid_by_name(self, task_name):
        """Get uuid of task with given name"""
        td = self._flowdetail.find_by_name(task_name)
        if td is not None:
            return td.uuid
        else:
            raise exceptions.NotFound("Unknown task name: %r" % task_name)

    def _taskdetail_by_uuid(self, uuid):
        td = self._flowdetail.find(uuid)
        if td is None:
            raise exceptions.NotFound("Unknown task: %r" % uuid)
        return td

    def _save_task_detail(self, conn, task_detail):
        # NOTE(harlowja): we need to update our contained task detail if
        # the result of the update actually added more (aka another process
        # is also modifying the task detail).
        task_detail.update(conn.update_task_details(task_detail))

    def set_task_state(self, uuid, state):
        """Set task state"""
        td = self._taskdetail_by_uuid(uuid)
        td.state = state
        self._with_connection(self._save_task_detail, task_detail=td)

    def get_task_state(self, uuid):
        """Get state of task with given uuid"""
        return self._taskdetail_by_uuid(uuid).state

    def set_task_progress(self, uuid, progress, details=None):
        """Set task progress.

        :param uuid: task uuid
        :param progress: task progress
        :param details: task specific progress information
        """
        td = self._taskdetail_by_uuid(uuid)
        if not td.meta:
            td.meta = {}
        td.meta['progress'] = progress
        if details is not None:
            # NOTE(imelnikov): as we can update progress without
            # updating details (e.g. automatically from engine)
            # we save progress value with details, too
            if details:
                td.meta['progress_details'] = dict(at_progress=progress,
                                                   details=details)
            else:
                td.meta['progress_details'] = None
        self._with_connection(self._save_task_detail, task_detail=td)

    def get_task_progress(self, uuid):
        """Get progress of task with given uuid.

        :param uuid: task uuid
        :returns: current task progress value
        """
        meta = self._taskdetail_by_uuid(uuid).meta
        if not meta:
            return 0.0
        return meta.get('progress', 0.0)

    def get_task_progress_details(self, uuid):
        """Get progress details of task with given uuid.

        :param uuid: task uuid
        :returns: None if progress_details not defined, else progress_details
                 dict
        """
        meta = self._taskdetail_by_uuid(uuid).meta
        if not meta:
            return None
        return meta.get('progress_details')

    def _check_all_results_provided(self, uuid, task_name, data):
        """Warn if task did not provide some of results

        This may happen if task returns shorter tuple or list or dict
        without all needed keys. It may also happen if task returns
        result of wrong type.
        """
        result_mapping = self._result_mappings.get(uuid, None)
        if result_mapping is None:
            return
        for name, index in six.iteritems(result_mapping):
            try:
                _item_from_result(data, index, name)
            except exceptions.NotFound:
                LOG.warning("Task %s did not supply result "
                            "with index %r (name %s)",
                            task_name, index, name)

    def save(self, uuid, data, state=states.SUCCESS):
        """Put result for task with id 'uuid' to storage"""
        td = self._taskdetail_by_uuid(uuid)
        td.state = state
        if state == states.FAILURE and isinstance(data, misc.Failure):
            td.results = None
            td.failure = data
            self._failures[td.name] = data
        else:
            td.results = data
            td.failure = None
            self._check_all_results_provided(uuid, td.name, data)
        self._with_connection(self._save_task_detail, task_detail=td)

    def _cache_failure(self, name, fail):
        """Ensure that cache has matching failure for task with this name.

        We leave cached version if it matches as it may contain more
        information. Returns cached failure.
        """
        cached = self._failures.get(name)
        if fail.matches(cached):
            return cached
        self._failures[name] = fail
        return fail

    def _reload_failures(self):
        """Refresh failures cache"""
        for td in self._flowdetail:
            if td.failure is not None:
                self._cache_failure(td.name, td.failure)

    def get(self, uuid):
        """Get result for task with id 'uuid' to storage"""
        td = self._taskdetail_by_uuid(uuid)
        if td.failure is not None:
            return self._cache_failure(td.name, td.failure)
        if td.state not in STATES_WITH_RESULTS:
            raise exceptions.NotFound("Result for task %r is not known" % uuid)
        return td.results

    def get_failures(self):
        """Get list of failures that happened with this flow.

        No order guaranteed.
        """
        return self._failures.copy()

    def has_failures(self):
        """Returns True if there are failed tasks in the storage"""
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

    def reset(self, uuid, state=states.PENDING):
        """Remove result for task with id 'uuid' from storage"""
        td = self._taskdetail_by_uuid(uuid)
        if self._reset_task(td, state):
            self._with_connection(self._save_task_detail, task_detail=td)

    def reset_tasks(self):
        """Reset all tasks to PENDING state, removing results.

        Returns list of (name, uuid) tuples for all tasks that were reset.
        """
        result = []

        def do_reset_all(connection):
            for td in self._flowdetail:
                if self._reset_task(td, states.PENDING):
                    self._save_task_detail(connection, td)
                    result.append((td.name, td.uuid))

        self._with_connection(do_reset_all)
        return result

    def inject(self, pairs):
        """Add values into storage

        This method should be used to put flow parameters (requirements that
        are not satisified by any task in the flow) into storage.
        """
        injector_td = self._flowdetail.find_by_name(self.injector_name)
        if injector_td is None:
            injector_uuid = uuidutils.generate_uuid()
            self.add_task(injector_uuid, self.injector_name)
            results = dict(pairs)
        else:
            injector_uuid = injector_td.uuid
            results = injector_td.results.copy()
            results.update(pairs)
        self.save(injector_uuid, results)
        names = six.iterkeys(results)
        self.set_result_mapping(injector_uuid,
                                dict((name, name) for name in names))

    def set_result_mapping(self, uuid, mapping):
        """Set mapping for naming task results

        The result saved with given uuid would be accessible by names
        defined in mapping. Mapping is a dict name => index. If index
        is None, the whole result will have this name; else, only
        part of it, result[index].
        """
        if not mapping:
            return
        self._result_mappings[uuid] = mapping
        for name, index in six.iteritems(mapping):
            entries = self._reverse_mapping.setdefault(name, [])

            # NOTE(imelnikov): We support setting same result mapping for
            # the same task twice (e.g when we are injecting 'a' and then
            # injecting 'a' again), so we should not log warning below in
            # that case and we should have only one item for each pair
            # (uuid, index) in entries. It should be put to the end of
            # entries list because order matters on fetching.
            try:
                entries.remove((uuid, index))
            except ValueError:
                pass

            entries.append((uuid, index))
            if len(entries) > 1:
                LOG.warning("Multiple provider mappings being created for %r",
                            name)

    def fetch(self, name):
        """Fetch named task result"""
        try:
            indexes = self._reverse_mapping[name]
        except KeyError:
            raise exceptions.NotFound("Name %r is not mapped" % name)
        # Return the first one that is found.
        for uuid, index in reversed(indexes):
            try:
                result = self.get(uuid)
                return _item_from_result(result, index, name)
            except exceptions.NotFound:
                pass
        raise exceptions.NotFound("Unable to find result %r" % name)

    def fetch_all(self):
        """Fetch all named task results known so far

        Should be used for debugging and testing purposes mostly.
        """
        result = {}
        for name in self._reverse_mapping:
            try:
                result[name] = self.fetch(name)
            except exceptions.NotFound:
                pass
        return result

    def fetch_mapped_args(self, args_mapping):
        """Fetch arguments for the task using arguments mapping"""
        return dict((key, self.fetch(name))
                    for key, name in six.iteritems(args_mapping))

    def set_flow_state(self, state):
        """Set flowdetails state and save it"""
        self._flowdetail.state = state
        self._with_connection(self._save_flow_detail)

    def get_flow_state(self):
        """Set state from flowdetails"""
        state = self._flowdetail.state
        if state is None:
            state = states.PENDING
        return state


class ThreadSafeStorage(six.with_metaclass(tu.ThreadSafeMeta, Storage)):
    pass
