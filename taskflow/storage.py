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

from taskflow import exceptions
from taskflow.openstack.common import uuidutils
from taskflow.persistence import flowdetail
from taskflow.persistence import logbook
from taskflow.persistence import taskdetail
from taskflow import states
from taskflow.utils import threading_utils


def temporary_flow_detail():
    """Creates flow detail class for temporary usage

    Creates in-memory logbook and flow detail in it. Should
    be useful for tests and other use cases where persistence
    is not needed
    """
    lb = logbook.LogBook('tmp', backend='memory')
    fd = flowdetail.FlowDetail(
        name='tmp', uuid=uuidutils.generate_uuid(),
        backend='memory')
    lb.add(fd)
    lb.save()
    fd.save()
    return fd


STATES_WITH_RESULTS = (states.SUCCESS, states.REVERTING, states.FAILURE)


class Storage(object):
    """Interface between engines and logbook

    This class provides simple interface to save task details and
    results to persistence layer for use by engines.
    """

    injector_name = '_TaskFlow_INJECTOR'

    def __init__(self, flow_detail=None):
        self._result_mappings = {}
        self._reverse_mapping = {}

        if flow_detail is None:
            # TODO(imelnikov): this is useful mainly for tests;
            #  maybe we should make flow_detail required parameter?
            self._flowdetail = temporary_flow_detail()
        else:
            self._flowdetail = flow_detail

    def add_task(self, uuid, task_name):
        """Add the task to storage

        Task becomes known to storage by that name and uuid.
        Task state is set to PENDING.
        """
        # TODO(imelnikov): check that task with same uuid or
        #   task name does not exist
        td = taskdetail.TaskDetail(name=task_name, uuid=uuid)
        td.state = states.PENDING
        self._flowdetail.add(td)
        self._flowdetail.save()
        td.save()

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

    def set_task_state(self, uuid, state):
        """Set task state"""
        td = self._taskdetail_by_uuid(uuid)
        td.state = state
        td.save()

    def get_task_state(self, uuid):
        """Get state of task with given uuid"""
        return self._taskdetail_by_uuid(uuid).state

    def save(self, uuid, data, state=states.SUCCESS):
        """Put result for task with id 'uuid' to storage"""
        td = self._taskdetail_by_uuid(uuid)
        td.state = state
        td.results = data
        td.save()

    def get(self, uuid):
        """Get result for task with id 'uuid' to storage"""
        td = self._taskdetail_by_uuid(uuid)
        if td.state not in STATES_WITH_RESULTS:
            raise exceptions.NotFound("Result for task %r is not known" % uuid)
        return td.results

    def reset(self, uuid, state=states.PENDING):
        """Remove result for task with id 'uuid' from storage"""
        td = self._taskdetail_by_uuid(uuid)
        td.results = None
        td.state = state
        td.save()

    def inject(self, pairs):
        """Add values into storage

        This method should be used by job in order to put flow parameters
        into storage and put it to action.
        """
        pairs = dict(pairs)
        injector_uuid = uuidutils.generate_uuid()
        self.add_task(injector_uuid, self.injector_name)
        self.save(injector_uuid, pairs)
        for name in pairs.iterkeys():
            entries = self._reverse_mapping.setdefault(name, [])
            entries.append((injector_uuid, name))

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
        for name, index in mapping.iteritems():
            entries = self._reverse_mapping.setdefault(name, [])
            entries.append((uuid, index))

    def fetch(self, name):
        """Fetch named task result"""
        try:
            indexes = self._reverse_mapping[name]
        except KeyError:
            raise exceptions.NotFound("Name %r is not mapped" % name)
        # Return the first one that is found.
        for uuid, index in indexes:
            try:
                result = self.get(uuid)
                if index is None:
                    return result
                else:
                    return result[index]
            except exceptions.NotFound:
                # NOTE(harlowja): No result was found for the given uuid.
                pass
            except (KeyError, IndexError, TypeError):
                # NOTE(harlowja): The result that the uuid returned can not be
                # accessed in the manner that the index is requesting. Perhaps
                # the result is a dictionary-like object and that key does
                # not exist (key error), or the result is a tuple/list and a
                # non-numeric key is being requested (index error), or there
                # was no result and an attempt to index into None is being
                # requested (type error).
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
                    for key, name in args_mapping.iteritems())

    def set_flow_state(self, state):
        """Set flowdetails state and save it"""
        self._flowdetail.state = state
        self._flowdetail.save()

    def get_flow_state(self):
        """Set state from flowdetails"""
        return self._flowdetail.state


class ThreadSafeStorage(Storage):
    __metaclass__ = threading_utils.ThreadSafeMeta
