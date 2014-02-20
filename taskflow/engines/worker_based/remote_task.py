# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import time

from concurrent import futures

from taskflow.engines.worker_based import protocol as pr
from taskflow.utils import misc
from taskflow.utils import persistence_utils as pu
from taskflow.utils import reflection


class RemoteTask(object):
    """Represents remote task with its request data and execution results.
    Every remote task is created in the PENDING state and will be expired
    within the given timeout.
    """

    def __init__(self, task, uuid, action, arguments, progress_callback,
                 timeout, **kwargs):
        self._task = task
        self._name = reflection.get_class_name(task)
        self._uuid = uuid
        self._action = action
        self._event = pr.ACTION_TO_EVENT[action]
        self._arguments = arguments
        self._progress_callback = progress_callback
        self._timeout = timeout
        self._kwargs = kwargs
        self._time = time.time()
        self._state = pr.PENDING
        self.result = futures.Future()

    def __repr__(self):
        return "%s:%s" % (self._name, self._action)

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name

    @property
    def request(self):
        """Return json-serializable task request, converting all `misc.Failure`
        objects into dictionaries.
        """
        request = dict(task=self._name, task_name=self._task.name,
                       task_version=self._task.version, action=self._action,
                       arguments=self._arguments)
        if 'result' in self._kwargs:
            result = self._kwargs['result']
            if isinstance(result, misc.Failure):
                request['result'] = ('failure', pu.failure_to_dict(result))
            else:
                request['result'] = ('success', result)
        if 'failures' in self._kwargs:
            failures = self._kwargs['failures']
            request['failures'] = {}
            for task, failure in failures.items():
                request['failures'][task] = pu.failure_to_dict(failure)
        return request

    @property
    def expired(self):
        """Check if task is expired.

        When new remote task is created its state is set to the PENDING,
        creation time is stored and timeout is given via constructor arguments.

        Remote task is considered to be expired when it is in the PENDING state
        for more then the given timeout (task is not considered to be expired
        in any other state). After remote task is expired - the `Timeout`
        exception is raised and task is removed from the remote tasks map.
        """
        if self._state == pr.PENDING:
            return time.time() - self._time > self._timeout
        return False

    def set_result(self, result):
        self.result.set_result((self._task, self._event, result))

    def set_running(self):
        self._state = pr.RUNNING

    def on_progress(self, event_data, progress):
        self._progress_callback(self._task, event_data, progress)
