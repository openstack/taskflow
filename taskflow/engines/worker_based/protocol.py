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

import six

from concurrent import futures

from taskflow.engines.action_engine import executor
from taskflow.utils import misc
from taskflow.utils import reflection

# NOTE(skudriashev): This is protocol events, not related to the task states.
WAITING = 'WAITING'
PENDING = 'PENDING'
RUNNING = 'RUNNING'
SUCCESS = 'SUCCESS'
FAILURE = 'FAILURE'
PROGRESS = 'PROGRESS'

# Remote task actions.
EXECUTE = 'execute'
REVERT = 'revert'

# Remote task action to event map.
ACTION_TO_EVENT = {
    EXECUTE: executor.EXECUTED,
    REVERT: executor.REVERTED
}

# NOTE(skudriashev): A timeout which specifies request expiration period.
REQUEST_TIMEOUT = 60

# NOTE(skudriashev): A timeout which controls for how long a queue can be
# unused before it is automatically deleted. Unused means the queue has no
# consumers, the queue has not been redeclared, the `queue.get` has not been
# invoked for a duration of at least the expiration period. In our case this
# period is equal to the request timeout, once request is expired - queue is
# no longer needed.
QUEUE_EXPIRE_TIMEOUT = REQUEST_TIMEOUT

# Workers notify period.
NOTIFY_PERIOD = 5

# Message types.
NOTIFY = 'NOTIFY'
REQUEST = 'REQUEST'
RESPONSE = 'RESPONSE'


@six.add_metaclass(abc.ABCMeta)
class Message(object):
    """Base class for all message types."""

    def __str__(self):
        return "<%s> %s" % (self.TYPE, self.to_dict())

    @abc.abstractmethod
    def to_dict(self):
        """Return json-serializable message representation."""


class Notify(Message):
    """Represents notify message type."""
    TYPE = NOTIFY

    def __init__(self, **data):
        self._data = data

    def to_dict(self):
        return self._data


class Request(Message):
    """Represents request with execution results. Every request is created in
    the WAITING state and is expired within the given timeout.
    """
    TYPE = REQUEST

    def __init__(self, task, uuid, action, arguments, progress_callback,
                 timeout, **kwargs):
        self._task = task
        self._task_cls = reflection.get_class_name(task)
        self._uuid = uuid
        self._action = action
        self._event = ACTION_TO_EVENT[action]
        self._arguments = arguments
        self._progress_callback = progress_callback
        self._kwargs = kwargs
        self._watch = misc.StopWatch(duration=timeout).start()
        self._state = WAITING
        self.result = futures.Future()

    def __repr__(self):
        return "%s:%s" % (self._task_cls, self._action)

    @property
    def uuid(self):
        return self._uuid

    @property
    def task_cls(self):
        return self._task_cls

    @property
    def state(self):
        return self._state

    @property
    def expired(self):
        """Check if request has expired.

        When new request is created its state is set to the WAITING, creation
        time is stored and timeout is given via constructor arguments.

        Request is considered to be expired when it is in the WAITING/PENDING
        state for more then the given timeout (it is not considered to be
        expired in any other state).
        """
        if self._state in (WAITING, PENDING):
            return self._watch.expired()
        return False

    def to_dict(self):
        """Return json-serializable request, converting all `misc.Failure`
        objects into dictionaries.
        """
        request = dict(task_cls=self._task_cls, task_name=self._task.name,
                       task_version=self._task.version, action=self._action,
                       arguments=self._arguments)
        if 'result' in self._kwargs:
            result = self._kwargs['result']
            if isinstance(result, misc.Failure):
                request['result'] = ('failure', result.to_dict())
            else:
                request['result'] = ('success', result)
        if 'failures' in self._kwargs:
            failures = self._kwargs['failures']
            request['failures'] = {}
            for task, failure in six.iteritems(failures):
                request['failures'][task] = failure.to_dict()
        return request

    def set_result(self, result):
        self.result.set_result((self._task, self._event, result))

    def set_pending(self):
        self._state = PENDING

    def set_running(self):
        self._state = RUNNING
        self._watch.stop()

    def on_progress(self, event_data, progress):
        self._progress_callback(self._task, event_data, progress)


class Response(Message):
    """Represents response message type."""
    TYPE = RESPONSE

    def __init__(self, state, **data):
        self._state = state
        self._data = data

    @classmethod
    def from_dict(cls, data):
        state = data['state']
        data = data['data']
        if state == FAILURE and 'result' in data:
            data['result'] = misc.Failure.from_dict(data['result'])
        return cls(state, **data)

    @property
    def state(self):
        return self._state

    @property
    def data(self):
        return self._data

    def to_dict(self):
        return dict(state=self._state, data=self._data)
