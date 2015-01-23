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
import threading

from concurrent import futures
import jsonschema
from jsonschema import exceptions as schema_exc
from oslo_utils import reflection
from oslo_utils import timeutils
import six

from taskflow.engines.action_engine import executor
from taskflow import exceptions as excp
from taskflow import logging
from taskflow.types import failure as ft
from taskflow.types import timing as tt
from taskflow.utils import lock_utils

# NOTE(skudriashev): This is protocol states and events, which are not
# related to task states.
WAITING = 'WAITING'
PENDING = 'PENDING'
RUNNING = 'RUNNING'
SUCCESS = 'SUCCESS'
FAILURE = 'FAILURE'
EVENT = 'EVENT'

# During these states the expiry is active (once out of these states the expiry
# no longer matters, since we have no way of knowing how long a task will run
# for).
WAITING_STATES = (WAITING, PENDING)

_ALL_STATES = (WAITING, PENDING, RUNNING, SUCCESS, FAILURE, EVENT)
_STOP_TIMER_STATES = (RUNNING, SUCCESS, FAILURE)

# Transitions that a request state can go through.
_ALLOWED_TRANSITIONS = (
    # Used when a executor starts to publish a request to a selected worker.
    (WAITING, PENDING),
    # When a request expires (isn't able to be processed by any worker).
    (WAITING, FAILURE),
    # Worker has started executing a request.
    (PENDING, RUNNING),
    # Worker failed to construct/process a request to run (either the worker
    # did not transition to RUNNING in the given timeout or the worker itself
    # had some type of failure before RUNNING started).
    #
    # Also used by the executor if the request was attempted to be published
    # but that did publishing process did not work out.
    (PENDING, FAILURE),
    # Execution failed due to some type of remote failure.
    (RUNNING, FAILURE),
    # Execution succeeded & has completed.
    (RUNNING, SUCCESS),
)

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

# Special jsonschema validation types/adjustments.
_SCHEMA_TYPES = {
    # See: https://github.com/Julian/jsonschema/issues/148
    'array': (list, tuple),
}

LOG = logging.getLogger(__name__)


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

    #: String constant representing this message type.
    TYPE = NOTIFY

    # NOTE(harlowja): the executor (the entity who initially requests a worker
    # to send back a notification response) schema is different than the
    # worker response schema (that's why there are two schemas here).

    #: Expected notify *response* message schema (in json schema format).
    RESPONSE_SCHEMA = {
        "type": "object",
        'properties': {
            'topic': {
                "type": "string",
            },
            'tasks': {
                "type": "array",
                "items": {
                    "type": "string",
                },
            }
        },
        "required": ["topic", 'tasks'],
        "additionalProperties": False,
    }

    #: Expected *sender* request message schema (in json schema format).
    SENDER_SCHEMA = {
        "type": "object",
        "additionalProperties": False,
    }

    def __init__(self, **data):
        self._data = data

    def to_dict(self):
        return self._data

    @classmethod
    def validate(cls, data, response):
        if response:
            schema = cls.RESPONSE_SCHEMA
        else:
            schema = cls.SENDER_SCHEMA
        try:
            jsonschema.validate(data, schema, types=_SCHEMA_TYPES)
        except schema_exc.ValidationError as e:
            if response:
                raise excp.InvalidFormat("%s message response data not of the"
                                         " expected format: %s"
                                         % (cls.TYPE, e.message), e)
            else:
                raise excp.InvalidFormat("%s message sender data not of the"
                                         " expected format: %s"
                                         % (cls.TYPE, e.message), e)


class Request(Message):
    """Represents request with execution results.

    Every request is created in the WAITING state and is expired within the
    given timeout if it does not transition out of the (WAITING, PENDING)
    states.
    """

    #: String constant representing this message type.
    TYPE = REQUEST

    #: Expected message schema (in json schema format).
    SCHEMA = {
        "type": "object",
        'properties': {
            # These two are typically only sent on revert actions (that is
            # why are are not including them in the required section).
            'result': {},
            'failures': {
                "type": "object",
            },
            'task_cls': {
                'type': 'string',
            },
            'task_name': {
                'type': 'string',
            },
            'task_version': {
                "oneOf": [
                    {
                        "type": "string",
                    },
                    {
                        "type": "array",
                    },
                ],
            },
            'action': {
                "type": "string",
                "enum": list(six.iterkeys(ACTION_TO_EVENT)),
            },
            # Keyword arguments that end up in the revert() or execute()
            # method of the remote task.
            'arguments': {
                "type": "object",
            },
        },
        'required': ['task_cls', 'task_name', 'task_version', 'action'],
    }

    def __init__(self, task, uuid, action, arguments, timeout, **kwargs):
        self._task = task
        self._uuid = uuid
        self._action = action
        self._event = ACTION_TO_EVENT[action]
        self._arguments = arguments
        self._kwargs = kwargs
        self._watch = tt.StopWatch(duration=timeout).start()
        self._state = WAITING
        self._lock = threading.Lock()
        self._created_on = timeutils.utcnow()
        self._result = futures.Future()
        self._result.atom = task
        self._notifier = task.notifier

    @property
    def result(self):
        return self._result

    @property
    def notifier(self):
        return self._notifier

    @property
    def uuid(self):
        return self._uuid

    @property
    def task(self):
        return self._task

    @property
    def state(self):
        return self._state

    @property
    def created_on(self):
        return self._created_on

    @property
    def expired(self):
        """Check if request has expired.

        When new request is created its state is set to the WAITING, creation
        time is stored and timeout is given via constructor arguments.

        Request is considered to be expired when it is in the WAITING/PENDING
        state for more then the given timeout (it is not considered to be
        expired in any other state).
        """
        if self._state in WAITING_STATES:
            return self._watch.expired()
        return False

    def to_dict(self):
        """Return json-serializable request.

        To convert requests that have failed due to some exception this will
        convert all `failure.Failure` objects into dictionaries (which will
        then be reconstituted by the receiver).
        """
        request = {
            'task_cls': reflection.get_class_name(self._task),
            'task_name': self._task.name,
            'task_version': self._task.version,
            'action': self._action,
            'arguments': self._arguments,
        }
        if 'result' in self._kwargs:
            result = self._kwargs['result']
            if isinstance(result, ft.Failure):
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
        self.result.set_result((self._event, result))

    def transition_and_log_error(self, new_state, logger=None):
        """Transitions *and* logs an error if that transitioning raises.

        This overlays the transition function and performs nearly the same
        functionality but instead of raising if the transition was not valid
        it logs a warning to the provided logger and returns False to
        indicate that the transition was not performed (note that this
        is *different* from the transition function where False means
        ignored).
        """
        if logger is None:
            logger = LOG
        moved = False
        try:
            moved = self.transition(new_state)
        except excp.InvalidState:
            logger.warn("Failed to transition '%s' to %s state.", self,
                        new_state, exc_info=True)
        return moved

    @lock_utils.locked
    def transition(self, new_state):
        """Transitions the request to a new state.

        If transition was performed, it returns True. If transition
        should was ignored, it returns False. If transition was not
        valid (and will not be performed), it raises an InvalidState
        exception.
        """
        old_state = self._state
        if old_state == new_state:
            return False
        pair = (old_state, new_state)
        if pair not in _ALLOWED_TRANSITIONS:
            raise excp.InvalidState("Request transition from %s to %s is"
                                    " not allowed" % pair)
        if new_state in _STOP_TIMER_STATES:
            self._watch.stop()
        self._state = new_state
        LOG.debug("Transitioned '%s' from %s state to %s state", self,
                  old_state, new_state)
        return True

    @classmethod
    def validate(cls, data):
        try:
            jsonschema.validate(data, cls.SCHEMA, types=_SCHEMA_TYPES)
        except schema_exc.ValidationError as e:
            raise excp.InvalidFormat("%s message response data not of the"
                                     " expected format: %s"
                                     % (cls.TYPE, e.message), e)


class Response(Message):
    """Represents response message type."""

    #: String constant representing this message type.
    TYPE = RESPONSE

    #: Expected message schema (in json schema format).
    SCHEMA = {
        "type": "object",
        'properties': {
            'state': {
                "type": "string",
                "enum": list(_ALL_STATES),
            },
            'data': {
                "anyOf": [
                    {
                        "$ref": "#/definitions/event",
                    },
                    {
                        "$ref": "#/definitions/completion",
                    },
                    {
                        "$ref": "#/definitions/empty",
                    },
                ],
            },
        },
        "required": ["state", 'data'],
        "additionalProperties": False,
        "definitions": {
            "event": {
                "type": "object",
                "properties": {
                    'event_type': {
                        'type': 'string',
                    },
                    'details': {
                        'type': 'object',
                    },
                },
                "required": ["event_type", 'details'],
                "additionalProperties": False,
            },
            # Used when sending *only* request state changes (and no data is
            # expected).
            "empty": {
                "type": "object",
                "additionalProperties": False,
            },
            "completion": {
                "type": "object",
                "properties": {
                    # This can be any arbitrary type that a task returns, so
                    # thats why we can't be strict about what type it is since
                    # any of the json serializable types are allowed.
                    "result": {},
                },
                "required": ["result"],
                "additionalProperties": False,
            },
        },
    }

    def __init__(self, state, **data):
        self._state = state
        self._data = data

    @classmethod
    def from_dict(cls, data):
        state = data['state']
        data = data['data']
        if state == FAILURE and 'result' in data:
            data['result'] = ft.Failure.from_dict(data['result'])
        return cls(state, **data)

    @property
    def state(self):
        return self._state

    @property
    def data(self):
        return self._data

    def to_dict(self):
        return dict(state=self._state, data=self._data)

    @classmethod
    def validate(cls, data):
        try:
            jsonschema.validate(data, cls.SCHEMA, types=_SCHEMA_TYPES)
        except schema_exc.ValidationError as e:
            raise excp.InvalidFormat("%s message response data not of the"
                                     " expected format: %s"
                                     % (cls.TYPE, e.message), e)
