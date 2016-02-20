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
import collections
import threading

from automaton import exceptions as machine_excp
from automaton import machines
import fasteners
import futurist
from oslo_serialization import jsonutils
from oslo_utils import reflection
from oslo_utils import timeutils
import six

from taskflow.engines.action_engine import executor
from taskflow import exceptions as excp
from taskflow import logging
from taskflow.types import failure as ft
from taskflow.utils import schema_utils as su

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

# Once these states have been entered a request can no longer be
# automatically expired.
STOP_TIMER_STATES = (RUNNING, SUCCESS, FAILURE)

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

# When a worker hasn't notified in this many seconds, it will get expired from
# being used/targeted for further work.
EXPIRES_AFTER = 60

# Message types.
NOTIFY = 'NOTIFY'
REQUEST = 'REQUEST'
RESPONSE = 'RESPONSE'

# Object that denotes nothing (none can actually be valid).
NO_RESULT = object()

LOG = logging.getLogger(__name__)


def make_an_event(new_state):
    """Turns a new/target state into an event name."""
    return ('on_%s' % new_state).lower()


def build_a_machine(freeze=True):
    """Builds a state machine that requests are allowed to go through."""

    m = machines.FiniteMachine()
    for st in (WAITING, PENDING, RUNNING):
        m.add_state(st)
    for st in (SUCCESS, FAILURE):
        m.add_state(st, terminal=True)

    # When a executor starts to publish a request to a selected worker but the
    # executor has not recved confirmation from that worker that anything has
    # happened yet.
    m.default_start_state = WAITING
    m.add_transition(WAITING, PENDING, make_an_event(PENDING))

    # When a request expires (isn't able to be processed by any worker).
    m.add_transition(WAITING, FAILURE, make_an_event(FAILURE))

    # Worker has started executing a request.
    m.add_transition(PENDING, RUNNING, make_an_event(RUNNING))

    # Worker failed to construct/process a request to run (either the worker
    # did not transition to RUNNING in the given timeout or the worker itself
    # had some type of failure before RUNNING started).
    #
    # Also used by the executor if the request was attempted to be published
    # but that did publishing process did not work out.
    m.add_transition(PENDING, FAILURE, make_an_event(FAILURE))

    # Execution failed due to some type of remote failure.
    m.add_transition(RUNNING, FAILURE, make_an_event(FAILURE))

    # Execution succeeded & has completed.
    m.add_transition(RUNNING, SUCCESS, make_an_event(SUCCESS))

    # No further changes allowed.
    if freeze:
        m.freeze()
    return m


def failure_to_dict(failure):
    """Attempts to convert a failure object into a jsonifyable dictionary."""
    failure_dict = failure.to_dict()
    try:
        # it's possible the exc_args can't be serialized as JSON
        # if that's the case, just get the failure without them
        jsonutils.dumps(failure_dict)
        return failure_dict
    except (TypeError, ValueError):
        return failure.to_dict(include_args=False)


@six.add_metaclass(abc.ABCMeta)
class Message(object):
    """Base class for all message types."""

    def __repr__(self):
        return ("<%s object at 0x%x with contents %s>"
                % (reflection.get_class_name(self, fully_qualified=False),
                   id(self), self.to_dict()))

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

    @property
    def topic(self):
        return self._data.get('topic')

    @property
    def tasks(self):
        return self._data.get('tasks')

    def to_dict(self):
        return self._data

    @classmethod
    def validate(cls, data, response):
        if response:
            schema = cls.RESPONSE_SCHEMA
        else:
            schema = cls.SENDER_SCHEMA
        try:
            su.schema_validate(data, schema)
        except su.ValidationError as e:
            cls_name = reflection.get_class_name(cls, fully_qualified=False)
            if response:
                excp.raise_with_cause(excp.InvalidFormat,
                                      "%s message response data not of the"
                                      " expected format: %s" % (cls_name,
                                                                e.message),
                                      cause=e)
            else:
                excp.raise_with_cause(excp.InvalidFormat,
                                      "%s message sender data not of the"
                                      " expected format: %s" % (cls_name,
                                                                e.message),
                                      cause=e)


_WorkUnit = collections.namedtuple('_WorkUnit', ['task_cls', 'task_name',
                                                 'action', 'arguments'])


class Request(Message):
    """Represents request with execution results.

    Every request is created in the WAITING state and is expired within the
    given timeout if it does not transition out of the (WAITING, PENDING)
    states.

    State machine a request goes through as it progresses (or expires)::

        +------------+------------+---------+----------+---------+
        |   Start    |   Event    |   End   | On Enter | On Exit |
        +------------+------------+---------+----------+---------+
        | FAILURE[$] |     .      |    .    |    .     |    .    |
        |  PENDING   | on_failure | FAILURE |    .     |    .    |
        |  PENDING   | on_running | RUNNING |    .     |    .    |
        |  RUNNING   | on_failure | FAILURE |    .     |    .    |
        |  RUNNING   | on_success | SUCCESS |    .     |    .    |
        | SUCCESS[$] |     .      |    .    |    .     |    .    |
        | WAITING[^] | on_failure | FAILURE |    .     |    .    |
        | WAITING[^] | on_pending | PENDING |    .     |    .    |
        +------------+------------+---------+----------+---------+
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

    def __init__(self, task, uuid, action,
                 arguments, timeout=REQUEST_TIMEOUT, result=NO_RESULT,
                 failures=None):
        self._action = action
        self._event = ACTION_TO_EVENT[action]
        self._arguments = arguments
        self._result = result
        self._failures = failures
        self._watch = timeutils.StopWatch(duration=timeout).start()
        self._lock = threading.Lock()
        self._machine = build_a_machine()
        self._machine.initialize()
        self.task = task
        self.uuid = uuid
        self.created_on = timeutils.now()
        self.future = futurist.Future()
        self.future.atom = task

    @property
    def current_state(self):
        """Current state the request is in."""
        return self._machine.current_state

    def set_result(self, result):
        """Sets the responses futures result."""
        self.future.set_result((self._event, result))

    @property
    def expired(self):
        """Check if request has expired.

        When new request is created its state is set to the WAITING, creation
        time is stored and timeout is given via constructor arguments.

        Request is considered to be expired when it is in the WAITING/PENDING
        state for more then the given timeout (it is not considered to be
        expired in any other state).
        """
        if self._machine.current_state in WAITING_STATES:
            return self._watch.expired()
        return False

    def to_dict(self):
        """Return json-serializable request.

        To convert requests that have failed due to some exception this will
        convert all `failure.Failure` objects into dictionaries (which will
        then be reconstituted by the receiver).
        """
        request = {
            'task_cls': reflection.get_class_name(self.task),
            'task_name': self.task.name,
            'task_version': self.task.version,
            'action': self._action,
            'arguments': self._arguments,
        }
        if self._result is not NO_RESULT:
            result = self._result
            if isinstance(result, ft.Failure):
                request['result'] = ('failure', failure_to_dict(result))
            else:
                request['result'] = ('success', result)
        if self._failures:
            request['failures'] = {}
            for atom_name, failure in six.iteritems(self._failures):
                request['failures'][atom_name] = failure_to_dict(failure)
        return request

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

    @fasteners.locked
    def transition(self, new_state):
        """Transitions the request to a new state.

        If transition was performed, it returns True. If transition
        was ignored, it returns False. If transition was not
        valid (and will not be performed), it raises an InvalidState
        exception.
        """
        old_state = self._machine.current_state
        if old_state == new_state:
            return False
        try:
            self._machine.process_event(make_an_event(new_state))
        except (machine_excp.NotFound, machine_excp.InvalidState) as e:
            raise excp.InvalidState("Request transition from %s to %s is"
                                    " not allowed: %s" % (old_state,
                                                          new_state, e))
        else:
            if new_state in STOP_TIMER_STATES:
                self._watch.stop()
            LOG.debug("Transitioned '%s' from %s state to %s state", self,
                      old_state, new_state)
            return True

    @classmethod
    def validate(cls, data):
        try:
            su.schema_validate(data, cls.SCHEMA)
        except su.ValidationError as e:
            cls_name = reflection.get_class_name(cls, fully_qualified=False)
            excp.raise_with_cause(excp.InvalidFormat,
                                  "%s message response data not of the"
                                  " expected format: %s" % (cls_name,
                                                            e.message),
                                  cause=e)
        else:
            # Validate all failure dictionaries that *may* be present...
            failures = []
            if 'failures' in data:
                failures.extend(six.itervalues(data['failures']))
            result = data.get('result')
            if result is not None:
                result_data_type, result_data = result
                if result_data_type == 'failure':
                    failures.append(result_data)
            for fail_data in failures:
                ft.Failure.validate(fail_data)

    @staticmethod
    def from_dict(data, task_uuid=None):
        """Parses **validated** data into a work unit.

        All :py:class:`~taskflow.types.failure.Failure` objects that have been
        converted to dict(s) on the remote side will now converted back
        to py:class:`~taskflow.types.failure.Failure` objects.
        """
        task_cls = data['task_cls']
        task_name = data['task_name']
        action = data['action']
        arguments = data.get('arguments', {})
        result = data.get('result')
        failures = data.get('failures')
        # These arguments will eventually be given to the task executor
        # so they need to be in a format it will accept (and using keyword
        # argument names that it accepts)...
        arguments = {
            'arguments': arguments,
        }
        if task_uuid is not None:
            arguments['task_uuid'] = task_uuid
        if result is not None:
            result_data_type, result_data = result
            if result_data_type == 'failure':
                arguments['result'] = ft.Failure.from_dict(result_data)
            else:
                arguments['result'] = result_data
        if failures is not None:
            arguments['failures'] = {}
            for task, fail_data in six.iteritems(failures):
                arguments['failures'][task] = ft.Failure.from_dict(fail_data)
        return _WorkUnit(task_cls, task_name, action, arguments)


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
                "enum": list(build_a_machine().states) + [EVENT],
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
        self.state = state
        self.data = data

    @classmethod
    def from_dict(cls, data):
        state = data['state']
        data = data['data']
        if state == FAILURE and 'result' in data:
            data['result'] = ft.Failure.from_dict(data['result'])
        return cls(state, **data)

    def to_dict(self):
        return dict(state=self.state, data=self.data)

    @classmethod
    def validate(cls, data):
        try:
            su.schema_validate(data, cls.SCHEMA)
        except su.ValidationError as e:
            cls_name = reflection.get_class_name(cls, fully_qualified=False)
            excp.raise_with_cause(excp.InvalidFormat,
                                  "%s message response data not of the"
                                  " expected format: %s" % (cls_name,
                                                            e.message),
                                  cause=e)
        else:
            state = data['state']
            if state == FAILURE and 'result' in data:
                ft.Failure.validate(data['result'])
