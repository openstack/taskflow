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

import collections
import copy
import logging
import weakref

from taskflow.openstack.common import uuidutils
from taskflow import states
from taskflow import utils
from taskflow.utils import misc


LOG = logging.getLogger(__name__)


class FlowFailure(object):
    """When a task failure occurs the following object will be given to revert
       and can be used to interrogate what caused the failure.
    """

    def __init__(self, runner, flow):
        self.runner = runner
        self.flow = flow

    @property
    def exc_info(self):
        return self.runner.exc_info

    @property
    def exc(self):
        return self.runner.exc_info[1]


class Runner(object):
    """A helper class that wraps a task and can find the needed inputs for
    the task to run, as well as providing a uuid and other useful functionality
    for users of the task.
    """

    def __init__(self, task, uuid=None):
        task_factory = getattr(task, utils.TASK_FACTORY_ATTRIBUTE, None)
        if task_factory:
            self.task = task_factory(task)
        else:
            self.task = task
        self.providers = {}
        self.result = None
        if not uuid:
            self._id = uuidutils.generate_uuid()
        else:
            self._id = str(uuid)
        self.exc_info = (None, None, None)

    @property
    def uuid(self):
        return str(self._id)

    @property
    def requires(self):
        return self.task.requires

    @property
    def provides(self):
        return self.task.provides

    @property
    def optional(self):
        return self.task.optional

    @property
    def runs_before(self):
        return []

    @property
    def version(self):
        return misc.get_task_version(self.task)

    @property
    def name(self):
        if hasattr(self.task, 'name'):
            return self.task.name
        return '?'

    def reset(self):
        self.result = None
        self.exc_info = (None, None, None)

    def __str__(self):
        lines = ["Runner: %s" % (self.name)]
        lines.append("%s" % (self.uuid))
        lines.append("%s" % (self.version))
        return "; ".join(lines)

    def __call__(self, *args, **kwargs):
        # Find all of our inputs first.
        kwargs = dict(kwargs)
        for (k, who_made) in self.providers.iteritems():
            if k in kwargs:
                continue
            try:
                kwargs[k] = who_made.result[k]
            except (TypeError, KeyError):
                pass
        optional_keys = self.optional
        optional_keys = optional_keys - set(kwargs.keys())
        for k in optional_keys:
            for who_ran in self.runs_before:
                matched = False
                if k in who_ran.provides:
                    try:
                        kwargs[k] = who_ran.result[k]
                        matched = True
                    except (TypeError, KeyError):
                        pass
                if matched:
                    break
        # Ensure all required keys are either existent or set to none.
        for k in self.requires:
            if k not in kwargs:
                kwargs[k] = None
        # And now finally run.
        self.result = self.task.execute(*args, **kwargs)
        return self.result


class AOTRunner(Runner):
    """A runner that knows who runs before this runner ahead of time from a
    known list of previous runners.
    """

    def __init__(self, task):
        super(AOTRunner, self).__init__(task)
        self._runs_before = []

    @property
    def runs_before(self):
        return self._runs_before

    @runs_before.setter
    def runs_before(self, runs_before):
        self._runs_before = list(runs_before)


class TransitionNotifier(object):
    """A utility helper class that can be used to subscribe to
    notifications of events occuring as well as allow a entity to post said
    notifications to subscribers.
    """

    RESERVED_KEYS = ('details',)
    ANY = '*'

    def __init__(self):
        self._listeners = collections.defaultdict(list)

    def reset(self):
        self._listeners = collections.defaultdict(list)

    def notify(self, state, details):
        listeners = list(self._listeners.get(self.ANY, []))
        for i in self._listeners[state]:
            if i not in listeners:
                listeners.append(i)
        if not listeners:
            return
        for (callback, args, kwargs) in listeners:
            if args is None:
                args = []
            if kwargs is None:
                kwargs = {}
            kwargs['details'] = details
            try:
                callback(state, *args, **kwargs)
            except Exception:
                LOG.exception(("Failure calling callback %s to notify about"
                               " state transition %s"), callback, state)

    def register(self, state, callback, args=None, kwargs=None):
        assert isinstance(callback, collections.Callable)
        for i, (cb, args, kwargs) in enumerate(self._listeners.get(state, [])):
            if cb is callback:
                raise ValueError("Callback %s already registered" % (callback))
        if kwargs:
            for k in self.RESERVED_KEYS:
                if k in kwargs:
                    raise KeyError(("Reserved key '%s' not allowed in "
                                    "kwargs") % k)
            kwargs = copy.copy(kwargs)
        if args:
            args = copy.copy(args)
        self._listeners[state].append((callback, args, kwargs))

    def deregister(self, state, callback):
        if state not in self._listeners:
            return
        for i, (cb, args, kwargs) in enumerate(self._listeners[state]):
            if cb is callback:
                self._listeners[state].pop(i)
                break


class Rollback(object):
    """A helper functor object that on being called will call the underlying
    runners tasks revert method (if said method exists) and do the appropriate
    notification to signal to others that the reverting is underway.
    """

    def __init__(self, context, runner, flow, notifier):
        self.runner = runner
        self.context = context
        self.notifier = notifier
        # Use weak references to give the GC a break.
        self.flow = weakref.proxy(flow)

    def __str__(self):
        return "Rollback: %s" % (self.runner)

    def _fire_notify(self, has_reverted):
        if self.notifier:
            if has_reverted:
                state = states.REVERTED
            else:
                state = states.REVERTING
            self.notifier.notify(state, details={
                'context': self.context,
                'flow': self.flow,
                'runner': self.runner,
            })

    def __call__(self, cause):
        self._fire_notify(False)
        task = self.runner.task
        if ((hasattr(task, "revert") and
             isinstance(task.revert, collections.Callable))):
            task.revert(self.context, self.runner.result, cause)
        self._fire_notify(True)


class RollbackAccumulator(object):
    """A utility class that can help in organizing 'undo' like code
    so that said code be rolled back on failure (automatically or manually)
    by activating rollback callables that were inserted during said codes
    progression.
    """

    def __init__(self):
        self._rollbacks = []

    def add(self, *callables):
        self._rollbacks.extend(callables)

    def reset(self):
        self._rollbacks = []

    def __len__(self):
        return len(self._rollbacks)

    def __enter__(self):
        return self

    def rollback(self, cause):
        LOG.warn("Activating %s rollbacks due to %s.", len(self), cause)
        for (i, f) in enumerate(reversed(self._rollbacks)):
            LOG.debug("Calling rollback %s: %s", i + 1, f)
            try:
                f(cause)
            except Exception:
                LOG.exception(("Failed rolling back %s: %s due "
                               "to inner exception."), i + 1, f)

    def __exit__(self, type, value, tb):
        if any((value, type, tb)):
            self.rollback(value)
