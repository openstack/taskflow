# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
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
import collections
import contextlib
import logging

import six

from taskflow import atom
from taskflow.utils import reflection

LOG = logging.getLogger(__name__)

# Constants passed into revert kwargs.
#
# Contain the execute() result (if any).
REVERT_RESULT = 'result'
#
# The cause of the flow failure/s
REVERT_FLOW_FAILURES = 'flow_failures'

# Common events
EVENT_UPDATE_PROGRESS = 'update_progress'


@six.add_metaclass(abc.ABCMeta)
class BaseTask(atom.Atom):
    """An abstraction that defines a potential piece of work.

    This potential piece of work is expected to be able to contain
    functionality that defines what can be executed to accomplish that work
    as well as a way of defining what can be executed to reverted/undo that
    same piece of work.
    """

    # Known events this task can have callbacks bound to (others that are not
    # in this set/tuple will not be able to be bound); this should be updated
    # and/or extended in subclasses as needed to enable or disable new or
    # existing events...
    TASK_EVENTS = (EVENT_UPDATE_PROGRESS,)

    def __init__(self, name, provides=None, inject=None):
        if name is None:
            name = reflection.get_class_name(self)
        super(BaseTask, self).__init__(name, provides, inject=inject)
        # Map of events => lists of callbacks to invoke on task events.
        self._events_listeners = collections.defaultdict(list)

    def pre_execute(self):
        """Code to be run prior to executing the task.

        A common pattern for initializing the state of the system prior to
        running tasks is to define some code in a base class that all your
        tasks inherit from.  In that class, you can define a ``pre_execute``
        method and it will always be invoked just prior to your tasks running.
        """

    @abc.abstractmethod
    def execute(self, *args, **kwargs):
        """Activate a given task which will perform some operation and return.

        This method can be used to perform an action on a given set of input
        requirements (passed in via ``*args`` and ``**kwargs``) to accomplish
        some type of operation. This operation may provide some named
        outputs/results as a result of it executing for later reverting (or for
        other tasks to depend on).

        NOTE(harlowja): the result (if any) that is returned should be
        persistable so that it can be passed back into this task if
        reverting is triggered (especially in the case where reverting
        happens in a different python process or on a remote machine) and so
        that the result can be transmitted to other tasks (which may be local
        or remote).

        :param args: positional arguments that task requires to execute.
        :param kwargs: any keyword arguments that task requires to execute.
        """

    def post_execute(self):
        """Code to be run after executing the task.

        A common pattern for cleaning up global state of the system after the
        execution of tasks is to define some code in a base class that all your
        tasks inherit from.  In that class, you can define a ``post_execute``
        method and it will always be invoked just after your tasks execute,
        regardless of whether they succeded or not.

        This pattern is useful if you have global shared database sessions
        that need to be cleaned up, for example.
        """

    def pre_revert(self):
        """Code to be run prior to reverting the task.

        This works the same as :meth:`.pre_execute`, but for the revert phase.
        """

    def revert(self, *args, **kwargs):
        """Revert this task.

        This method should undo any side-effects caused by previous execution
        of the task using the result of the :py:meth:`execute` method and
        information on the failure which triggered reversion of the flow the
        task is contained in (if applicable).

        :param args: positional arguments that the task required to execute.
        :param kwargs: any keyword arguments that the task required to
                       execute; the special key ``'result'`` will contain
                       the :py:meth:`execute` result (if any) and
                       the ``**kwargs`` key ``'flow_failures'`` will contain
                       any failure information.
        """

    def post_revert(self):
        """Code to be run after reverting the task.

        This works the same as :meth:`.post_execute`, but for the revert phase.
        """

    def update_progress(self, progress, **kwargs):
        """Update task progress and notify all registered listeners.

        :param progress: task progress float value between 0.0 and 1.0
        :param kwargs: any keyword arguments that are tied to the specific
                       progress value.
        """
        if progress > 1.0:
            LOG.warn("Progress must be <= 1.0, clamping to upper bound")
            progress = 1.0
        if progress < 0.0:
            LOG.warn("Progress must be >= 0.0, clamping to lower bound")
            progress = 0.0
        self.trigger(EVENT_UPDATE_PROGRESS, progress, **kwargs)

    def trigger(self, event_name, *args, **kwargs):
        """Execute all callbacks registered for the given event type.

        NOTE(harlowja): if a bound callback raises an exception it will be
                        logged (at a ``WARNING`` level) and the exception
                        will be dropped.

        :param event_name: event name to trigger
        :param args: arbitrary positional arguments passed to the triggered
                     callbacks (if any are matched), these will be in addition
                     to any ``kwargs`` provided on binding (these are passed
                     as positional arguments to the callback).
        :param kwargs: arbitrary keyword arguments passed to the triggered
                     callbacks (if any are matched), these will be in addition
                     to any ``kwargs`` provided on binding (these are passed
                     as keyword arguments to the callback).
        """
        for (cb, event_data) in self._events_listeners.get(event_name, []):
            try:
                cb(self, event_data, *args, **kwargs)
            except Exception:
                LOG.warn("Failed calling callback `%s` on event '%s'",
                         reflection.get_callable_name(cb), event_name,
                         exc_info=True)

    @contextlib.contextmanager
    def autobind(self, event_name, callback, **kwargs):
        """Binds & unbinds a given callback to the task.

        This function binds and unbinds using the context manager protocol.
        When events are triggered on the task of the given event name this
        callback will automatically be called with the provided
        keyword arguments as the first argument (further arguments may be
        provided by the entity triggering the event).

        The arguments are interpreted as for :func:`bind() <bind>`.
        """
        bound = False
        if callback is not None:
            try:
                self.bind(event_name, callback, **kwargs)
                bound = True
            except ValueError:
                LOG.warn("Failed binding callback `%s` as a receiver of"
                         " event '%s' notifications emitted from task '%s'",
                         reflection.get_callable_name(callback), event_name,
                         self, exc_info=True)
        try:
            yield self
        finally:
            if bound:
                self.unbind(event_name, callback)

    def bind(self, event_name, callback, **kwargs):
        """Attach a callback to be triggered on a task event.

        Callbacks should *not* be bound, modified, or removed after execution
        has commenced (they may be adjusted after execution has finished). This
        is primarily due to the need to preserve the callbacks that exist at
        execution time for engines which run tasks remotely or out of
        process (so that those engines can correctly proxy back transmitted
        events).

        Callbacks should also be *quick* to execute so that the engine calling
        them can continue execution in a timely manner (if long running
        callbacks need to exist, consider creating a separate pool + queue
        for those that the attached callbacks put long running operations into
        for execution by other entities).

        :param event_name: event type name
        :param callback: callable to execute each time event is triggered
        :param kwargs: optional named parameters that will be passed to the
                       callable object as a dictionary to the callbacks
                       *second* positional parameter.
        :raises ValueError: if invalid event type, or callback is passed
        """
        if event_name not in self.TASK_EVENTS:
            raise ValueError("Unknown task event '%s', can only bind"
                             " to events %s" % (event_name, self.TASK_EVENTS))
        if callback is not None:
            if not six.callable(callback):
                raise ValueError("Event handler callback must be callable")
            self._events_listeners[event_name].append((callback, kwargs))

    def unbind(self, event_name, callback=None):
        """Remove a previously-attached event callback from the task.

        If a callback is not passed, then this will unbind *all* event
        callbacks for the provided event. If multiple of the same callbacks
        are bound, then the first match is removed (and only the first match).

        :param event_name: event type
        :param callback: callback previously bound

        :rtype: boolean
        :return: whether anything was removed
        """
        removed_any = False
        if not callback:
            removed_any = self._events_listeners.pop(event_name, removed_any)
        else:
            event_listeners = self._events_listeners.get(event_name, [])
            for i, (cb, _event_data) in enumerate(event_listeners):
                if reflection.is_same_callback(cb, callback):
                    # NOTE(harlowja): its safe to do this as long as we stop
                    # iterating after we do the removal, otherwise its not
                    # safe (since this could have resized the list).
                    event_listeners.pop(i)
                    removed_any = True
                    break
        return bool(removed_any)

    def listeners_iter(self):
        """Return an iterator over the mapping of event => callbacks bound."""
        for event_name in list(six.iterkeys(self._events_listeners)):
            # Use get() just incase it was removed while iterating...
            event_listeners = self._events_listeners.get(event_name, [])
            if event_listeners:
                yield (event_name, event_listeners[:])


class Task(BaseTask):
    """Base class for user-defined tasks (derive from it at will!).

    Adds the following features on top of the :py:class:`.BaseTask`:

    - Auto-generates a name from the class name if a name is not
      explicitly provided.
    - Automatically adds all :py:meth:`.BaseTask.execute` argument names to
      the task requirements (items provided by the task may be also specified
      via ``default_provides`` class attribute or instance property).
    """

    default_provides = None

    def __init__(self, name=None, provides=None, requires=None,
                 auto_extract=True, rebind=None, inject=None):
        if provides is None:
            provides = self.default_provides
        super(Task, self).__init__(name, provides=provides, inject=inject)
        self._build_arg_mapping(self.execute, requires, rebind, auto_extract)


class FunctorTask(BaseTask):
    """Adaptor to make a task from a callable.

    Take any callable pair and make a task from it.

    NOTE(harlowja): If a name is not provided the function/method name of
    the ``execute`` callable will be used as the name instead (the name of
    the ``revert`` callable is not used).
    """

    def __init__(self, execute, name=None, provides=None,
                 requires=None, auto_extract=True, rebind=None, revert=None,
                 version=None, inject=None):
        if not six.callable(execute):
            raise ValueError("Function to use for executing must be"
                             " callable")
        if revert is not None:
            if not six.callable(revert):
                raise ValueError("Function to use for reverting must"
                                 " be callable")
        if name is None:
            name = reflection.get_callable_name(execute)
        super(FunctorTask, self).__init__(name, provides=provides,
                                          inject=inject)
        self._execute = execute
        self._revert = revert
        if version is not None:
            self.version = version
        self._build_arg_mapping(execute, requires, rebind, auto_extract)

    def execute(self, *args, **kwargs):
        return self._execute(*args, **kwargs)

    def revert(self, *args, **kwargs):
        if self._revert:
            return self._revert(*args, **kwargs)
        else:
            return None
