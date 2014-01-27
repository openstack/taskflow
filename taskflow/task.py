# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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


@six.add_metaclass(abc.ABCMeta)
class BaseTask(atom.Atom):
    """An abstraction that defines a potential piece of work that can be
    applied and can be reverted to undo the work as a single task.
    """
    TASK_EVENTS = ('update_progress', )

    def __init__(self, name, provides=None):
        if name is None:
            name = reflection.get_class_name(self)
        super(BaseTask, self).__init__(name, provides)
        # Map of events => lists of callbacks to invoke on task events.
        self._events_listeners = collections.defaultdict(list)

    @abc.abstractmethod
    def execute(self, *args, **kwargs):
        """Activate a given task which will perform some operation and return.

        This method can be used to perform an action on a given set of input
        requirements (passed in via *args and **kwargs) to accomplish some type
        of operation. This operation may provide some named outputs/results as
        a result of it executing for later reverting (or for other tasks to
        depend on).

        NOTE(harlowja): the result (if any) that is returned should be
        persistable so that it can be passed back into this task if
        reverting is triggered (especially in the case where reverting
        happens in a different python process or on a remote machine) and so
        that the result can be transmitted to other tasks (which may be local
        or remote).
        """

    def revert(self, *args, **kwargs):
        """Revert this task using the result that the execute function
        provided as well as any failure information which caused the
        reversion to be triggered in the first place.

        NOTE(harlowja): The **kwargs which are passed into the execute()
        method will also be passed into this method. The **kwargs key 'result'
        will contain the execute() functions result (if any) and the **kwargs
        key 'flow_failures' will contain the failure information.
        """

    def update_progress(self, progress, **kwargs):
        """Update task progress and notify all registered listeners.

        :param progress: task progress float value between 0 and 1
        :param kwargs: task specific progress information
        """
        if progress > 1.0:
            LOG.warn("Progress must be <= 1.0, clamping to upper bound")
            progress = 1.0
        if progress < 0.0:
            LOG.warn("Progress must be >= 0.0, clamping to lower bound")
            progress = 0.0
        self._trigger('update_progress', progress, **kwargs)

    def _trigger(self, event, *args, **kwargs):
        """Execute all handlers for the given event type."""
        for (handler, event_data) in self._events_listeners.get(event, []):
            try:
                handler(self, event_data, *args, **kwargs)
            except Exception:
                LOG.exception("Failed calling `%s` on event '%s'",
                              reflection.get_callable_name(handler), event)

    @contextlib.contextmanager
    def autobind(self, event_name, handler_func, **kwargs):
        """Binds a given function to the task for a given event name and then
        unbinds that event name and associated function automatically on exit.
        """
        bound = False
        if handler_func is not None:
            try:
                self.bind(event_name, handler_func, **kwargs)
                bound = True
            except ValueError:
                LOG.exception("Failed binding functor `%s` as a receiver of"
                              " event '%s' notifications emitted from task %s",
                              handler_func, event_name, self)
        try:
            yield self
        finally:
            if bound:
                self.unbind(event_name, handler_func)

    def bind(self, event, handler, **kwargs):
        """Attach a handler to an event for the task.

        :param event: event type
        :param handler: callback to execute each time event is triggered
        :param kwargs: optional named parameters that will be passed to the
                       event handler
        :raises ValueError: if invalid event type passed
        """
        if event not in self.TASK_EVENTS:
            raise ValueError("Unknown task event '%s', can only bind"
                             " to events %s" % (event, self.TASK_EVENTS))
        assert six.callable(handler), "Handler must be callable"
        self._events_listeners[event].append((handler, kwargs))

    def unbind(self, event, handler=None):
        """Remove a previously-attached event handler from the task. If handler
        function not passed, then unbind all event handlers for the provided
        event. If multiple of the same handlers are bound, then the first
        match is removed (and only the first match).

        :param event: event type
        :param handler: handler previously bound

        :rtype: boolean
        :return: whether anything was removed
        """
        removed_any = False
        if not handler:
            removed_any = self._events_listeners.pop(event, removed_any)
        else:
            event_listeners = self._events_listeners.get(event, [])
            for i, (handler2, _event_data) in enumerate(event_listeners):
                if reflection.is_same_callback(handler, handler2):
                    event_listeners.pop(i)
                    removed_any = True
                    break
        return bool(removed_any)


class Task(BaseTask):
    """Base class for user-defined tasks.

    Adds following features to Task:
        - auto-generates name from type of self
        - adds all execute argument names to task requirements
        - items provided by the task may be specified via
          'default_provides' class attribute or property
    """

    default_provides = None

    def __init__(self, name=None, provides=None, requires=None,
                 auto_extract=True, rebind=None):
        """Initialize task instance."""
        if provides is None:
            provides = self.default_provides
        super(Task, self).__init__(name, provides=provides)
        self._build_arg_mapping(self.execute, requires, rebind, auto_extract)


class FunctorTask(BaseTask):
    """Adaptor to make a task from a callable.

    Take any callable and make a task from it.
    """

    def __init__(self, execute, name=None, provides=None,
                 requires=None, auto_extract=True, rebind=None, revert=None,
                 version=None):
        assert six.callable(execute), ("Function to use for executing must be"
                                       " callable")
        if revert:
            assert six.callable(revert), ("Function to use for reverting must"
                                          " be callable")
        if name is None:
            name = reflection.get_callable_name(execute)
        super(FunctorTask, self).__init__(name, provides=provides)
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
