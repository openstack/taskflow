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
import logging

import six

from taskflow.utils import misc
from taskflow.utils import reflection

LOG = logging.getLogger(__name__)


def _save_as_to_mapping(save_as):
    """Convert save_as to mapping name => index

    Result should follow storage convention for mappings.
    """
    # TODO(harlowja): we should probably document this behavior & convention
    # outside of code so that its more easily understandable, since what a task
    # returns is pretty crucial for other later operations.
    if save_as is None:
        return {}
    if isinstance(save_as, six.string_types):
        # NOTE(harlowja): this means that your task will only return one item
        # instead of a dictionary-like object or a indexable object (like a
        # list or tuple).
        return {save_as: None}
    elif isinstance(save_as, (tuple, list)):
        # NOTE(harlowja): this means that your task will return a indexable
        # object, like a list or tuple and the results can be mapped by index
        # to that tuple/list that is returned for others to use.
        return dict((key, num) for num, key in enumerate(save_as))
    elif isinstance(save_as, set):
        # NOTE(harlowja): in the case where a set is given we will not be
        # able to determine the numeric ordering in a reliable way (since it is
        # a unordered set) so the only way for us to easily map the result of
        # the task will be via the key itself.
        return dict((key, key) for key in save_as)
    raise TypeError('Task provides parameter '
                    'should be str, set or tuple/list, not %r' % save_as)


def _build_rebind_dict(args, rebind_args):
    """Build a argument remapping/rebinding dictionary.

    This dictionary allows a task to declare that it will take a needed
    requirement bound to a given name with another name instead (mapping the
    new name onto the required name).
    """
    if rebind_args is None:
        return {}
    elif isinstance(rebind_args, (list, tuple)):
        rebind = dict(zip(args, rebind_args))
        if len(args) < len(rebind_args):
            rebind.update((a, a) for a in rebind_args[len(args):])
        return rebind
    elif isinstance(rebind_args, dict):
        return rebind_args
    else:
        raise TypeError('Invalid rebind value: %s' % rebind_args)


def _build_arg_mapping(task_name, reqs, rebind_args, function, do_infer):
    """Given a function, its requirements and a rebind mapping this helper
    function will build the correct argument mapping for the given function as
    well as verify that the final argument mapping does not have missing or
    extra arguments (where applicable).
    """
    task_args = reflection.get_callable_args(function, required_only=True)
    result = {}
    if reqs:
        result.update((a, a) for a in reqs)
    if do_infer:
        result.update((a, a) for a in task_args)
    result.update(_build_rebind_dict(task_args, rebind_args))

    if not reflection.accepts_kwargs(function):
        all_args = reflection.get_callable_args(function, required_only=False)
        extra_args = set(result) - set(all_args)
        if extra_args:
            extra_args_str = ', '.join(sorted(extra_args))
            raise ValueError('Extra arguments given to task %s: %s'
                             % (task_name, extra_args_str))

    # NOTE(imelnikov): don't use set to preserve order in error message
    missing_args = [arg for arg in task_args if arg not in result]
    if missing_args:
        raise ValueError('Missing arguments for task %s: %s'
                         % (task_name, ' ,'.join(missing_args)))
    return result


class BaseTask(six.with_metaclass(abc.ABCMeta)):
    """An abstraction that defines a potential piece of work that can be
    applied and can be reverted to undo the work as a single unit.
    """
    TASK_EVENTS = ('update_progress', )

    def __init__(self, name, provides=None):
        self._name = name
        # An *immutable* input 'resource' name mapping this task depends
        # on existing before this task can be applied.
        #
        # Format is input_name:arg_name
        self.rebind = {}
        # An *immutable* output 'resource' name dict this task
        # produces that other tasks may depend on this task providing.
        #
        # Format is output index:arg_name
        self.save_as = _save_as_to_mapping(provides)
        # This identifies the version of the task to be ran which
        # can be useful in resuming older versions of tasks. Standard
        # major, minor version semantics apply.
        self.version = (1, 0)
        # Map of events => callback functions to invoke on task events.
        self._events_listeners = {}

    @property
    def name(self):
        return self._name

    def __str__(self):
        return "%s==%s" % (self.name, misc.get_version_string(self))

    @abc.abstractmethod
    def execute(self, *args, **kwargs):
        """Activate a given task which will perform some operation and return.

           This method can be used to apply some given context and given set
           of args and kwargs to accomplish some goal. Note that the result
           that is returned needs to be serializable so that it can be passed
           back into this task if reverting is triggered.
        """

    def revert(self, *args, **kwargs):
        """Revert this task using the given context, result that the apply
           provided as well as any information which may have caused
           said reversion.
        """

    @property
    def provides(self):
        return set(self.save_as)

    @property
    def requires(self):
        return set(self.rebind.values())

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
        if event in self._events_listeners:
            for handler in self._events_listeners[event]:
                event_data = self._events_listeners[event][handler]
                try:
                    handler(self, event_data, *args, **kwargs)
                except Exception:
                    LOG.exception("Failed calling `%s` on event '%s'",
                                  reflection.get_callable_name(handler), event)

    def bind(self, event, handler, **kwargs):
        """Attach a handler to an event for the task.

        :param event: event type
        :param handler: function to execute each time event is triggered
        :param kwargs: optional named parameters that will be passed to the
                       event handler
        :raises ValueError: if invalid event type passed
        """
        if event not in self.TASK_EVENTS:
            raise ValueError("Unknown task event %s" % event)
        if event not in self._events_listeners:
            self._events_listeners[event] = {}
        self._events_listeners[event][handler] = kwargs

    def unbind(self, event, handler=None):
        """Remove a previously-attached event handler from the task. If handler
        function not passed, then unbind all event handlers.

        :param event: event type
        :param handler: previously attached to event function
        """
        if event in self._events_listeners:
            if not handler:
                self._events_listeners[event] = {}
            else:
                if handler in self._events_listeners[event]:
                    self._events_listeners[event].pop(handler)


class Task(BaseTask):
    """Base class for user-defined tasks

    Adds following features to Task:
        - auto-generates name from type of self
        - adds all execute argument names to task requirements
        - items provided by the task may be specified via
          'default_provides' class attribute or property
    """

    default_provides = None

    def __init__(self, name=None, provides=None, requires=None,
                 auto_extract=True, rebind=None):
        """Initialize task instance"""
        if name is None:
            name = reflection.get_class_name(self)
        if provides is None:
            provides = self.default_provides
        super(Task, self).__init__(name,
                                   provides=provides)
        self.rebind = _build_arg_mapping(self.name, requires, rebind,
                                         self.execute, auto_extract)


class FunctorTask(BaseTask):
    """Adaptor to make task from a callable

    Take any callable and make a task from it.
    """

    def __init__(self, execute, name=None, provides=None,
                 requires=None, auto_extract=True, rebind=None, revert=None,
                 version=None):
        """Initialize FunctorTask instance with given callable and kwargs"""
        if name is None:
            name = reflection.get_callable_name(execute)
        super(FunctorTask, self).__init__(name, provides=provides)
        self._execute = execute
        self._revert = revert
        if version is not None:
            self.version = version
        self.rebind = _build_arg_mapping(self.name, requires, rebind,
                                         execute, auto_extract)

    def execute(self, *args, **kwargs):
        return self._execute(*args, **kwargs)

    def revert(self, *args, **kwargs):
        if self._revert:
            return self._revert(*args, **kwargs)
        else:
            return None
