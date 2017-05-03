# -*- coding: utf-8 -*-

#    Copyright 2015 Hewlett-Packard Development Company, L.P.
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
import copy

from oslo_utils import reflection
import six
from six.moves import map as compat_map
from six.moves import reduce as compat_reduce

from taskflow import atom
from taskflow import logging
from taskflow.types import notifier
from taskflow.utils import misc

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
class Task(atom.Atom):
    """An abstraction that defines a potential piece of work.

    This potential piece of work is expected to be able to contain
    functionality that defines what can be executed to accomplish that work
    as well as a way of defining what can be executed to reverted/undo that
    same piece of work.
    """

    # Known internal events this task can have callbacks bound to (others that
    # are not in this set/tuple will not be able to be bound); this should be
    # updated and/or extended in subclasses as needed to enable or disable new
    # or existing internal events...
    TASK_EVENTS = (EVENT_UPDATE_PROGRESS,)

    def __init__(self, name=None, provides=None, requires=None,
                 auto_extract=True, rebind=None, inject=None,
                 ignore_list=None, revert_rebind=None, revert_requires=None):
        if name is None:
            name = reflection.get_class_name(self)
        super(Task, self).__init__(name, provides=provides, requires=requires,
                                   auto_extract=auto_extract, rebind=rebind,
                                   inject=inject, revert_rebind=revert_rebind,
                                   revert_requires=revert_requires)
        self._notifier = notifier.RestrictedNotifier(self.TASK_EVENTS)

    @property
    def notifier(self):
        """Internal notification dispatcher/registry.

        A notification object that will dispatch events that occur related
        to *internal* notifications that the task internally emits to
        listeners (for example for progress status updates, telling others
        that a task has reached 50% completion...).
        """
        return self._notifier

    def copy(self, retain_listeners=True):
        """Clone/copy this task.

        :param retain_listeners: retain the attached notification listeners
                                 when cloning, when false the listeners will
                                 be emptied, when true the listeners will be
                                 copied and retained

        :return: the copied task
        """
        c = copy.copy(self)
        c._notifier = self._notifier.copy()
        if not retain_listeners:
            c._notifier.reset()
        return c

    def update_progress(self, progress):
        """Update task progress and notify all registered listeners.

        :param progress: task progress float value between 0.0 and 1.0
        """
        def on_clamped():
            LOG.warning("Progress value must be greater or equal to 0.0 or"
                        " less than or equal to 1.0 instead of being '%s'",
                        progress)
        cleaned_progress = misc.clamp(progress, 0.0, 1.0,
                                      on_clamped=on_clamped)
        self._notifier.notify(EVENT_UPDATE_PROGRESS,
                              {'progress': cleaned_progress})


class FunctorTask(Task):
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
        mapping = self._build_arg_mapping(execute, requires, rebind,
                                          auto_extract)
        self.rebind, exec_requires, self.optional = mapping

        if revert:
            revert_mapping = self._build_arg_mapping(revert, requires, rebind,
                                                     auto_extract)
        else:
            revert_mapping = (self.rebind, exec_requires, self.optional)
        (self.revert_rebind, revert_requires,
         self.revert_optional) = revert_mapping
        self.requires = exec_requires.union(revert_requires)

    def execute(self, *args, **kwargs):
        return self._execute(*args, **kwargs)

    def revert(self, *args, **kwargs):
        if self._revert:
            return self._revert(*args, **kwargs)
        else:
            return None


class ReduceFunctorTask(Task):
    """General purpose Task to reduce a list by applying a function.

    This Task mimics the behavior of Python's built-in ``reduce`` function. The
    Task takes a functor (lambda or otherwise) and a list. The list is
    specified using the ``requires`` argument of the Task. When executed, this
    task calls ``reduce`` with the functor and list as arguments. The resulting
    value from the call to ``reduce`` is then returned after execution.
    """
    def __init__(self, functor, requires, name=None, provides=None,
                 auto_extract=True, rebind=None, inject=None):

        if not six.callable(functor):
            raise ValueError("Function to use for reduce must be callable")

        f_args = reflection.get_callable_args(functor)
        if len(f_args) != 2:
            raise ValueError("%s arguments were provided. Reduce functor "
                             "must take exactly 2 arguments." % len(f_args))

        if not misc.is_iterable(requires):
            raise TypeError("%s type was provided for requires. Requires "
                            "must be an iterable." % type(requires))

        if len(requires) < 2:
            raise ValueError("%s elements were provided. Requires must have "
                             "at least 2 elements." % len(requires))

        if name is None:
            name = reflection.get_callable_name(functor)
        super(ReduceFunctorTask, self).__init__(name=name,
                                                provides=provides,
                                                inject=inject,
                                                requires=requires,
                                                rebind=rebind,
                                                auto_extract=auto_extract)

        self._functor = functor

    def execute(self, *args, **kwargs):
        l = [kwargs[r] for r in self.requires]
        return compat_reduce(self._functor, l)


class MapFunctorTask(Task):
    """General purpose Task to map a function to a list.

    This Task mimics the behavior of Python's built-in ``map`` function. The
    Task takes a functor (lambda or otherwise) and a list. The list is
    specified using the ``requires`` argument of the Task. When executed, this
    task calls ``map`` with the functor and list as arguments. The resulting
    list from the call to ``map`` is then returned after execution.

    Each value of the returned list can be bound to individual names using
    the ``provides`` argument, following taskflow standard behavior. Order is
    preserved in the returned list.
    """

    def __init__(self, functor, requires, name=None, provides=None,
                 auto_extract=True, rebind=None, inject=None):

        if not six.callable(functor):
            raise ValueError("Function to use for map must be callable")

        f_args = reflection.get_callable_args(functor)
        if len(f_args) != 1:
            raise ValueError("%s arguments were provided. Map functor must "
                             "take exactly 1 argument." % len(f_args))

        if not misc.is_iterable(requires):
            raise TypeError("%s type was provided for requires. Requires "
                            "must be an iterable." % type(requires))

        if name is None:
            name = reflection.get_callable_name(functor)
        super(MapFunctorTask, self).__init__(name=name, provides=provides,
                                             inject=inject, requires=requires,
                                             rebind=rebind,
                                             auto_extract=auto_extract)

        self._functor = functor

    def execute(self, *args, **kwargs):
        l = [kwargs[r] for r in self.requires]
        return list(compat_map(self._functor, l))
