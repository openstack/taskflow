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
import copy

from oslo_utils import reflection
import six

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
class BaseTask(atom.Atom):
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

    def __init__(self, name, provides=None, inject=None):
        if name is None:
            name = reflection.get_class_name(self)
        super(BaseTask, self).__init__(name, provides, inject=inject)
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
            LOG.warn("Progress value must be greater or equal to 0.0 or less"
                     " than or equal to 1.0 instead of being '%s'", progress)
        cleaned_progress = misc.clamp(progress, 0.0, 1.0,
                                      on_clamped=on_clamped)
        self._notifier.notify(EVENT_UPDATE_PROGRESS,
                              {'progress': cleaned_progress})


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
