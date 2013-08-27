# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
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
import inspect

from taskflow import utils

# These arguments are ones that we will skip when parsing for requirements
# for a function to operate (when used as a task).
AUTO_ARGS = ('self', 'context', 'cls')


class Task(object):
    """An abstraction that defines a potential piece of work that can be
    applied and can be reverted to undo the work as a single unit.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, name):
        self._name = name
        # An *immutable* input 'resource' name set this task depends
        # on existing before this task can be applied.
        self.requires = set()
        # An *immutable* input 'resource' name set this task would like to
        # depends on existing before this task can be applied (but does not
        # strongly depend on existing).
        self.optional = set()
        # An *immutable* output 'resource' name set this task
        # produces that other tasks may depend on this task providing.
        self.provides = set()
        # This identifies the version of the task to be ran which
        # can be useful in resuming older versions of tasks. Standard
        # major, minor version semantics apply.
        self.version = (1, 0)

    @property
    def name(self):
        return self._name

    def __str__(self):
        return "%s==%s" % (self.name, utils.get_task_version(self))

    @abc.abstractmethod
    def __call__(self, context, *args, **kwargs):
        """Activate a given task which will perform some operation and return.

           This method can be used to apply some given context and given set
           of args and kwargs to accomplish some goal. Note that the result
           that is returned needs to be serializable so that it can be passed
           back into this task if reverting is triggered.
        """

    def revert(self, context, result, cause):
        """Revert this task using the given context, result that the apply
           provided as well as any information which may have caused
           said reversion.
        """


def _filter_arg(arg):
    if arg in AUTO_ARGS:
        return False
    # In certain decorator cases it seems like we get the function to be
    # decorated as an argument, we don't want to take that as a real argument.
    if not isinstance(arg, basestring):
        return False
    return True


class FunctorTask(Task):
    """Adaptor to make task from a callable

    Take any callable and make a task from it.
    """
    @staticmethod
    def _get_callable_name(execute_with):
        """Generate a name from callable"""
        im_class = getattr(execute_with, 'im_class', None)
        if im_class is not None:
            parts = (im_class.__module__, im_class.__name__,
                     execute_with.__name__)
        else:
            parts = (execute_with.__module__, execute_with.__name__)
        return '.'.join(parts)

    def __init__(self, execute_with, **kwargs):
        """Initialize FunctorTask instance with given callable and kwargs

        :param execute_with: the callable
        :param kwargs: reserved keywords (all optional) are
            name: name of the task, default None (auto generate)
            revert_with: the callable to revert, default None
            version: version of the task, default Task's version 1.0
            optionals: optionals of the task, default ()
            provides: provides of the task, default ()
            requires: requires of the task, default ()
            auto_extract: auto extract execute_with's args and put it into
                requires, default True
        """
        name = kwargs.pop('name', None)
        if name is None:
            name = self._get_callable_name(execute_with)
        super(FunctorTask, self).__init__(name)
        self._execute_with = execute_with
        self._revert_with = kwargs.pop('revert_with', None)
        self.version = kwargs.pop('version', self.version)
        self.optional.update(kwargs.pop('optional', ()))
        self.provides.update(kwargs.pop('provides', ()))
        self.requires.update(kwargs.pop('requires', ()))
        if kwargs.pop('auto_extract', True):
            f_args = inspect.getargspec(execute_with).args
            self.requires.update([arg for arg in f_args if _filter_arg(arg)])
        if kwargs:
            raise TypeError('__init__() got an unexpected keyword argument %r'
                            % kwargs.keys[0])

    def __call__(self, *args, **kwargs):
        return self._execute_with(*args, **kwargs)

    def revert(self, *args, **kwargs):
        if self._revert_with:
            return self._revert_with(*args, **kwargs)
        else:
            return None
