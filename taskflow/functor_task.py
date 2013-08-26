# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
#    Copyright (C) 2013 AT&T Labs Inc. All Rights Reserved.
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

import inspect

from taskflow import task as base

# These arguments are ones that we will skip when parsing for requirements
# for a function to operate (when used as a task).
AUTO_ARGS = ('self', 'context', 'cls')


def _filter_arg(arg):
    if arg in AUTO_ARGS:
        return False
    # In certain decorator cases it seems like we get the function to be
    # decorated as an argument, we don't want to take that as a real argument.
    if not isinstance(arg, basestring):
        return False
    return True


class FunctorTask(base.Task):
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
            task_id:  id of the task, default None (auto generate)
            revert_with: the callable to revert, default None
            version: version of the task, default Task's version 1.0
            optionals: optionals of the task, default ()
            provides: provides of the task, default ()
            requires: requires of the task, default ()
            auto_extract: auto extract execute_with's args and put it into
                requires, default True
        """
        name = kwargs.pop('name', None)
        task_id = kwargs.pop('task_id', None)
        if name is None:
            name = self._get_callable_name(execute_with)
        super(FunctorTask, self).__init__(name, task_id)
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
