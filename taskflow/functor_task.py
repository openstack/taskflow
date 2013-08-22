# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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


def _take_arg(a):
    if a in AUTO_ARGS:
        return False
    # In certain decorator cases it seems like we get the function to be
    # decorated as an argument, we don't want to take that as a real argument.
    if not isinstance(a, basestring):
        return False
    return True


class FunctorTask(base.Task):
    """Adaptor to make task from a callable

    Take any callable and make a task from it.
    """
    @staticmethod
    def _callable_name(function):
        """Generate a name from callable"""
        im_class = getattr(function, 'im_class', None)
        if im_class is not None:
            parts = (im_class.__module__, im_class.__name__, function.__name__)
        else:
            parts = (function.__module__, function.__name__)
        return '.'.join(parts)

    def __init__(self, execute_with, **kwargs):
        name = kwargs.pop('name', None)
        if name is None:
            name = self._callable_name(execute_with)
        super(FunctorTask, self).__init__(name, kwargs.pop('task_id', None))
        self._execute_with = execute_with
        self._revert_with = kwargs.pop('revert_with', None)
        self.version = kwargs.pop('version', self.version)

        self.requires.update(kwargs.pop('requires', ()))
        if kwargs.pop('auto_extract', True):
            f_args = inspect.getargspec(execute_with).args
            self.requires.update([a for a in f_args if _take_arg(a)])

        self.optional.update(kwargs.pop('optional', ()))
        self.provides.update(kwargs.pop('provides', ()))
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
