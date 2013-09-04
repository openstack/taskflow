
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

"""Terminal blocks that actually run code
"""

from taskflow.blocks import base
from taskflow.utils import reflection


def _save_as_to_mapping(save_as):
    """Convert save_as to mapping name => index

    Result should follow taskflow.storage.Storage convention
    for mappings.
    """
    if save_as is None:
        return None
    if isinstance(save_as, basestring):
        return {save_as: None}
    elif isinstance(save_as, tuple):
        return dict((key, num) for num, key in enumerate(save_as))
    raise TypeError('Task block save_as parameter '
                    'should be str or tuple, not %r' % save_as)


def _build_arg_mapping(rebind_args, task):
    if rebind_args is None:
        rebind_args = {}
    task_args = reflection.get_required_callable_args(task.execute)
    nargs = len(task_args)
    if isinstance(rebind_args, (list, tuple)):
        if len(rebind_args) < nargs:
            raise ValueError('Task %(name)s takes %(nargs)d positional '
                             'arguments (%(real)d given)'
                             % dict(name=task.name, nargs=nargs,
                                    real=len(rebind_args)))
        result = dict(zip(task_args, rebind_args[:nargs]))
        # extra rebind_args go to kwargs
        result.update((a, a) for a in rebind_args[nargs:])
        return result
    elif isinstance(rebind_args, dict):
        result = dict((a, a) for a in task_args)
        result.update(rebind_args)
        return result
    else:
        raise TypeError('rebind_args should be list, tuple or dict')


class Task(base.Block):
    """A block that wraps a single task

    The task should be executed, and produced results should be saved.
    """

    def __init__(self, task, save_as=None, rebind_args=None):
        super(Task, self).__init__()
        self._task = task
        if isinstance(self._task, type):
            self._task = self._task()

        self._result_mapping = _save_as_to_mapping(save_as)
        self._args_mapping = _build_arg_mapping(rebind_args, self._task)

    @property
    def task(self):
        return self._task

    @property
    def result_mapping(self):
        return self._result_mapping

    @property
    def args_mapping(self):
        return self._args_mapping
