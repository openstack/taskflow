# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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
import functools
import inspect

from taskflow import task

AUTO_ARGS = ('self', 'context',)


def _get_args(f):
    args = []
    if hasattr(f, '__argspec'):
        args = list(f.__argspec.args)
    return set([a for a in args if a not in AUTO_ARGS and not
                isinstance(a, collections.Callable)])


def _set_argspec(f):
    if not hasattr(f, '__argspec'):
        f.__argspec = inspect.getargspec(f)


def requires(*args, **kwargs):

    def decorator(f):
        # Ensure we copy its arg spec since wrappers lose there wrapping
        # functions arg specification. This is supposedly fixed in python 3.3.
        _set_argspec(f)
        f.requires = _get_args(f)
        f.requires.update([a for a in args if a not in AUTO_ARGS and
                           not isinstance(a, collections.Callable)])

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            f(*args, **kwargs)

        return wrapper

    if kwargs or not args:
        return decorator
    else:
        if isinstance(args[0], collections.Callable):
            return decorator(args[0])
        else:
            return decorator


def provides(*args, **kwargs):

    def decorator(f):
        # Ensure we copy its arg spec since wrappers lose there wrapping
        # functions arg specification. This is supposedly fixed in python 3.3.
        _set_argspec(f)
        f.provides = set([a for a in args if a not in AUTO_ARGS and
                          not isinstance(a, collections.Callable)])

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            f(*args, **kwargs)

        return wrapper

    if kwargs or not args:
        return decorator
    else:
        if isinstance(args[0], collections.Callable):
            return decorator(args[0])
        else:
            return decorator


class FunctorTask(task.Task):
    """A simple task that can wrap two given functions and allow them to be
    in combination used in apply and reverting a given task. Useful for
    situations where existing functions already are in place and you just want
    to wrap them up."""

    def __init__(self, name, apply_functor, revert_functor,
                 provides_what=None, extract_requires=False):
        if not name:
            name = "_".join([apply_functor.__name__, revert_functor.__name__])
        super(FunctorTask, self).__init__(name)
        self._apply_functor = apply_functor
        self._revert_functor = revert_functor
        if provides_what:
            self.provides.update(provides_what)
        if extract_requires:
            for arg_name in inspect.getargspec(apply_functor).args:
                # These are automatically given, ignore.
                if arg_name in AUTO_ARGS:
                    continue
                self.requires.add(arg_name)

    def __call__(self, context, *args, **kwargs):
        return self._apply_functor(context, *args, **kwargs)

    def revert(self, context, result, cause):
        if self._revert_functor:
            self._revert_functor(context, result, cause)
