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

import functools

from taskflow import task as base
from taskflow import utils


def wraps(fn):
    """This will not be needed in python 3.2 or greater which already has this
    built-in to its functools.wraps method.
    """

    def wrapper(f):
        f = functools.wraps(fn)(f)
        f.__wrapped__ = getattr(fn, '__wrapped__', fn)
        return f

    return wrapper


def locked(*args, **kwargs):

    def decorator(f):
        attr_name = kwargs.get('lock', '_lock')

        @wraps(f)
        def wrapper(*args, **kwargs):
            lock = getattr(args[0], attr_name)
            with lock:
                return f(*args, **kwargs)

        return wrapper

    # This is needed to handle when the decorator has args or the decorator
    # doesn't have args, python is rather weird here...
    if kwargs or not args:
        return decorator
    else:
        if len(args) == 1:
            return decorator(args[0])
        else:
            return decorator


def _original_function(fun):
    """Get original function from static or class method"""
    if isinstance(fun, staticmethod):
        return fun.__get__(object())
    elif isinstance(fun, classmethod):
        return fun.__get__(object()).im_func
    return fun


def task(*args, **kwargs):
    """Decorates a given function so that it can be used as a task"""

    def decorator(f):
        def task_factory(execute_with, **factory_kwargs):
            merged = kwargs.copy()
            merged.update(factory_kwargs)
            # NOTE(imelnikov): we can't capture f here because for
            # bound methods and bound class methods the object it
            # is bound to is yet unknown at the moment
            return base.FunctorTask(execute_with, **merged)
        w_f = _original_function(f)
        setattr(w_f, utils.TASK_FACTORY_ATTRIBUTE, task_factory)
        return f

    # This is needed to handle when the decorator has args or the decorator
    # doesn't have args, python is rather weird here...
    if kwargs:
        if args:
            raise TypeError('task decorator takes 0 positional arguments,'
                            '%s given' % len(args))
        return decorator
    else:
        if len(args) == 1:
            return decorator(args[0])
        else:
            return decorator
