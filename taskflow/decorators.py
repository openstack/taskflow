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

AUTO_ARGS = ('self', 'context',)


def wraps(fn):
    """This will not be needed in python 3.2 or greater which already has this
    built-in to its functools.wraps method."""

    def wrapper(f):
        f = functools.wraps(fn)(f)
        f.__wrapped__ = getattr(fn, '__wrapped__', fn)
        return f

    return wrapper


def requires(*args, **kwargs):

    def decorator(f):
        if not hasattr(f, 'requires'):
            f.requires = set()

        inspect_what = f
        if hasattr(f, '__wrapped__'):
            inspect_what = f.__wrapped__

        f.requires.update([a for a in inspect.getargspec(inspect_what).args
                           if a not in AUTO_ARGS and not
                           isinstance(a, collections.Callable)])
        f.requires.update([a for a in args if a not in AUTO_ARGS and
                           not isinstance(a, collections.Callable)])

        @wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapper

    # This is needed to handle when the decorator has args or the decorator
    # doesn't have args, python is rather weird here...
    if kwargs or not args:
        return decorator
    else:
        if isinstance(args[0], collections.Callable):
            return decorator(args[0])
        else:
            return decorator


def provides(*args, **kwargs):

    def decorator(f):
        if not hasattr(f, 'provides'):
            f.provides = set()

        f.provides = set([a for a in args if a not in AUTO_ARGS and
                          not isinstance(a, collections.Callable)])

        @wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapper

    # This is needed to handle when the decorator has args or the decorator
    # doesn't have args, python is rather weird here...
    if kwargs or not args:
        return decorator
    else:
        if isinstance(args[0], collections.Callable):
            return decorator(args[0])
        else:
            return decorator
