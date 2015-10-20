# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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
import itertools

import six
from six.moves import range as compat_range


def _ensure_iterable(func):

    @six.wraps(func)
    def wrapper(it, *args, **kwargs):
        if not isinstance(it, collections.Iterable):
            raise ValueError("Iterable expected, but '%s' is not"
                             " iterable" % it)
        return func(it, *args, **kwargs)

    return wrapper


@_ensure_iterable
def fill(it, desired_len, filler=None):
    """Iterates over a provided iterator up to the desired length.

    If the source iterator does not have enough values then the filler
    value is yielded until the desired length is reached.
    """
    if desired_len > 0:
        count = 0
        for value in it:
            yield value
            count += 1
            if count >= desired_len:
                return
        while count < desired_len:
            yield filler
            count += 1


@_ensure_iterable
def count(it):
    """Returns how many values in the iterator (depletes the iterator)."""
    return sum(1 for _value in it)


def unique_seen(it, *its):
    """Yields unique values from iterator(s) (and retains order)."""

    def _gen_it(all_its):
        # NOTE(harlowja): Generation is delayed so that validation
        # can happen before generation/iteration... (instead of
        # during generation/iteration)
        seen = set()
        while all_its:
            it = all_its.popleft()
            for value in it:
                if value not in seen:
                    yield value
                    seen.add(value)

    all_its = collections.deque([it])
    if its:
        all_its.extend(its)
    for it in all_its:
        if not isinstance(it, collections.Iterable):
            raise ValueError("Iterable expected, but '%s' is"
                             " not iterable" % it)
    return _gen_it(all_its)


@_ensure_iterable
def find_first_match(it, matcher, not_found_value=None):
    """Searches iterator for first value that matcher callback returns true."""
    for value in it:
        if matcher(value):
            return value
    return not_found_value


@_ensure_iterable
def while_is_not(it, stop_value):
    """Yields given values from iterator until stop value is passed.

    This uses the ``is`` operator to determine equivalency (and not the
    ``==`` operator).
    """
    for value in it:
        yield value
        if value is stop_value:
            break


def iter_forever(limit):
    """Yields values from iterator until a limit is reached.

    if limit is negative, we iterate forever.
    """
    if limit < 0:
        i = itertools.count()
        while True:
            yield next(i)
    else:
        for i in compat_range(0, limit):
            yield i
