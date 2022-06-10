# -*- coding: utf-8 -*-

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

from collections import abc
import functools
import itertools


def _ensure_iterable(func):

    @functools.wraps(func)
    def wrapper(it, *args, **kwargs):
        if not isinstance(it, abc.Iterable):
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


def generate_delays(delay, max_delay, multiplier=2):
    """Generator/iterator that provides back delays values.

    The values it generates increments by a given multiple after each
    iteration (using the max delay as a upper bound). Negative values
    will never be generated... and it will iterate forever (ie it will never
    stop generating values).
    """
    if max_delay < 0:
        raise ValueError("Provided delay (max) must be greater"
                         " than or equal to zero")
    if delay < 0:
        raise ValueError("Provided delay must start off greater"
                         " than or equal to zero")
    if multiplier < 1.0:
        raise ValueError("Provided multiplier must be greater than"
                         " or equal to 1.0")

    def _gen_it():
        # NOTE(harlowja): Generation is delayed so that validation
        # can happen before generation/iteration... (instead of
        # during generation/iteration)
        curr_delay = delay
        while True:
            curr_delay = max(0, min(max_delay, curr_delay))
            yield curr_delay
            curr_delay = curr_delay * multiplier

    return _gen_it()


def unique_seen(its, seen_selector=None):
    """Yields unique values from iterator(s) (and retains order)."""

    def _gen_it(all_its):
        # NOTE(harlowja): Generation is delayed so that validation
        # can happen before generation/iteration... (instead of
        # during generation/iteration)
        seen = set()
        for it in all_its:
            for value in it:
                if seen_selector is not None:
                    maybe_seen_value = seen_selector(value)
                else:
                    maybe_seen_value = value
                if maybe_seen_value not in seen:
                    yield value
                    seen.add(maybe_seen_value)

    all_its = list(its)
    for it in all_its:
        if not isinstance(it, abc.Iterable):
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
        for i in range(0, limit):
            yield i
