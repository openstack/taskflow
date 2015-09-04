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

import itertools


def count(it):
    """Returns how many values in the iterator (depletes the iterator)."""
    return sum(1 for _value in it)


def unique_seen(it, *its):
    """Yields unique values from iterator(s) (and retains order)."""
    seen = set()
    for value in itertools.chain(it, *its):
        if value in seen:
            continue
        else:
            yield value
            seen.add(value)


def find_first_match(it, matcher, not_found_value=None):
    """Searches iterator for first value that matcher callback returns true."""
    for value in it:
        if matcher(value):
            return value
    return not_found_value


def while_is_not(it, stop_value):
    """Yields given values from iterator until stop value is passed.

    This uses the ``is`` operator to determine equivalency (and not the
    ``==`` operator).
    """
    for value in it:
        yield value
        if value is stop_value:
            break
