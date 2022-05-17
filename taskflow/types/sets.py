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

import collections
from collections import abc
import itertools


# Used for values that don't matter in sets backed by dicts...
_sentinel = object()


def _merge_in(target, iterable=None, sentinel=_sentinel):
    """Merges iterable into the target and returns the target."""
    if iterable is not None:
        for value in iterable:
            target.setdefault(value, sentinel)
    return target


class OrderedSet(abc.Set, abc.Hashable):
    """A read-only hashable set that retains insertion/initial ordering.

    It should work in all existing places that ``frozenset`` is used.

    See: https://mail.python.org/pipermail/python-ideas/2009-May/004567.html
    for an idea thread that *may* eventually (*someday*) result in this (or
    similar) code being included in the mainline python codebase (although
    the end result of that thread is somewhat discouraging in that regard).
    """

    __slots__ = ['_data']

    def __init__(self, iterable=None):
        self._data = _merge_in(collections.OrderedDict(), iterable)

    def __hash__(self):
        return self._hash()

    def __contains__(self, value):
        return value in self._data

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        for value in self._data.keys():
            yield value

    def __setstate__(self, items):
        self.__init__(iterable=iter(items))

    def __getstate__(self):
        return tuple(self)

    def __repr__(self):
        return "%s(%s)" % (type(self).__name__, list(self))

    def copy(self):
        """Return a shallow copy of a set."""
        return self._from_iterable(iter(self))

    def intersection(self, *sets):
        """Return the intersection of two or more sets as a new set.

        (i.e. elements that are common to all of the sets.)
        """
        def absorb_it(sets):
            for value in iter(self):
                matches = 0
                for s in sets:
                    if value in s:
                        matches += 1
                    else:
                        break
                if matches == len(sets):
                    yield value
        return self._from_iterable(absorb_it(sets))

    def issuperset(self, other):
        """Report whether this set contains another set."""
        for value in other:
            if value not in self:
                return False
        return True

    def issubset(self, other):
        """Report whether another set contains this set."""
        for value in iter(self):
            if value not in other:
                return False
        return True

    def difference(self, *sets):
        """Return the difference of two or more sets as a new set.

        (i.e. all elements that are in this set but not the others.)
        """
        def absorb_it(sets):
            for value in iter(self):
                seen = False
                for s in sets:
                    if value in s:
                        seen = True
                        break
                if not seen:
                    yield value
        return self._from_iterable(absorb_it(sets))

    def union(self, *sets):
        """Return the union of sets as a new set.

        (i.e. all elements that are in either set.)
        """
        return self._from_iterable(itertools.chain(iter(self), *sets))
