# -*- coding: utf-8 -*-

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

from taskflow.utils import misc


class Depth(misc.StrEnum):
    """Enumeration of decider(s) *area of influence*."""

    ALL = 'ALL'
    """
    **Default** decider depth that affects **all** successor atoms (including
    ones that are in successor nested flows).
    """

    FLOW = 'FLOW'
    """
    Decider depth that affects **all** successor tasks in the **same**
    flow (it will **not** affect tasks/retries that are in successor
    nested flows).

    .. warning::

       While using this kind we are allowed to execute successors of
       things that have been ignored (for example nested flows and the
       tasks they contain), this may result in symbol lookup errors during
       running, user beware.
    """

    NEIGHBORS = 'NEIGHBORS'
    """
    Decider depth that affects only **next** successor tasks (and does
    not traverse past **one** level of successor tasks).

    .. warning::

       While using this kind we are allowed to execute successors of
       things that have been ignored (for example nested flows and the
       tasks they contain), this may result in symbol lookup errors during
       running, user beware.
    """

    ATOM = 'ATOM'
    """
    Decider depth that affects only **targeted** atom (and does
    **not** traverse into **any** level of successor atoms).

    .. warning::

       While using this kind we are allowed to execute successors of
       things that have been ignored (for example nested flows and the
       tasks they contain), this may result in symbol lookup errors during
       running, user beware.
    """

    @classmethod
    def translate(cls, desired_depth):
        """Translates a string into a depth enumeration."""
        if isinstance(desired_depth, cls):
            # Nothing to do in the first place...
            return desired_depth
        if not isinstance(desired_depth, str):
            raise TypeError("Unexpected desired depth type, string type"
                            " expected, not %s" % type(desired_depth))
        try:
            return cls(desired_depth.upper())
        except ValueError:
            pretty_depths = sorted([a_depth.name for a_depth in cls])
            raise ValueError("Unexpected decider depth value, one of"
                             " %s (case-insensitive) is expected and"
                             " not '%s'" % (pretty_depths, desired_depth))


# Depth area of influence order (from greater influence to least).
#
# Order very much matters here...
_ORDERING = tuple([
    Depth.ALL, Depth.FLOW, Depth.NEIGHBORS, Depth.ATOM,
])


def pick_widest(depths):
    """Pick from many depths which has the **widest** area of influence."""
    return _ORDERING[min(_ORDERING.index(d) for d in depths)]
