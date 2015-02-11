# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Hewlett-Packard Development Company, L.P.
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
import math
import os
import sys

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from taskflow import engines
from taskflow.patterns import linear_flow
from taskflow import task

# INTRO: This shows how to use a tasks/atoms ability to take requirements from
# its execute functions default parameters and shows how to provide those
# via different methods when needed, to influence those parameters to in
# this case calculate the distance between two points in 2D space.

# A 2D point.
Point = collections.namedtuple("Point", "x,y")


def is_near(val, expected, tolerance=0.001):
    # Floats don't really provide equality...
    if val > (expected + tolerance):
        return False
    if val < (expected - tolerance):
        return False
    return True


class DistanceTask(task.Task):
    # See: http://en.wikipedia.org/wiki/Distance#Distance_in_Euclidean_space

    default_provides = 'distance'

    def execute(self, a=Point(0, 0), b=Point(0, 0)):
        return math.sqrt(math.pow(b.x - a.x, 2) + math.pow(b.y - a.y, 2))


if __name__ == '__main__':
    # For these we rely on the execute() methods points by default being
    # at the origin (and we override it with store values when we want) at
    # execution time (which then influences what is calculated).
    any_distance = linear_flow.Flow("origin").add(DistanceTask())
    results = engines.run(any_distance)
    print(results)
    print("%s is near-enough to %s: %s" % (results['distance'],
                                           0.0,
                                           is_near(results['distance'], 0.0)))

    results = engines.run(any_distance, store={'a': Point(1, 1)})
    print(results)
    print("%s is near-enough to %s: %s" % (results['distance'],
                                           1.4142,
                                           is_near(results['distance'],
                                                   1.4142)))

    results = engines.run(any_distance, store={'a': Point(10, 10)})
    print(results)
    print("%s is near-enough to %s: %s" % (results['distance'],
                                           14.14199,
                                           is_near(results['distance'],
                                                   14.14199)))

    results = engines.run(any_distance,
                          store={'a': Point(5, 5), 'b': Point(10, 10)})
    print(results)
    print("%s is near-enough to %s: %s" % (results['distance'],
                                           7.07106,
                                           is_near(results['distance'],
                                                   7.07106)))

    # For this we use the ability to override at task creation time the
    # optional arguments so that we don't need to continue to send them
    # in via the 'store' argument like in the above (and we fix the new
    # starting point 'a' at (10, 10) instead of (0, 0)...

    ten_distance = linear_flow.Flow("ten")
    ten_distance.add(DistanceTask(inject={'a': Point(10, 10)}))
    results = engines.run(ten_distance, store={'b': Point(10, 10)})
    print(results)
    print("%s is near-enough to %s: %s" % (results['distance'],
                                           0.0,
                                           is_near(results['distance'], 0.0)))

    results = engines.run(ten_distance)
    print(results)
    print("%s is near-enough to %s: %s" % (results['distance'],
                                           14.14199,
                                           is_near(results['distance'],
                                                   14.14199)))
