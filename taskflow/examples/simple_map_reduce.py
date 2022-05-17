# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

import logging
import os
import sys

logging.basicConfig(level=logging.ERROR)

self_dir = os.path.abspath(os.path.dirname(__file__))
top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)
sys.path.insert(0, self_dir)

# INTRO: These examples show a simplistic map/reduce implementation where
# a set of mapper(s) will sum a series of input numbers (in parallel) and
# return their individual summed result. A reducer will then use those
# produced values and perform a final summation and this result will then be
# printed (and verified to ensure the calculation was as expected).

from taskflow import engines
from taskflow.patterns import linear_flow
from taskflow.patterns import unordered_flow
from taskflow import task


class SumMapper(task.Task):
    def execute(self, inputs):
        # Sums some set of provided inputs.
        return sum(inputs)


class TotalReducer(task.Task):
    def execute(self, *args, **kwargs):
        # Reduces all mapped summed outputs into a single value.
        total = 0
        for (k, v) in kwargs.items():
            # If any other kwargs was passed in, we don't want to use those
            # in the calculation of the total...
            if k.startswith('reduction_'):
                total += v
        return total


def chunk_iter(chunk_size, upperbound):
    """Yields back chunk size pieces from zero to upperbound - 1."""
    chunk = []
    for i in range(0, upperbound):
        chunk.append(i)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []


# Upper bound of numbers to sum for example purposes...
UPPER_BOUND = 10000

# How many mappers we want to have.
SPLIT = 10

# How big of a chunk we want to give each mapper.
CHUNK_SIZE = UPPER_BOUND // SPLIT

# This will be the workflow we will compose and run.
w = linear_flow.Flow("root")

# The mappers will run in parallel.
store = {}
provided = []
mappers = unordered_flow.Flow('map')
for i, chunk in enumerate(chunk_iter(CHUNK_SIZE, UPPER_BOUND)):
    mapper_name = 'mapper_%s' % i
    # Give that mapper some information to compute.
    store[mapper_name] = chunk
    # The reducer uses all of the outputs of the mappers, so it needs
    # to be recorded that it needs access to them (under a specific name).
    provided.append("reduction_%s" % i)
    mappers.add(SumMapper(name=mapper_name,
                          rebind={'inputs': mapper_name},
                          provides=provided[-1]))
w.add(mappers)

# The reducer will run last (after all the mappers).
w.add(TotalReducer('reducer', requires=provided))

# Now go!
e = engines.load(w, engine='parallel', store=store, max_workers=4)
print("Running a parallel engine with options: %s" % e.options)
e.run()

# Now get the result the reducer created.
total = e.storage.get('reducer')
print("Calculated result = %s" % total)

# Calculate it manually to verify that it worked...
calc_total = sum(range(0, UPPER_BOUND))
if calc_total != total:
    sys.exit(1)
