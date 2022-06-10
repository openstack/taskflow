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

import csv
import logging
import os
import random
import sys

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

import futurist

from taskflow import engines
from taskflow.patterns import unordered_flow as uf
from taskflow import task

# INTRO: This example walks through a miniature workflow which does a parallel
# table modification where each row in the table gets adjusted by a thread, or
# green thread (if eventlet is available) in parallel and then the result
# is reformed into a new table and some verifications are performed on it
# to ensure everything went as expected.


MULTIPLER = 10


class RowMultiplier(task.Task):
    """Performs a modification of an input row, creating a output row."""

    def __init__(self, name, index, row, multiplier):
        super(RowMultiplier, self).__init__(name=name)
        self.index = index
        self.multiplier = multiplier
        self.row = row

    def execute(self):
        return [r * self.multiplier for r in self.row]


def make_flow(table):
    # This creation will allow for parallel computation (since the flow here
    # is specifically unordered; and when things are unordered they have
    # no dependencies and when things have no dependencies they can just be
    # ran at the same time, limited in concurrency by the executor or max
    # workers of that executor...)
    f = uf.Flow("root")
    for i, row in enumerate(table):
        f.add(RowMultiplier("m-%s" % i, i, row, MULTIPLER))
    # NOTE(harlowja): at this point nothing has ran, the above is just
    # defining what should be done (but not actually doing it) and associating
    # an ordering dependencies that should be enforced (the flow pattern used
    # forces this), the engine in the later main() function will actually
    # perform this work...
    return f


def main():
    if len(sys.argv) == 2:
        tbl = []
        with open(sys.argv[1], 'rb') as fh:
            reader = csv.reader(fh)
            for row in reader:
                tbl.append([float(r) if r else 0.0 for r in row])
    else:
        # Make some random table out of thin air...
        tbl = []
        cols = random.randint(1, 100)
        rows = random.randint(1, 100)
        for _i in range(0, rows):
            row = []
            for _j in range(0, cols):
                row.append(random.random())
            tbl.append(row)

    # Generate the work to be done.
    f = make_flow(tbl)

    # Now run it (using the specified executor)...
    try:
        executor = futurist.GreenThreadPoolExecutor(max_workers=5)
    except RuntimeError:
        # No eventlet currently active, use real threads instead.
        executor = futurist.ThreadPoolExecutor(max_workers=5)
    try:
        e = engines.load(f, engine='parallel', executor=executor)
        for st in e.run_iter():
            print(st)
    finally:
        executor.shutdown()

    # Find the old rows and put them into place...
    #
    # TODO(harlowja): probably easier just to sort instead of search...
    computed_tbl = []
    for i in range(0, len(tbl)):
        for t in f:
            if t.index == i:
                computed_tbl.append(e.storage.get(t.name))

    # Do some basic validation (which causes the return code of this process
    # to be different if things were not as expected...)
    if len(computed_tbl) != len(tbl):
        return 1
    else:
        return 0


if __name__ == "__main__":
    sys.exit(main())
