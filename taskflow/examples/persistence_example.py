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

import contextlib
import logging
import os
import sys
import traceback

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from taskflow import engines
from taskflow.patterns import linear_flow as lf
from taskflow.persistence import backends
from taskflow.persistence import logbook
from taskflow import task
from taskflow.utils import persistence_utils as p_utils

import tempfile


# INTRO: In this example we create two tasks, one that will say hi and one
# that will say bye with optional capability to raise an error while
# executing. During execution if a later task fails, the reverting that will
# occur in the hi task will undo this (in a ~funny~ way).
#
# To also show the effect of task persistence we create a temporary database
# that will track the state transitions of this hi + bye workflow, this
# persistence allows for you to examine what is stored (using a sqlite client)
# as well as shows you what happens during reversion and what happens to
# the database during both of these modes (failing or not failing).

def print_wrapped(text):
    print("-" * (len(text)))
    print(text)
    print("-" * (len(text)))


class HiTask(task.Task):
    def execute(self):
        print("Hi!")

    def revert(self, **kwargs):
        print("Whooops, said hi to early, take that back!")


class ByeTask(task.Task):
    def __init__(self, blowup):
        super(ByeTask, self).__init__()
        self._blowup = blowup

    def execute(self):
        if self._blowup:
            raise Exception("Fail!")
        print("Bye!")


# This generates your flow structure (at this stage nothing is ran).
def make_flow(blowup=False):
    flo = lf.Flow("hello-world")
    flo.add(HiTask(), ByeTask(blowup))
    return flo


# Persist the flow and task state here, if the file exists already blowup
# if not don't blowup, this allows a user to see both the modes and to
# see what is stored in each case.
persist_filename = os.path.join(tempfile.gettempdir(), "persisting.db")
if os.path.isfile(persist_filename):
    blowup = False
else:
    blowup = True

# Ensure schema upgraded before we continue working.
backend_config = {
    'connection': "sqlite:///%s" % (persist_filename),
}
with contextlib.closing(backends.fetch(backend_config)) as be:
    with contextlib.closing(be.get_connection()) as conn:
        conn.upgrade()

# Now we can run.
engine_config = {
    'backend': backend_config,
    'engine_conf': 'serial',
    'book': logbook.LogBook("my-test"),
}

# Make a flow that will blowup if the file doesn't exist previously, if it
# did exist, assume we won't blowup (and therefore this shows the undo
# and redo that a flow will go through).
flo = make_flow(blowup=blowup)
print_wrapped("Running")

try:
    eng = engines.load(flo, **engine_config)
    eng.run()
    try:
        os.unlink(persist_filename)
    except (OSError, IOError):
        pass
except Exception:
    # NOTE(harlowja): don't exit with non-zero status code, so that we can
    # print the book contents, as well as avoiding exiting also makes the
    # unit tests (which also runs these examples) pass.
    traceback.print_exc(file=sys.stdout)

print_wrapped("Book contents")
print(p_utils.pformat(engine_config['book']))
