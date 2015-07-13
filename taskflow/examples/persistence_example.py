# -*- coding: utf-8 -*-

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

import logging
import os
import sys
import tempfile
import traceback

logging.basicConfig(level=logging.ERROR)

self_dir = os.path.abspath(os.path.dirname(__file__))
top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)
sys.path.insert(0, self_dir)

from taskflow import engines
from taskflow.patterns import linear_flow as lf
from taskflow.persistence import models
from taskflow import task

import example_utils as eu  # noqa

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


class HiTask(task.Task):
    def execute(self):
        print("Hi!")

    def revert(self, **kwargs):
        print("Whooops, said hi too early, take that back!")


class ByeTask(task.Task):
    def __init__(self, blowup):
        super(ByeTask, self).__init__()
        self._blowup = blowup

    def execute(self):
        if self._blowup:
            raise Exception("Fail!")
        print("Bye!")


# This generates your flow structure (at this stage nothing is run).
def make_flow(blowup=False):
    flow = lf.Flow("hello-world")
    flow.add(HiTask(), ByeTask(blowup))
    return flow


# Persist the flow and task state here, if the file/dir exists already blow up
# if not don't blow up, this allows a user to see both the modes and to see
# what is stored in each case.
if eu.SQLALCHEMY_AVAILABLE:
    persist_path = os.path.join(tempfile.gettempdir(), "persisting.db")
    backend_uri = "sqlite:///%s" % (persist_path)
else:
    persist_path = os.path.join(tempfile.gettempdir(), "persisting")
    backend_uri = "file:///%s" % (persist_path)

if os.path.exists(persist_path):
    blowup = False
else:
    blowup = True

with eu.get_backend(backend_uri) as backend:
    # Make a flow that will blow up if the file didn't exist previously, if it
    # did exist, assume we won't blow up (and therefore this shows the undo
    # and redo that a flow will go through).
    book = models.LogBook("my-test")
    flow = make_flow(blowup=blowup)
    eu.print_wrapped("Running")
    try:
        eng = engines.load(flow, engine='serial',
                           backend=backend, book=book)
        eng.run()
        if not blowup:
            eu.rm_path(persist_path)
    except Exception:
        # NOTE(harlowja): don't exit with non-zero status code, so that we can
        # print the book contents, as well as avoiding exiting also makes the
        # unit tests (which also runs these examples) pass.
        traceback.print_exc(file=sys.stdout)

    eu.print_wrapped("Book contents")
    print(book.pformat())
