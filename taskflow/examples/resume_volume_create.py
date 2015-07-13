# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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
import hashlib
import logging
import os
import random
import sys
import time

logging.basicConfig(level=logging.ERROR)

self_dir = os.path.abspath(os.path.dirname(__file__))
top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)
sys.path.insert(0, self_dir)

from oslo_utils import uuidutils

from taskflow import engines
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.persistence import models
from taskflow import task

import example_utils  # noqa

# INTRO: These examples show how a hierarchy of flows can be used to create a
# pseudo-volume in a reliable & resumable manner using taskflow + a miniature
# version of what cinder does while creating a volume (very miniature).


@contextlib.contextmanager
def slow_down(how_long=0.5):
    try:
        yield how_long
    finally:
        print("** Ctrl-c me please!!! **")
        time.sleep(how_long)


def find_flow_detail(backend, book_id, flow_id):
    # NOTE(harlowja): this is used to attempt to find a given logbook with
    # a given id and a given flow details inside that logbook, we need this
    # reference so that we can resume the correct flow (as a logbook tracks
    # flows and a flow detail tracks a individual flow).
    #
    # Without a reference to the logbook and the flow details in that logbook
    # we will not know exactly what we should resume and that would mean we
    # can't resume what we don't know.
    with contextlib.closing(backend.get_connection()) as conn:
        lb = conn.get_logbook(book_id)
        return lb.find(flow_id)


class PrintText(task.Task):
    def __init__(self, print_what, no_slow=False):
        content_hash = hashlib.md5(print_what.encode('utf-8')).hexdigest()[0:8]
        super(PrintText, self).__init__(name="Print: %s" % (content_hash))
        self._text = print_what
        self._no_slow = no_slow

    def execute(self):
        if self._no_slow:
            print("-" * (len(self._text)))
            print(self._text)
            print("-" * (len(self._text)))
        else:
            with slow_down():
                print("-" * (len(self._text)))
                print(self._text)
                print("-" * (len(self._text)))


class CreateSpecForVolumes(task.Task):
    def execute(self):
        volumes = []
        for i in range(0, random.randint(1, 10)):
            volumes.append({
                'type': 'disk',
                'location': "/dev/vda%s" % (i + 1),
            })
        return volumes


class PrepareVolumes(task.Task):
    def execute(self, volume_specs):
        for v in volume_specs:
            with slow_down():
                print("Dusting off your hard drive %s" % (v))
            with slow_down():
                print("Taking a well deserved break.")
            print("Your drive %s has been certified." % (v))


# Setup the set of things to do (mini-cinder).
flow = lf.Flow("root").add(
    PrintText("Starting volume create", no_slow=True),
    gf.Flow('maker').add(
        CreateSpecForVolumes("volume_specs", provides='volume_specs'),
        PrintText("I need a nap, it took me a while to build those specs."),
        PrepareVolumes(),
    ),
    PrintText("Finished volume create", no_slow=True))

# Setup the persistence & resumption layer.
with example_utils.get_backend() as backend:
    try:
        book_id, flow_id = sys.argv[2].split("+", 1)
    except (IndexError, ValueError):
        book_id = None
        flow_id = None

    if not all([book_id, flow_id]):
        # If no 'tracking id' (think a fedex or ups tracking id) is provided
        # then we create one by creating a logbook (where flow details are
        # stored) and creating a flow detail (where flow and task state is
        # stored). The combination of these 2 objects unique ids (uuids) allows
        # the users of taskflow to reassociate the workflows that were
        # potentially running (and which may have partially completed) back
        # with taskflow so that those workflows can be resumed (or reverted)
        # after a process/thread/engine has failed in someway.
        book = models.LogBook('resume-volume-create')
        flow_detail = models.FlowDetail("root", uuid=uuidutils.generate_uuid())
        book.add(flow_detail)
        with contextlib.closing(backend.get_connection()) as conn:
            conn.save_logbook(book)
        print("!! Your tracking id is: '%s+%s'" % (book.uuid,
                                                   flow_detail.uuid))
        print("!! Please submit this on later runs for tracking purposes")
    else:
        flow_detail = find_flow_detail(backend, book_id, flow_id)

    # Load and run.
    engine = engines.load(flow,
                          flow_detail=flow_detail,
                          backend=backend, engine='serial')
    engine.run()

# How to use.
#
# 1. $ python me.py "sqlite:////tmp/cinder.db"
# 2. ctrl-c before this finishes
# 3. Find the tracking id (search for 'Your tracking id is')
# 4. $ python me.py "sqlite:////tmp/cinder.db" "$tracking_id"
# 5. Profit!
