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

import contextlib
import logging
import os
import random
import sys
import time

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from oslo_utils import reflection

from taskflow import engines
from taskflow.listeners import printing
from taskflow.patterns import unordered_flow as uf
from taskflow import task

# INTRO: These examples show how unordered_flow can be used to create a large
# number of fake volumes in parallel (or serially, depending on a constant that
# can be easily changed).


@contextlib.contextmanager
def show_time(name):
    start = time.time()
    yield
    end = time.time()
    print(" -- %s took %0.3f seconds" % (name, end - start))


# This affects how many volumes to create and how much time to *simulate*
# passing for that volume to be created.
MAX_CREATE_TIME = 3
VOLUME_COUNT = 5

# This will be used to determine if all the volumes are created in parallel
# or whether the volumes are created serially (in an undefined ordered since
# a unordered flow is used). Note that there is a disconnection between the
# ordering and the concept of parallelism (since unordered items can still be
# ran in a serial ordering). A typical use-case for offering both is to allow
# for debugging using a serial approach, while when running at a larger scale
# one would likely want to use the parallel approach.
#
# If you switch this flag from serial to parallel you can see the overall
# time difference that this causes.
SERIAL = False
if SERIAL:
    engine = 'serial'
else:
    engine = 'parallel'


class VolumeCreator(task.Task):
    def __init__(self, volume_id):
        # Note here that the volume name is composed of the name of the class
        # along with the volume id that is being created, since a name of a
        # task uniquely identifies that task in storage it is important that
        # the name be relevant and identifiable if the task is recreated for
        # subsequent resumption (if applicable).
        #
        # UUIDs are *not* used as they can not be tied back to a previous tasks
        # state on resumption (since they are unique and will vary for each
        # task that is created). A name based off the volume id that is to be
        # created is more easily tied back to the original task so that the
        # volume create can be resumed/revert, and is much easier to use for
        # audit and tracking purposes.
        base_name = reflection.get_callable_name(self)
        super(VolumeCreator, self).__init__(name="%s-%s" % (base_name,
                                                            volume_id))
        self._volume_id = volume_id

    def execute(self):
        print("Making volume %s" % (self._volume_id))
        time.sleep(random.random() * MAX_CREATE_TIME)
        print("Finished making volume %s" % (self._volume_id))


# Assume there is no ordering dependency between volumes.
flow = uf.Flow("volume-maker")
for i in range(0, VOLUME_COUNT):
    flow.add(VolumeCreator(volume_id="vol-%s" % (i)))


# Show how much time the overall engine loading and running takes.
with show_time(name=flow.name.title()):
    eng = engines.load(flow, engine=engine)
    # This context manager automatically adds (and automatically removes) a
    # helpful set of state transition notification printing helper utilities
    # that show you exactly what transitions the engine is going through
    # while running the various volume create tasks.
    with printing.PrintingListener(eng):
        eng.run()
