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
import random
import sys
import time

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from taskflow import engines
from taskflow.listeners import printing
from taskflow.patterns import unordered_flow as uf
from taskflow import task
from taskflow.utils import reflection


@contextlib.contextmanager
def show_time(name=''):
    start = time.time()
    yield
    end = time.time()
    print(" -- %s took %0.3f seconds" % (name, end - start))


MAX_CREATE_TIME = 3
VOLUME_COUNT = 5
SERIAL = False


class VolumeCreator(task.Task):
    def __init__(self, volume_id):
        base_name = reflection.get_callable_name(self)
        super(VolumeCreator, self).__init__(name="%s-%s" % (base_name,
                                                            volume_id))
        self._volume_id = volume_id

    def execute(self):
        print("Making volume %s" % (self._volume_id))
        time.sleep(random.random() * MAX_CREATE_TIME)
        print("Finished making volume %s" % (self._volume_id))


# Assume there is no ordering dependency between volumes
flow = uf.Flow("volume-maker")
for i in xrange(0, VOLUME_COUNT):
    flow.add(VolumeCreator(volume_id="vol-%s" % (i)))

if SERIAL:
    engine_conf = {
        'engine': 'serial',
    }
else:
    engine_conf = {
        'engine': 'parallel',
    }


with show_time(name=flow.name.title()):
    eng = engines.load(flow, engine_conf=engine_conf)
    with printing.PrintingListener(eng):
        eng.run()
