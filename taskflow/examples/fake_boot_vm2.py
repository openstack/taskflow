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

import logging
import os
import random
import sys

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf

from taskflow import engines
from taskflow import task


class PrintText(task.Task):
    def __init__(self, print_what):
        super(PrintText, self).__init__(name="Print: %s" % print_what)
        self._text = print_what

    def execute(self):
        print("-" * (len(self._text)))
        print(self._text)
        print("-" * (len(self._text)))


class AllocateIP(task.Task):
    def execute(self):
        return "192.168.0.%s" % (random.randint(1, 254))


class AllocateVolumes(task.Task):
    def execute(self):
        volumes = []
        for i in range(0, random.randint(0, 10)):
            volumes.append("/dev/vda%s" % (i + 1))
        return volumes


class CreateVM(task.Task):
    def execute(self, net_ip, volumes):
        print("Making vm %s using ip %s" % (self.name, net_ip))
        if volumes:
            print("With volumes:")
            for v in volumes:
                print(" - %s" % (v))


flow = lf.Flow("root").add(
    PrintText("Starting"),
    gf.Flow('maker').add(
        # First vm creation
        AllocateVolumes("volumes-1", provides='volumes_1'),
        AllocateIP("ip-1", provides='net_ip1'),
        CreateVM("vm-1", rebind=['net_ip1', 'volumes_1']),
        # Second vm creation
        AllocateVolumes("volumes-2", provides='volumes_2'),
        AllocateIP("ip-2", provides='net_ip2'),
        CreateVM("vm-2", rebind=['net_ip2', 'volumes_2'])
    ),
    PrintText("Finished"))


# The above vms will be created in parallel, dependencies will be ran in order
# so that means the ip and volume creation for each vm will run before the
# final vm creation is done, but both vm creates will run at the same time.
#
# Pretty cool!
engines.run(flow, engine_conf='parallel')
