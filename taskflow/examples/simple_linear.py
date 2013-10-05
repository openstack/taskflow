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
import sys

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

import taskflow.engines
from taskflow.patterns import linear_flow as lf
from taskflow import task


class CallJim(task.Task):

    def __init__(self):
        super(CallJim, self).__init__()

    def execute(self, jim_number, *args, **kwargs):
        print("Calling jim %s." % jim_number)


class CallJoe(task.Task):

    def __init__(self):
        super(CallJoe, self).__init__()

    def execute(self, joe_number, *args, **kwargs):
        print("Calling joe %s." % joe_number)


flow = lf.Flow('simple-linear').add(
    CallJim(),
    CallJoe()
)

taskflow.engines.run(flow, store=dict(joe_number=444,
                                      jim_number=555))
