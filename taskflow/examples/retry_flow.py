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

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)

import taskflow.engines
from taskflow.patterns import linear_flow as lf
from taskflow import retry
from taskflow import task

# INTRO: In this example we create a retry controller that receives a phone
# directory and tries different phone numbers. The next task tries to call Jim
# using the given number. If it is not a Jim's number, the task raises an
# exception and retry controller takes the next number from the phone
# directory and retries the call.
#
# This example shows a basic usage of retry controllers in a flow.
# Retry controllers allows to revert and retry a failed subflow with new
# parameters.


class CallJim(task.Task):
    def execute(self, jim_number):
        print ("Calling jim %s." % jim_number)
        if jim_number != 555:
            raise Exception("Wrong number!")
        else:
            print ("Hello Jim!")

    def revert(self, jim_number, **kwargs):
        print ("Wrong number, apologizing.")


# Create your flow and associated tasks (the work to be done).
flow = lf.Flow('retrying-linear',
               retry=retry.ParameterizedForEach(
                   rebind=['phone_directory'],
                   provides='jim_number')).add(CallJim())

# Now run that flow using the provided initial data (store below).
taskflow.engines.run(flow, store={'phone_directory': [333, 444, 555, 666]})
