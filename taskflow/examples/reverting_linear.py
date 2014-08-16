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
from taskflow import task

# INTRO: In this example we create three tasks, each of which ~calls~ a given
# number (provided as a function input), one of those tasks *fails* calling a
# given number (the suzzie calling); this causes the workflow to enter the
# reverting process, which activates the revert methods of the previous two
# phone ~calls~.
#
# This simulated calling makes it appear like all three calls occur or all
# three don't occur (transaction-like capabilities). No persistence layer is
# used here so reverting and executing will *not* be tolerant of process
# failure.


class CallJim(task.Task):
    def execute(self, jim_number, *args, **kwargs):
        print("Calling jim %s." % jim_number)

    def revert(self, jim_number, *args, **kwargs):
        print("Calling %s and apologizing." % jim_number)


class CallJoe(task.Task):
    def execute(self, joe_number, *args, **kwargs):
        print("Calling joe %s." % joe_number)

    def revert(self, joe_number, *args, **kwargs):
        print("Calling %s and apologizing." % joe_number)


class CallSuzzie(task.Task):
    def execute(self, suzzie_number, *args, **kwargs):
        raise IOError("Suzzie not home right now.")


# Create your flow and associated tasks (the work to be done).
flow = lf.Flow('simple-linear').add(
    CallJim(),
    CallJoe(),
    CallSuzzie()
)

try:
    # Now run that flow using the provided initial data (store below).
    taskflow.engines.run(flow, store=dict(joe_number=444,
                                          jim_number=555,
                                          suzzie_number=666))
except Exception as e:
    # NOTE(harlowja): This exception will be the exception that came out of the
    # 'CallSuzzie' task instead of a different exception, this is useful since
    # typically surrounding code wants to handle the original exception and not
    # a wrapped or altered one.
    #
    # *WARNING* If this flow was multi-threaded and multiple active tasks threw
    # exceptions then the above exception would be wrapped into a combined
    # exception (the object has methods to iterate over the contained
    # exceptions). See: exceptions.py and the class 'WrappedFailure' to look at
    # how to deal with multiple tasks failing while running.
    #
    # You will also note that this is not a problem in this case since no
    # parallelism is involved; this is ensured by the usage of a linear flow
    # and the default engine type which is 'serial' vs being 'parallel'.
    print("Flow failed: %s" % e)
