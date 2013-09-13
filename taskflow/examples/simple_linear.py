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
