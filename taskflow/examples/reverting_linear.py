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

    def revert(self, suzzie_number, *args, **kwargs):
        # TODO(imelnikov): this method should not be requred
        pass


flow = lf.Flow('simple-linear').add(
    CallJim(),
    CallJoe(),
    CallSuzzie()
)

try:
    taskflow.engines.run(flow, store=dict(joe_number=444,
                                          jim_number=555,
                                          suzzie_number=666))
except Exception as e:
    print "Flow failed: %r" % e
