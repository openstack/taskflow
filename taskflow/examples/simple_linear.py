import logging
import os
import sys

logging.basicConfig(level=logging.ERROR)

my_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.join(my_dir_path, os.pardir),
                                os.pardir))

from taskflow.engines.action_engine import engine as eng
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

engine = eng.SingleThreadedActionEngine(flow)

engine.storage.inject({
    "joe_number": 444,
    "jim_number": 555,
})

engine.run()
