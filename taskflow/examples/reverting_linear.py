import logging
import os
import sys

logging.basicConfig(level=logging.ERROR)

my_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.join(my_dir_path, os.pardir),
                                os.pardir))

from taskflow import blocks
from taskflow.engines.action_engine import engine as eng
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


flow = blocks.LinearFlow().add(blocks.Task(CallJim),
                               blocks.Task(CallJoe),
                               blocks.Task(CallSuzzie))
engine = eng.SingleThreadedActionEngine(flow)

engine.storage.inject({
    "joe_number": 444,
    "jim_number": 555,
    "suzzie_number": 666
})

try:
    engine.run()
except Exception as e:
    print "Flow failed: %r" % e
