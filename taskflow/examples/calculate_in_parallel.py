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

# This examples shows how LinearFlow and ParallelFlow can be used
# together to execute calculations in parallel and then use the
# result for the next task. Adder task is used for all calculations
# and arguments' bindings are used to set correct parameters to the task.


class Provider(task.Task):

    def __init__(self, name, *args):
        super(Provider, self).__init__(name)
        self._provide = args

    def execute(self):
        return self._provide


class Adder(task.Task):

    def __init__(self, name):
        super(Adder, self).__init__(name)

    def execute(self, x, y):
        return x + y


flow = blocks.LinearFlow().add(
    # x1 = 2, y1 = 3, x2 = 5, x3 = 8
    blocks.Task(Provider("provide-adder", 2, 3, 5, 8),
                save_as=('x1', 'y1', 'x2', 'y2')),
    blocks.ParallelFlow().add(
        # z1 = x1+y1 = 5
        blocks.Task(Adder("add"), save_as='z1', rebind_args=['x1', 'y1']),
        # z2 = x2+y2 = 13
        blocks.Task(Adder("add"), save_as='z2', rebind_args=['x2', 'y2'])),
    # r = z1+z2 = 18
    blocks.Task(Adder("add"), save_as='r', rebind_args=['z1', 'z2']))

engine = eng.MultiThreadedActionEngine(flow)
engine.run()

print engine.storage.fetch_all()
