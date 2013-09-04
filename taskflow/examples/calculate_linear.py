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


# In this example LinearFlow is used to group four tasks to
# calculate value. Added task is used twice. In the first case
# it uses default parameters ('x' and 'y') and in the second one
# arguments are binding with 'z' and 'd' keys from engine storage.
# Multiplier task uses binding too, but explicitly shows that 'z'
# parameter is binded with 'a' key from engine storage.


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


class Multiplier(task.Task):
    def __init__(self, name, multiplier):
        super(Multiplier, self).__init__(name)
        self._multiplier = multiplier

    def execute(self, z):
        return z * self._multiplier


flow = blocks.LinearFlow().add(
    # x = 2, y = 3, d = 5
    blocks.Task(Provider("provide-adder", 2, 3, 5), save_as=('x', 'y', 'd')),
    # z = x+y = 5
    blocks.Task(Adder("add"), save_as='z'),
    # a = z+d = 10
    blocks.Task(Adder("add"), save_as='a', rebind_args=['z', 'd']),
    # r = a*3 = 30
    blocks.Task(Multiplier("multi", 3), save_as='r', rebind_args={'z': 'a'}))

engine = eng.SingleThreadedActionEngine(flow)
engine.run()

print engine.storage.fetch_all()
