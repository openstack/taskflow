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


# In this example LinearFlow is used to group four tasks to
# calculate value. Added task is used twice. In the first case
# it uses default parameters ('x' and 'y') and in the second one
# arguments are binding with 'z' and 'd' keys from engine storage.
# Multiplier task uses binding too, but explicitly shows that 'z'
# parameter is binded with 'a' key from engine storage.


class Provider(task.Task):

    def __init__(self, name, *args, **kwargs):
        super(Provider, self).__init__(name=name, **kwargs)
        self._provide = args

    def execute(self):
        return self._provide


class Adder(task.Task):

    def __init__(self, name, provides=None, rebind=None):
        super(Adder, self).__init__(name=name, provides=provides,
                                    rebind=rebind)

    def execute(self, x, y):
        return x + y


class Multiplier(task.Task):
    def __init__(self, name, multiplier, provides=None, rebind=None):
        super(Multiplier, self).__init__(name=name, provides=provides,
                                         rebind=rebind)
        self._multiplier = multiplier

    def execute(self, z):
        return z * self._multiplier


flow = lf.Flow('root').add(
    # x = 2, y = 3, d = 5
    Provider("provide-adder", 2, 3, 5, provides=('x', 'y', 'd')),
    # z = x+y = 5
    Adder("add-1", provides='z'),
    # a = z+d = 10
    Adder("add-2", provides='a', rebind=['z', 'd']),
    # r = a*3 = 30
    Multiplier("multi", 3, provides='r', rebind={'z': 'a'})
)

results = taskflow.engines.run(flow)
print results
