import logging
import os
import sys

logging.basicConfig(level=logging.ERROR)

my_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.join(my_dir_path, os.pardir),
                                os.pardir))

from taskflow.engines.action_engine import engine as eng
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow import task

# This examples shows how LinearFlow and ParallelFlow can be used
# together to execute calculations in parallel and then use the
# result for the next task. Adder task is used for all calculations
# and arguments' bindings are used to set correct parameters to the task.


class Provider(task.Task):

    def __init__(self, name, *args, **kwargs):
        super(Provider, self).__init__(name=name, **kwargs)
        self._provide = args

    def execute(self):
        return self._provide


class Adder(task.Task):

    def __init__(self, name, provides, rebind):
        super(Adder, self).__init__(name=name, provides=provides,
                                    rebind=rebind)

    def execute(self, x, y):
        return x + y


flow = lf.Flow('root').add(
    # x1 = 2, y1 = 3, x2 = 5, x3 = 8
    Provider("provide-adder", 2, 3, 5, 8,
             provides=('x1', 'y1', 'x2', 'y2')),
    uf.Flow('adders').add(
        # z1 = x1+y1 = 5
        Adder(name="add", provides='z1', rebind=['x1', 'y1']),
        # z2 = x2+y2 = 13
        Adder(name="add-2", provides='z2', rebind=['x2', 'y2']),
    ),
    # r = z1+z2 = 18
    Adder(name="sum-1", provides='r', rebind=['z1', 'z2']))

engine = eng.MultiThreadedActionEngine(flow)
engine.run()

print engine.storage.fetch_all()
