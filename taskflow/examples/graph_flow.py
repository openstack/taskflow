import logging
import os
import sys

logging.basicConfig(level=logging.ERROR)

my_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.join(my_dir_path, os.pardir),
                                os.pardir))

from taskflow.engines.action_engine import engine as eng
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow import task


# In this example there are complex dependencies between
# tasks. User shouldn't care about ordering the Tasks.
# GraphFlow resolves dependencies automatically using tasks'
# requirements and provided values.
# Flows of any types can be nested into Graph flow. Subflows
# dependencies will be resolved too.


class Adder(task.Task):

    def execute(self, x, y):
        return x + y


flow = gf.Flow('root').add(
    lf.Flow('nested_linear').add(
        # x2 = y3+y4 = 12
        Adder("add2", provides='x2', rebind=['y3', 'y4']),
        # x1 = y1+y2 = 4
        Adder("add1", provides='x1', rebind=['y1', 'y2'])
    ),
    # x5 = x1+x3 = 20
    Adder("add5", provides='x5', rebind=['x1', 'x3']),
    # x3 = x1+x2 = 16
    Adder("add3", provides='x3', rebind=['x1', 'x2']),
    # x4 = x2+y5 = 21
    Adder("add4", provides='x4', rebind=['x2', 'y5']),
    # x6 = x5+x4 = 41
    Adder("add6", provides='x6', rebind=['x5', 'x4']),
    # x7 = x6+x6 = 82
    Adder("add7", provides='x7', rebind=['x6', 'x6']))

single_threaded_engine = eng.SingleThreadedActionEngine(flow)
single_threaded_engine.storage.inject({
    "y1": 1,
    "y2": 3,
    "y3": 5,
    "y4": 7,
    "y5": 9,
})

single_threaded_engine.run()

print ("Single threaded engine result %s" %
       single_threaded_engine.storage.fetch_all())

multi_threaded_engine = eng.MultiThreadedActionEngine(flow)
multi_threaded_engine.storage.inject({
    "y1": 1,
    "y2": 3,
    "y3": 5,
    "y4": 7,
    "y5": 9,
})

multi_threaded_engine.run()

print ("Multi threaded engine result %s" %
       multi_threaded_engine.storage.fetch_all())
