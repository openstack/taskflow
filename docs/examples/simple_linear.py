import logging
import os
import sys

logging.basicConfig(level=logging.ERROR)

my_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.join(my_dir_path, os.pardir),
                                os.pardir))

from taskflow.patterns import linear_flow as lf


def call_jim(context):
    print("Calling jim.")
    print("Context = %s" % (context))


def call_joe(context):
    print("Calling joe.")
    print("Context = %s" % (context))


flow = lf.Flow("call-them")
flow.add(call_jim)
flow.add(call_joe)

context = {
    "joe_number": 444,
    "jim_number": 555,
}
flow.run(context)
