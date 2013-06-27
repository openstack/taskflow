import logging
import os
import sys

logging.basicConfig(level=logging.ERROR)

if not os.path.isfile(os.path.join(os.getcwd(), "taskflow", '__init__.py')):
    sys.path.insert(0, os.path.join(os.path.abspath(os.getcwd()), os.pardir))
else:
    sys.path.insert(0, os.path.abspath(os.getcwd()))

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
