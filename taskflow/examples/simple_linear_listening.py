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


def call_jim(context):
    print("Calling jim.")
    print("Context = %s" % (context))


def call_joe(context):
    print("Calling joe.")
    print("Context = %s" % (context))


def flow_watch(state, details):
    print('Flow => %s' % state)


def task_watch(state, details):
    print('Task %s => %s' % (details.get('task_name'), state))


flow = lf.Flow("Call-them")
flow.add(task.FunctorTask(execute=call_jim))
flow.add(task.FunctorTask(execute=call_joe))

engine = eng.SingleThreadedActionEngine(flow)
engine.notifier.register('*', flow_watch)
engine.task_notifier.register('*', task_watch)

context = {
    "joe_number": 444,
    "jim_number": 555,
}
engine.storage.inject({'context': context})
engine.run()
