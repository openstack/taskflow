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


def flow_watch(state, details):
    flow = details['flow']
    old_state = details['old_state']
    context = details['context']
    print('Flow "%s": %s => %s' % (flow.name, old_state, flow.state))
    print('Flow "%s": context=%s' % (flow.name, context))


def task_watch(state, details):
    flow = details['flow']
    runner = details['runner']
    context = details['context']
    print('Flow "%s": runner "%s"' % (flow.name, runner.name))
    print('Flow "%s": context=%s' % (flow.name, context))


flow = lf.Flow("Call-them")
flow.add(call_jim)
flow.add(call_joe)
flow.notifier.register('*', flow_watch)
flow.task_notifier.register('*', task_watch)

context = {
    "joe_number": 444,
    "jim_number": 555,
}
flow.run(context)
