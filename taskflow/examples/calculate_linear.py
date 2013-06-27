import logging
import os
import sys

logging.basicConfig(level=logging.ERROR)

my_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.join(my_dir_path, os.pardir),
                                os.pardir))

from taskflow.patterns import linear_flow as lf
from taskflow import task


def flow_notify(state, details):
    print("'%s' entered state: %s" % (details['flow'], state))


def task_notify(state, details):
    print("'%s' entered state: %s" % (details['runner'], state))


# This class is used to populate requirements that further tasks need to run
# by populating those tasks from a dictionary and returning said dictionary
# when the flow runs (so that further tasks can use those values). You can
# think of this a needed bootstrapping of a flow in a way.
class Provider(task.Task):
    def __init__(self, name, **kwargs):
        super(Provider, self).__init__(name)
        self.provides.update(kwargs.keys())
        self._provide = kwargs

    def __call__(self, context):
        return self._provide


class Adder(task.Task):
    def __init__(self, name, x_name, y_name, provides_name):
        super(Adder, self).__init__(name)
        self.requires.update([x_name, y_name])
        self.provides.update([provides_name])
        self._provides_name = provides_name

    def __call__(self, context, **kwargs):
        return {
            self._provides_name: sum(kwargs.values()),
        }


class Multiplier(task.Task):
    def __init__(self, name, z_name, by_how_much):
        super(Multiplier, self).__init__(name)
        self.requires.update([z_name])
        self._by_how_much = by_how_much
        self._z_name = z_name

    def __call__(self, context, **kwargs):
        return kwargs.pop(self._z_name) * self._by_how_much


flow = lf.Flow("calc-them")
flow.add(Provider("provide-adder", x=2, y=3, d=5))

# Add x + y to produce z (5)
flow.add(Adder('add', 'x', 'y', 'z'))

# Add z + d to produce a (5 + 5)
flow.add(Adder('add', 'z', 'd', 'a'))

# Multiple a by 3 (30)
multi_uuid = flow.add(Multiplier('multi', 'a', 3))

# Get notified of the state changes the flow is going through.
flow.notifier.register('*', flow_notify)

# Get notified of the state changes the flows tasks/runners are going through.
flow.task_notifier.register('*', task_notify)

# Context is typically passed in openstack, it is not needed here.
print '-' * 7
print 'Running'
print '-' * 7
context = {}
flow.run(context)

# This will have the last results and the task that produced that result,
# but we don't care about the task that produced it and just want the result
# itself.
print '-' * 11
print 'All results'
print '-' * 11
for (uuid, v) in flow.results.items():
    print '%s => %s' % (uuid, v)

multi_results = flow.results[multi_uuid]
print '-' * 15
print "Multiply result"
print '-' * 15
print(multi_results)
assert multi_results == 30, "Example is broken"
