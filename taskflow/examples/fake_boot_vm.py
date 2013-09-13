import logging
import os
import random
import sys
import time
import uuid


logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)


import taskflow.engines
from taskflow.patterns import graph_flow as gf
from taskflow import task

# Simulates some type of persistance.
MY_DB = {
    'space': {
        'cpus': 2,
        'memory': 8192,
        'disk': 1024,
    },
    'vms': {},
    'places': ['127.0.0.1', '127.0.0.2', '127.0.0.3'],
}


# This prints out the transitions a flow is going through.
def flow_notify(state, details):
    print("'%s' entered state: %s" % (details.get('flow_name'), state))


# This prints out the transitions a flows tasks are going through.
def task_notify(state, details):
    print("'%s' entered state: %s" % (details.get('task_name'), state))


# Simulates what nova/glance/keystone... calls a context
class Context(object):
    def __init__(self, **items):
        self.__dict__.update(items)

    def __str__(self):
        return "Context: %s" % (self.__dict__)


# Simulates translating an api request into a validated format (aka a vm-spec)
# that will later be acted upon.
class ValidateAPIInputs(task.Task):
    def __init__(self):
        super(ValidateAPIInputs, self).__init__('validates-api-inputs',
                                                provides='vm_spec')

    def execute(self, context):
        print "Validating api inputs for %s" % (context)
        return {'cpus': 1,
                'memory': 512,
                'disk': 100,
                }


# Simulates reserving the space for a vm and associating the vm to be with
# a unique identifier.
class PeformReservation(task.Task):
    def __init__(self):
        super(PeformReservation, self).__init__('reserve-vm',
                                                provides='vm_reservation_spec')

    def revert(self, context, vm_spec, result):
        reserved_spec = result
        print("Undoing reservation of %s" % (reserved_spec['uuid']))
        vm_spec = MY_DB['vms'].pop(reserved_spec['uuid'])
        print 'Space before: %s' % (MY_DB['space'])
        # Unreserve 'atomically'
        for (k, v) in vm_spec.items():
            if k in ['scheduled']:
                continue
            MY_DB['space'][k] += v
        print 'Space after: %s' % (MY_DB['space'])

    def execute(self, context, vm_spec):
        print 'Reserving %s for %s' % (vm_spec, context)
        # Reserve 'atomically'
        print 'Space before: %s' % (MY_DB['space'])
        for (k, v) in vm_spec.items():
            if MY_DB['space'][k] < v:
                raise RuntimeError("Not enough %s available" % (k))
            MY_DB['space'][k] -= vm_spec[k]
        print 'Space after: %s' % (MY_DB['space'])
        # Create a fake 'db' entry for the vm
        vm_uuid = str(uuid.uuid4())
        MY_DB['vms'][vm_uuid] = vm_spec
        MY_DB['vms'][vm_uuid]['scheduled'] = False
        return {'uuid': vm_uuid,
                'reserved_on': time.time(),
                'vm_spec': vm_spec,
                }


# Simulates scheudling a vm to some location
class ScheduleVM(task.Task):
    def __init__(self):
        super(ScheduleVM, self).__init__('find-hole-for-vm',
                                         provides=['vm_hole', 'vm_uuid'])

    def revert(self, context, vm_reservation_spec, result):
        vm_uuid = result[1]
        vm_place = result[0]
        print "Marking %s as not having a home at %s anymore" % (vm_uuid,
                                                                 vm_place)
        MY_DB['vms'][vm_uuid]['scheduled'] = False
        MY_DB['places'].append(vm_place)

    def execute(self, context, vm_reservation_spec):
        print "Finding a place to put %s" % (vm_reservation_spec)
        vm_uuid = vm_reservation_spec['uuid']
        MY_DB['vms'][vm_uuid]['scheduled'] = True
        # Reserve the place 'atomically'
        vm_place = random.choice(MY_DB['places'])
        print 'Placing %s at %s' % (vm_uuid, vm_place)
        MY_DB['places'].remove(vm_place)
        return vm_place, vm_uuid


# Fail booting a vm to see what happens.
class BootVM(task.Task):
    def __init__(self):
        super(BootVM, self).__init__('boot-vm', provides='vm_booted')

    def execute(self, context, vm_reservation_spec, vm_hole):
        raise RuntimeError("Failed booting")


# Lets try booting a vm (not really) and see how the reversions work.
flow = gf.Flow("Boot-Fake-Vm").add(
    ValidateAPIInputs(),
    PeformReservation(),
    ScheduleVM(),
    BootVM())

# Simulates what nova/glance/keystone... calls a context
context = {
    'user_id': 'xyz',
    'project_id': 'abc',
    'is_admin': True,
}
context = Context(**context)

# Load the flow
engine = taskflow.engines.load(flow, store={'context': context})

# Get notified of the state changes the flow is going through.
engine.notifier.register('*', flow_notify)

# Get notified of the state changes the flows tasks/runners are going through.
engine.task_notifier.register('*', task_notify)


print '-' * 7
print 'Running'
print '-' * 7
try:
    engine.run()
except Exception as e:
    print 'Flow failed: %r' % e

print '-' * 11
print 'All results'
print '-' * 11
result = engine.storage.fetch_all()
for tid in sorted(result):
    print '%s => %s' % (tid, result[tid])
