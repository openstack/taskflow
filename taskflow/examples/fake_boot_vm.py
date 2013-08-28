import logging
import os
import random
import sys
import time
import uuid

logging.basicConfig(level=logging.ERROR)

my_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.join(my_dir_path, os.pardir),
                                os.pardir))

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
    print("'%s' entered state: %s" % (details['flow'], state))


# This prints out the transitions a flows tasks are going through.
def task_notify(state, details):
    print("'%s' entered state: %s" % (details['runner'], state))


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
        super(ValidateAPIInputs, self).__init__('validates-api-inputs')
        self.provides.update(['vm_spec'])

    def execute(self, context):
        print "Validating api inputs for %s" % (context)
        return {
            'vm_spec': {
                'cpus': 1,
                'memory': 512,
                'disk': 100,
            }
        }


# Simulates reserving the space for a vm and associating the vm to be with
# a unique identifier.
class PeformReservation(task.Task):
    def __init__(self):
        super(PeformReservation, self).__init__('reserve-vm')
        self.provides.update(['vm_reservation_spec'])
        self.requires.update(['vm_spec'])

    def revert(self, context, result, cause):
        reserved_spec = result['vm_reservation_spec']
        print("Undoing reservation of %s" % (reserved_spec['uuid']))
        vm_spec = MY_DB['vms'].pop(reserved_spec['uuid'])
        print 'Space before: %s' % (MY_DB['space'])
        # Unreserve 'atomically'
        for (k, v) in vm_spec.items():
            if k in ['scheduled']:
                continue
            MY_DB['space'][k] += vm_spec[k]
        print 'Space after: %s' % (MY_DB['space'])

    def execute(self, context, vm_spec):
        print 'Reserving %s for %s' % (vm_spec, context)
        # Reserve 'atomically'
        print 'Space before: %s' % (MY_DB['space'])
        for (k, v) in vm_spec.items():
            if MY_DB['space'][k] < vm_spec[k]:
                raise RuntimeError("Not enough %s available" % (k))
            MY_DB['space'][k] -= vm_spec[k]
        print 'Space after: %s' % (MY_DB['space'])
        # Create a fake 'db' entry for the vm
        vm_uuid = str(uuid.uuid4())
        MY_DB['vms'][vm_uuid] = vm_spec
        MY_DB['vms'][vm_uuid]['scheduled'] = False
        return {
            'vm_reservation_spec': {
                'uuid': vm_uuid,
                'reserved_on': time.time(),
                'vm_spec': vm_spec,
            }
        }


# Simulates scheudling a vm to some location
class ScheduleVM(task.Task):
    def __init__(self):
        super(ScheduleVM, self).__init__('find-hole-for-vm')
        self.provides.update(['vm_hole'])
        self.requires.update(['vm_reservation_spec'])

    def revert(self, context, result, cause):
        vm_uuid = result['vm_uuid']
        vm_place = result['vm_hole']
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
        return {
            'vm_hole': vm_place,
            'vm_uuid': vm_uuid,
        }


# Fail booting a vm to see what happens.
class BootVM(task.Task):
    def __init__(self):
        super(BootVM, self).__init__('boot-vm')
        self.provides.update(['vm_booted'])
        self.requires.update(['vm_reservation_spec', 'vm_hole'])

    def execute(self, context, vm_reservation_spec, vm_hole):
        raise RuntimeError("Failed booting")


# Lets try booting a vm (not really) and see how the reversions work.
flo = gf.Flow("Boot-Fake-Vm")
flo.add(ValidateAPIInputs())
flo.add(PeformReservation())
flo.add(ScheduleVM())
flo.add(BootVM())

# Get notified of the state changes the flow is going through.
flo.notifier.register('*', flow_notify)

# Get notified of the state changes the flows tasks/runners are going through.
flo.task_notifier.register('*', task_notify)

# Simulates what nova/glance/keystone... calls a context
context = {
    'user_id': 'xyz',
    'project_id': 'abc',
    'is_admin': True,
}
context = Context(**context)

print '-' * 7
print 'Running'
print '-' * 7
try:
    flo.run(context)
except Exception as e:
    print 'Flow failed: %r' % e

print '-- Flow state %s' % (flo.state)

print '-' * 11
print 'All results'
print '-' * 11
for (tid, v) in sorted(flo.results.items()):
    print '%s => %s' % (tid, v)
