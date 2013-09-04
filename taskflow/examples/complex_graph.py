

import logging
import os
import sys


print('GraphFlow is under refactoring now, so this example '
      'is temporarily broken')
sys.exit(0)

logging.basicConfig(level=logging.ERROR)

my_dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.join(my_dir_path, os.pardir),
                                os.pardir))

from taskflow import decorators
from taskflow.patterns import graph_flow as gf


def flow_notify(state, details):
    print("'%s' entered state: %s" % (details['flow'], state))


def task_notify(state, details):
    print("'%s' entered state: %s" % (details['runner'], state))


@decorators.task(provides=['spec'])
def build_spec(context):
    params = context['inputs']
    verified = {}
    for k, v in params.items():
        verified[k] = int(v)
    return {
        'spec': verified,
    }


@decorators.task(provides=['frame'])
def build_frame(context, spec):
    return {
        'frame': 'steel',
    }


@decorators.task(provides=['engine'])
def build_engine(context, spec):
    return {
        'engine': 'honda',
    }


@decorators.task(provides=['doors'])
def build_doors(context, spec):
    return {
        'doors': '2',
    }


@decorators.task(provides=['wheels'])
def build_wheels(context, spec):
    return {
        'wheels': '4',
    }


@decorators.task(provides=['wheels'])
def build_windows(context, spec):
    return {
        'windows': '4',
    }


@decorators.task(provides=['engine_installed'])
def install_engine(context, frame, engine):
    return {
        'engine_installed': True,
    }


@decorators.task
def install_doors(context, frame, windows_installed, doors):
    pass


@decorators.task(provides=['windows_installed'])
def install_windows(context, frame, doors):
    return {
        'windows_installed': True,
    }


@decorators.task
def install_wheels(context, frame, engine, engine_installed, wheels):
    pass


def trash(context, result, cause):
    print("Throwing away pieces of car!")


@decorators.task(revert=trash)
def startup(context, **kwargs):
    pass
    # TODO(harlowja): try triggering reversion here!
    # raise ValueError("Car not verified")
    return {
        'ran': True,
    }


flow = gf.Flow("make-auto")
flow.notifier.register('*', flow_notify)
flow.task_notifier.register('*', task_notify)


# Lets build a car!!
flow.add(build_spec)
flow.add(build_frame)
flow.add(build_engine)
flow.add(build_doors)
flow.add(build_wheels)
i_uuid1 = flow.add(install_engine)
i_uuid2 = flow.add(install_doors)
i_uuid3 = flow.add(install_windows)
i_uuid4 = flow.add(install_wheels)
install_uuids = [i_uuid1, i_uuid2, i_uuid3, i_uuid4]

# Lets add a manual dependency that startup needs all the installation to
# complete, this could be done automatically but lets now instead ;)
startup_uuid = flow.add(startup)
for i_uuid in install_uuids:
    flow.add_dependency(i_uuid, startup_uuid)

# Now begin the build!
context = {
    "inputs": {
        'engine': 123,
        'tire': '234',
    }
}
print '-' * 7
print 'Running'
print '-' * 7
flow.run(context)

print '-' * 11
print 'All results'
print '-' * 11
for (uuid, v) in flow.results.items():
    print '%s => %s' % (uuid, v)
