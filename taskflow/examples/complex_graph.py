

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


def build_frame():
    return 'steel'


def build_engine():
    return 'honda'


def build_doors():
    return '2'


def build_wheels():
    return '4'


def install_engine(frame, engine):
    return True


def install_doors(frame, windows_installed, doors):
    return True


def install_windows(frame, doors):
    return True


def install_wheels(frame, engine, engine_installed, wheels):
    return True


def trash(**kwargs):
    print("Throwing away pieces of car!")


def startup(**kwargs):
    pass
    # TODO(harlowja): try triggering reversion here!
    # raise ValueError("Car not verified")
    return True


def verify(spec, **kwargs):
    for key, value in kwargs.items():
        if spec[key] != value:
            raise Exception("Car doesn't match spec!")
    return True


def flow_watch(state, details):
    print('Flow => %s' % state)


def task_watch(state, details):
    print('Task %s => %s' % (details.get('task_name'), state))


flow = lf.Flow("make-auto").add(
    task.FunctorTask(startup, revert=trash, provides='ran'),
    gf.Flow("install-parts").add(
        task.FunctorTask(build_frame, provides='frame'),
        task.FunctorTask(build_engine, provides='engine'),
        task.FunctorTask(build_doors, provides='doors'),
        task.FunctorTask(build_wheels, provides='wheels'),
        task.FunctorTask(install_engine, provides='engine_installed'),
        task.FunctorTask(install_doors, provides='doors_installed'),
        task.FunctorTask(install_windows, provides='windows_installed'),
        task.FunctorTask(install_wheels, provides='wheels_installed')),
    task.FunctorTask(verify, requires=['frame',
                                       'engine',
                                       'doors',
                                       'wheels',
                                       'engine_installed',
                                       'doors_installed',
                                       'windows_installed',
                                       'wheels_installed']))

engine = eng.SingleThreadedActionEngine(flow)
engine.notifier.register('*', flow_watch)
engine.task_notifier.register('*', task_watch)

engine.storage.inject({'spec': {
    "frame": 'steel',
    "engine": 'honda',
    "doors": '2',
    "wheels": '4',
    "engine_installed": True,
    "doors_installed": True,
    "windows_installed": True,
    "wheels_installed": True,
}})

print "Build a car"
engine.run()

engine = eng.SingleThreadedActionEngine(flow)
engine.notifier.register('*', flow_watch)
engine.task_notifier.register('*', task_watch)

engine.storage.inject({'spec': {
    "frame": 'steel',
    "engine": 'honda',
    "doors": '5',
    "wheels": '4',
    "engine_installed": True,
    "doors_installed": True,
    "windows_installed": True,
    "wheels_installed": True,
}})

try:
    print "Build a wrong car that doesn't match specification"
    engine.run()
except Exception as e:
    print e
