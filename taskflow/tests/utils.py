# -*- coding: utf-8 -*-

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import contextlib
import string

import six

from taskflow import exceptions
from taskflow.persistence.backends import impl_memory
from taskflow import retry
from taskflow import task
from taskflow.types import failure
from taskflow.utils import kazoo_utils
from taskflow.utils import threading_utils

ARGS_KEY = '__args__'
KWARGS_KEY = '__kwargs__'
ORDER_KEY = '__order__'
ZK_TEST_CONFIG = {
    'timeout': 1.0,
    'hosts': ["localhost:2181"],
}
# If latches/events take longer than this to become empty/set, something is
# usually wrong and should be debugged instead of deadlocking...
WAIT_TIMEOUT = 300


@contextlib.contextmanager
def wrap_all_failures():
    """Convert any exceptions to WrappedFailure.

    When you expect several failures, it may be convenient
    to wrap any exception with WrappedFailure in order to
    unify error handling.
    """
    try:
        yield
    except Exception:
        raise exceptions.WrappedFailure([failure.Failure()])


def zookeeper_available(min_version, timeout=3):
    client = kazoo_utils.make_client(ZK_TEST_CONFIG.copy())
    try:
        # NOTE(imelnikov): 3 seconds we should be enough for localhost
        client.start(timeout=float(timeout))
        if min_version:
            zk_ver = client.server_version()
            if zk_ver >= min_version:
                return True
            else:
                return False
        else:
            return True
    except Exception:
        return False
    finally:
        kazoo_utils.finalize_client(client)


class NoopRetry(retry.AlwaysRevert):
    pass


class NoopTask(task.Task):

    def execute(self):
        pass


class DummyTask(task.Task):

    def execute(self, context, *args, **kwargs):
        pass


class FakeTask(object):

    def execute(self, **kwargs):
        pass


if six.PY3:
    RUNTIME_ERROR_CLASSES = ['RuntimeError', 'Exception',
                             'BaseException', 'object']
else:
    RUNTIME_ERROR_CLASSES = ['RuntimeError', 'StandardError', 'Exception',
                             'BaseException', 'object']


class ProvidesRequiresTask(task.Task):
    def __init__(self, name, provides, requires, return_tuple=True):
        super(ProvidesRequiresTask, self).__init__(name=name,
                                                   provides=provides,
                                                   requires=requires)
        self.return_tuple = isinstance(provides, (tuple, list))

    def execute(self, *args, **kwargs):
        if self.return_tuple:
            return tuple(range(len(self.provides)))
        else:
            return dict((k, k) for k in self.provides)


def task_callback(state, values, details):
    name = details.get('task_name', None)
    if not name:
        name = details.get('retry_name', '<unknown>')
    values.append('%s %s' % (name, state))


def flow_callback(state, values, details):
    values.append('flow %s' % state)


def register_notifiers(engine, values):
    engine.notifier.register('*', flow_callback, kwargs={'values': values})
    engine.task_notifier.register('*', task_callback,
                                  kwargs={'values': values})


class SaveOrderTask(task.Task):

    def __init__(self, name=None, *args, **kwargs):
        super(SaveOrderTask, self).__init__(name=name, *args, **kwargs)
        self.values = EngineTestBase.values

    def execute(self, **kwargs):
        self.update_progress(0.0)
        self.values.append(self.name)
        self.update_progress(1.0)
        return 5

    def revert(self, **kwargs):
        self.update_progress(0)
        self.values.append(self.name + ' reverted(%s)'
                           % kwargs.get('result'))
        self.update_progress(1.0)


class FailingTask(SaveOrderTask):
    def execute(self, **kwargs):
        self.update_progress(0)
        self.update_progress(0.99)
        raise RuntimeError('Woot!')


class TaskWithFailure(task.Task):

    def execute(self, **kwargs):
        raise RuntimeError('Woot!')


class ProgressingTask(task.Task):

    def execute(self, *args, **kwargs):
        self.update_progress(0.0)
        self.update_progress(1.0)
        return 5


class FailingTaskWithOneArg(SaveOrderTask):
    def execute(self, x, **kwargs):
        raise RuntimeError('Woot with %s' % x)


class NastyTask(task.Task):

    def execute(self, **kwargs):
        pass

    def revert(self, **kwargs):
        raise RuntimeError('Gotcha!')


class NastyFailingTask(NastyTask):
    def execute(self, **kwargs):
        raise RuntimeError('Woot!')


class TaskNoRequiresNoReturns(task.Task):

    def execute(self, **kwargs):
        pass

    def revert(self, **kwargs):
        pass


class TaskOneArg(task.Task):

    def execute(self, x, **kwargs):
        pass

    def revert(self, x, **kwargs):
        pass


class TaskMultiArg(task.Task):

    def execute(self, x, y, z, **kwargs):
        pass

    def revert(self, x, y, z, **kwargs):
        pass


class TaskOneReturn(task.Task):

    def execute(self, **kwargs):
        return 1

    def revert(self, **kwargs):
        pass


class TaskMultiReturn(task.Task):

    def execute(self, **kwargs):
        return 1, 3, 5

    def revert(self, **kwargs):
        pass


class TaskOneArgOneReturn(task.Task):

    def execute(self, x, **kwargs):
        return 1

    def revert(self, x, **kwargs):
        pass


class TaskMultiArgOneReturn(task.Task):

    def execute(self, x, y, z, **kwargs):
        return x + y + z

    def revert(self, x, y, z, **kwargs):
        pass


class TaskMultiArgMultiReturn(task.Task):

    def execute(self, x, y, z, **kwargs):
        return 1, 3, 5

    def revert(self, x, y, z, **kwargs):
        pass


class TaskMultiDict(task.Task):

    def execute(self):
        output = {}
        for i, k in enumerate(sorted(self.provides)):
            output[k] = i
        return output


class NeverRunningTask(task.Task):
    def execute(self, **kwargs):
        assert False, 'This method should not be called'

    def revert(self, **kwargs):
        assert False, 'This method should not be called'


class EngineTestBase(object):
    values = None

    def setUp(self):
        super(EngineTestBase, self).setUp()
        EngineTestBase.values = []
        self.backend = impl_memory.MemoryBackend(conf={})

    def tearDown(self):
        EngineTestBase.values = None
        with contextlib.closing(self.backend) as be:
            with contextlib.closing(be.get_connection()) as conn:
                conn.clear_all()
        super(EngineTestBase, self).tearDown()

    def _make_engine(self, flow, **kwargs):
        raise exceptions.NotImplementedError("_make_engine() must be"
                                             " overridden if an engine is"
                                             " desired")


class FailureMatcher(object):
    """Needed for failure objects comparison."""

    def __init__(self, failure):
        self._failure = failure

    def __repr__(self):
        return str(self._failure)

    def __eq__(self, other):
        return self._failure.matches(other)


class OneReturnRetry(retry.AlwaysRevert):

    def execute(self, **kwargs):
        return 1

    def revert(self, **kwargs):
        pass


class ConditionalTask(SaveOrderTask):

    def execute(self, x, y):
        super(ConditionalTask, self).execute()
        if x != y:
            raise RuntimeError('Woot!')


class WaitForOneFromTask(SaveOrderTask):

    def __init__(self, name, wait_for, wait_states, **kwargs):
        super(WaitForOneFromTask, self).__init__(name, **kwargs)
        if isinstance(wait_for, six.string_types):
            self.wait_for = [wait_for]
        else:
            self.wait_for = wait_for
        if isinstance(wait_states, six.string_types):
            self.wait_states = [wait_states]
        else:
            self.wait_states = wait_states
        self.event = threading_utils.Event()

    def execute(self):
        if not self.event.wait(WAIT_TIMEOUT):
            raise RuntimeError('%s second timeout occurred while waiting '
                               'for %s to change state to %s'
                               % (WAIT_TIMEOUT, self.wait_for,
                                  self.wait_states))
        return super(WaitForOneFromTask, self).execute()

    def callback(self, state, details):
        name = details.get('task_name', None)
        if name not in self.wait_for or state not in self.wait_states:
            return
        self.event.set()


def make_many(amount, task_cls=DummyTask, offset=0):
    name_pool = string.ascii_lowercase + string.ascii_uppercase
    tasks = []
    while amount > 0:
        if offset >= len(name_pool):
            raise AssertionError('Name pool size to small (%s < %s)'
                                 % (len(name_pool), offset + 1))
        tasks.append(task_cls(name=name_pool[offset]))
        offset += 1
        amount -= 1
    return tasks
