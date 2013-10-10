# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

import logging
import multiprocessing
import six
import threading
import time
import types

from taskflow.utils import lock_utils

LOG = logging.getLogger(__name__)


def await(check_functor, timeout=None):
    """Spin-loops + sleeps awaiting for a given functor to return true."""
    if timeout is not None:
        end_time = time.time() + max(0, timeout)
    else:
        end_time = None
    # Use the same/similar scheme that the python condition class uses.
    delay = 0.0005
    while not check_functor():
        time.sleep(delay)
        if end_time is not None:
            remaining = end_time - time.time()
            if remaining <= 0:
                return False
            delay = min(delay * 2, remaining, 0.05)
        else:
            delay = min(delay * 2, 0.05)
    return True


def get_optimal_thread_count():
    """Try to guess optimal thread count for current system."""
    try:
        return multiprocessing.cpu_count() + 1
    except NotImplementedError:
        # NOTE(harlowja): apparently may raise so in this case we will
        # just setup two threads since its hard to know what else we
        # should do in this situation.
        return 2


class CountDownLatch(object):
    """Similar in concept to the java count down latch."""

    def __init__(self, count=0):
        self.count = count
        self.lock = threading.Condition()

    def countDown(self):
        with self.lock:
            self.count -= 1
            if self.count <= 0:
                self.lock.notifyAll()

    def await(self, timeout=None):
        end_time = None
        if timeout is not None:
            timeout = max(0, timeout)
            end_time = time.time() + timeout
        time_up = False
        with self.lock:
            while True:
                # Stop waiting on these 2 conditions.
                if time_up or self.count <= 0:
                    break
                # Was this a spurious wakeup or did we really end??
                self.lock.wait(timeout=timeout)
                if end_time is not None:
                    if time.time() >= end_time:
                        time_up = True
                    else:
                        # Reduce the timeout so that we don't wait extra time
                        # over what we initially were requested to.
                        timeout = end_time - time.time()
            return self.count <= 0


class ThreadSafeMeta(type):
    """Metaclass that adds locking to all pubic methods of a class"""

    def __new__(cls, name, bases, attrs):
        for attr_name, attr_value in six.iteritems(attrs):
            if isinstance(attr_value, types.FunctionType):
                if attr_name[0] != '_':
                    attrs[attr_name] = lock_utils.locked(attr_value)
        return super(ThreadSafeMeta, cls).__new__(cls, name, bases, attrs)

    def __call__(cls, *args, **kwargs):
        instance = super(ThreadSafeMeta, cls).__call__(*args, **kwargs)
        if not hasattr(instance, '_lock'):
            instance._lock = threading.RLock()
        return instance
