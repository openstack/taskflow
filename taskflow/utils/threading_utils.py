# -*- coding: utf-8 -*-

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

import _thread
import collections
import multiprocessing
import threading

from taskflow.utils import misc


def is_alive(thread):
    """Helper to determine if a thread is alive (handles none safely)."""
    if not thread:
        return False
    return thread.is_alive()


def get_ident():
    """Return the 'thread identifier' of the current thread."""
    return _thread.get_ident()


def get_optimal_thread_count(default=2):
    """Try to guess optimal thread count for current system."""
    try:
        return multiprocessing.cpu_count() + 1
    except NotImplementedError:
        # NOTE(harlowja): apparently may raise so in this case we will
        # just setup two threads since it's hard to know what else we
        # should do in this situation.
        return default


def daemon_thread(target, *args, **kwargs):
    """Makes a daemon thread that calls the given target when started."""
    thread = threading.Thread(target=target, args=args, kwargs=kwargs)
    # NOTE(skudriashev): When the main thread is terminated unexpectedly
    # and thread is still alive - it will prevent main thread from exiting
    # unless the daemon property is set to True.
    thread.daemon = True
    return thread


# Container for thread creator + associated callbacks.
_ThreadBuilder = collections.namedtuple('_ThreadBuilder',
                                        ['thread_factory',
                                         'before_start', 'after_start',
                                         'before_join', 'after_join'])
_ThreadBuilder.fields = tuple([
    'thread_factory',
    'before_start',
    'after_start',
    'before_join',
    'after_join',
])


def no_op(*args, **kwargs):
    """Function that does nothing."""


class ThreadBundle(object):
    """A group/bundle of threads that start/stop together."""

    def __init__(self):
        self._threads = []
        self._lock = threading.Lock()

    def bind(self, thread_factory,
             before_start=None, after_start=None,
             before_join=None, after_join=None):
        """Adds a thread (to-be) into this bundle (with given callbacks).

        NOTE(harlowja): callbacks provided should not attempt to call
                        mutating methods (:meth:`.stop`, :meth:`.start`,
                        :meth:`.bind` ...) on this object as that will result
                        in dead-lock since the lock on this object is not
                        meant to be (and is not) reentrant...
        """
        if before_start is None:
            before_start = no_op
        if after_start is None:
            after_start = no_op
        if before_join is None:
            before_join = no_op
        if after_join is None:
            after_join = no_op
        builder = _ThreadBuilder(thread_factory,
                                 before_start, after_start,
                                 before_join, after_join)
        for attr_name in builder.fields:
            cb = getattr(builder, attr_name)
            if not callable(cb):
                raise ValueError("Provided callback for argument"
                                 " '%s' must be callable" % attr_name)
        with self._lock:
            self._threads.append([
                builder,
                # The built thread.
                None,
                # Whether the built thread was started (and should have
                # ran or still be running).
                False,
            ])

    def start(self):
        """Creates & starts all associated threads (that are not running)."""
        count = 0
        with self._lock:
            it = enumerate(self._threads)
            for i, (builder, thread, started) in it:
                if thread and started:
                    continue
                if not thread:
                    self._threads[i][1] = thread = builder.thread_factory()
                builder.before_start(thread)
                thread.start()
                count += 1
                try:
                    builder.after_start(thread)
                finally:
                    # Just incase the 'after_start' callback blows up make sure
                    # we always set this...
                    self._threads[i][2] = started = True
        return count

    def stop(self):
        """Stops & joins all associated threads (that have been started)."""
        count = 0
        with self._lock:
            it = misc.reverse_enumerate(self._threads)
            for i, (builder, thread, started) in it:
                if not thread or not started:
                    continue
                builder.before_join(thread)
                thread.join()
                count += 1
                try:
                    builder.after_join(thread)
                finally:
                    # Just incase the 'after_join' callback blows up make sure
                    # we always set/reset these...
                    self._threads[i][1] = thread = None
                    self._threads[i][2] = started = False
        return count

    def __len__(self):
        """Returns how many threads (to-be) are in this bundle."""
        return len(self._threads)
