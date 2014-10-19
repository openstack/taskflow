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

import multiprocessing
import sys
import threading

from six.moves import _thread


if sys.version_info[0:2] == (2, 6):
    # This didn't return that was/wasn't set in 2.6, since we actually care
    # whether it did or didn't add that feature by taking the code from 2.7
    # that added this functionality...
    #
    # TODO(harlowja): remove when we can drop 2.6 support.
    class Event(threading._Event):
        def wait(self, timeout=None):
            self.__cond.acquire()
            try:
                if not self.__flag:
                    self.__cond.wait(timeout)
                return self.__flag
            finally:
                self.__cond.release()
else:
    Event = threading.Event


def get_ident():
    """Return the 'thread identifier' of the current thread."""
    return _thread.get_ident()


def get_optimal_thread_count():
    """Try to guess optimal thread count for current system."""
    try:
        return multiprocessing.cpu_count() + 1
    except NotImplementedError:
        # NOTE(harlowja): apparently may raise so in this case we will
        # just setup two threads since it's hard to know what else we
        # should do in this situation.
        return 2


def daemon_thread(target, *args, **kwargs):
    """Makes a daemon thread that calls the given target when started."""
    thread = threading.Thread(target=target, args=args, kwargs=kwargs)
    # NOTE(skudriashev): When the main thread is terminated unexpectedly
    # and thread is still alive - it will prevent main thread from exiting
    # unless the daemon property is set to True.
    thread.daemon = True
    return thread
