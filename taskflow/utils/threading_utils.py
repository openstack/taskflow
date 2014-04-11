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
import threading

import six

if six.PY2:
    from thread import get_ident  # noqa
else:
    # In python3+ the get_ident call moved (whhhy??)
    from threading import get_ident  # noqa


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
