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

from concurrent import futures

from taskflow.utils import eventlet_utils as eu


def wait_for_any(fs, timeout=None):
    """Wait for one of the futures to complete.

    Works correctly with both green and non-green futures.

    Returns pair (done, not_done).
    """
    any_green = any(isinstance(f, eu.GreenFuture) for f in fs)
    if any_green:
        return eu.wait_for_any(fs, timeout=timeout)
    else:
        return tuple(futures.wait(fs, timeout=timeout,
                                  return_when=futures.FIRST_COMPLETED))


def make_completed_future(result):
    """Make with completed with given result."""
    future = futures.Future()
    future.set_result(result)
    return future
