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
import types

from taskflow.utils import lock_utils

LOG = logging.getLogger(__name__)


def get_optimal_thread_count():
    """Try to guess optimal thread count for current system."""
    try:
        return multiprocessing.cpu_count() + 1
    except NotImplementedError:
        # NOTE(harlowja): apparently may raise so in this case we will
        # just setup two threads since its hard to know what else we
        # should do in this situation.
        return 2


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
