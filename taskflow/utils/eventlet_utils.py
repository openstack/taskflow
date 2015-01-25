# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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

from oslo_utils import importutils

_eventlet = importutils.try_import('eventlet')

EVENTLET_AVAILABLE = bool(_eventlet)


def check_for_eventlet(exc=None):
    """Check if eventlet is available and if not raise a runtime error.

    :param exc: exception to raise instead of raising a runtime error
    :type exc: exception
    """
    if not EVENTLET_AVAILABLE:
        if exc is None:
            raise RuntimeError('Eventlet is not current available')
        else:
            raise exc
