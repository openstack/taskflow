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

from oslo_utils import eventletutils as _eventletutils

# Give a nice warning that if eventlet is being used these modules
# are highly recommended to be patched (or otherwise bad things could
# happen).
_eventletutils.warn_eventlet_not_patched(
    expected_patched_modules=['time', 'thread'])


# Promote helpers to this module namespace (for easy access).
from taskflow.engines.helpers import flow_from_detail   # noqa
from taskflow.engines.helpers import load               # noqa
from taskflow.engines.helpers import load_from_detail   # noqa
from taskflow.engines.helpers import load_from_factory  # noqa
from taskflow.engines.helpers import run                # noqa
from taskflow.engines.helpers import save_factory_details  # noqa
