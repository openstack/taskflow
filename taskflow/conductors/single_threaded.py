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

from taskflow.conductors.backends import impl_blocking
from taskflow.utils import deprecation

# TODO(harlowja): remove this module soon...
deprecation.removed_module(__name__,
                           replacement_name="the conductor entrypoints",
                           version="0.8", removal_version="?")

# TODO(harlowja): remove this proxy/legacy class soon...
SingleThreadedConductor = deprecation.moved_inheritable_class(
    impl_blocking.BlockingConductor, 'SingleThreadedConductor',
    __name__, version="0.8", removal_version="?")
