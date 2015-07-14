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

from debtcollector import moves
from debtcollector import removals

from taskflow.conductors.backends import impl_blocking

# TODO(harlowja): remove this module soon...
removals.removed_module(__name__,
                        replacement="the conductor entrypoints",
                        version="0.8", removal_version="2.0",
                        stacklevel=4)

# TODO(harlowja): remove this proxy/legacy class soon...
SingleThreadedConductor = moves.moved_class(
    impl_blocking.BlockingConductor, 'SingleThreadedConductor',
    __name__, version="0.8", removal_version="2.0")
