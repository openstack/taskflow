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

import automaton
from automaton import exceptions as excp
from automaton import machines
from debtcollector import removals


# TODO(harlowja): remove me in a future version, since the futurist
# is the replacement for this whole module...
removals.removed_module(__name__,
                        replacement="the '%s' library" % automaton.__name__,
                        version="1.16", removal_version='2.0',
                        stacklevel=4)


# Keep alias classes/functions... around until this module is removed.
FSM = machines.FiniteMachine
FrozenMachine = excp.FrozenMachine
NotInitialized = excp.NotInitialized
InvalidState = excp.InvalidState
NotFound = excp.NotFound
Duplicate = excp.Duplicate
