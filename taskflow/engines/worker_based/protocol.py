# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

from taskflow.engines.action_engine import executor

# NOTE(skudriashev): This is protocol events, not related to the task states.
PENDING = 'PENDING'
RUNNING = 'RUNNING'
SUCCESS = 'SUCCESS'
FAILURE = 'FAILURE'
PROGRESS = 'PROGRESS'

# Remote task actions
EXECUTE = 'execute'
REVERT = 'revert'

# Remote task action to event map
ACTION_TO_EVENT = {
    EXECUTE: executor.EXECUTED,
    REVERT: executor.REVERTED
}

# NOTE(skudriashev): A timeout which specifies request expiration period.
REQUEST_TIMEOUT = 60

# NOTE(skudriashev): A timeout which controls for how long a queue can be
# unused before it is automatically deleted. Unused means the queue has no
# consumers, the queue has not been redeclared, the `queue.get` has not been
# invoked for a duration of at least the expiration period. In our case this
# period is equal to the request timeout, once request is expired - queue is
# no longer needed.
QUEUE_EXPIRE_TIMEOUT = REQUEST_TIMEOUT
