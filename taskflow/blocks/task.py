
# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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

"""Terminal blocks that actually run code
"""

from taskflow.blocks import base
from taskflow.openstack.common import uuidutils


class Task(base.Block):
    """A block that wraps a single task

    The task should be executed, and produced results should be saved.
    """

    def __init__(self, task, uuid=None):
        super(Task, self).__init__()
        self._task = task
        if uuid is None:
            self._id = uuidutils.generate_uuid()
        else:
            self._id = str(uuid)

    @property
    def task(self):
        return self._task

    @property
    def uuid(self):
        return self._id
