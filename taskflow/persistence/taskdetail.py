# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
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

from datetime import datetime

from taskflow.openstack.common import uuidutils
from taskflow.persistence.backends import api as b_api


class TaskDetail(object):
    def __init__(self, name, task, td_id=None):
        if td_id:
            self._uuid = td_id
        else:
            self._uuid = uuidutils.generate_uuid()

        self._name = name
        self.updated_at = datetime.now()
        self.state = None
        self.results = None
        self.exception = None
        self.stacktrace = None
        self.meta = None
        self.task = task

    def save(self):
        b_api.taskdetail_save(self)

    def delete(self):
        b_api.taskdetail_delete(self)

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name
