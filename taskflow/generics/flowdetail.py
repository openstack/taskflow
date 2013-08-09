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

from taskflow.backends import api as b_api
from taskflow.openstack.common import uuidutils


class FlowDetail(object):
    """Generic backend class that contains TaskDetail snapshots from when
    the FlowDetail is gotten. The data contained within this class is
    not backed by the backend storage in real time.
    The data in this class will only be persisted by the backend when
    the save() method is called.
    """
    def __init__(self, name, wf, fd_id=None):
        if fd_id:
            self._uuid = fd_id
        else:
            self._uuid = uuidutils.generate_uuid()
        self._name = name
        self._flow = wf
        self.updated_at = datetime.now()
        self._taskdetails = []

    def save(self):
        b_api.flowdetail_save(self)

    def delete(self):
        b_api.flowdetail_delete(self)

    def add_task_detail(self, td):
        self._taskdetails.append(td)

    def remove_task_detail(self, td):
        self._taskdetails = [d for d in self._taskdetails
                             if d.uuid != td.uuid]

    def __contains__(self, td):
        for self_td in self:
            if self_td.taskdetail_id == td.taskdetail_id:
                return True
        return False

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name

    @property
    def flow(self):
        return self._flow

    def __iter__(self):
        for td in self._taskdetails:
            yield td

    def __len__(self):
        return len(self._taskdetails)
