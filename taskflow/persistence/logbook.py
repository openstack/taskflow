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


class LogBook(object):
    """Generic backend class that contains TaskDetail snapshots from when
    the FlowDetail is gotten. The data contained within this class is
    not backed by the backend storage in real time.
    The data in this class will only be persisted by the backend when
    the save method is called.
    """
    def __init__(self, name, lb_id=None):
        if lb_id:
            self._uuid = lb_id
        else:
            self._uuid = uuidutils.generate_uuid()
        self._name = name
        self.updated_at = datetime.now()
        self._flowdetails = []

    def save(self):
        b_api.logbook_save(self)

    def delete(self):
        b_api.logbook_delete(self)

    def add_flow_detail(self, fd):
        self._flowdetails.append(fd)

    def remove_flow_detail(self, fd):
        self._flowdetails = [d for d in self._flowdetails
                             if d.uuid != fd.uuid]

    def __contains__(self, fd):
        for self_fd in self:
            if self_fd.flowdetail_id == fd.flowdetail_id:
                return True
        return False

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name

    def __iter__(self):
        for fd in self._flowdetails:
            yield fd

    def __len__(self):
        return len(self._flowdetails)
