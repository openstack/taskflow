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

# from taskflow.backends import api as b_api
from taskflow.openstack.common import uuidutils


class Job(object):
    def __init__(self, name, uuid=None):
        if uuid:
            self._uuid = uuid
        else:
            self._uuid = uuidutils.generate_uuid()

        self._name = name
        self.owner = None
        self.state = None
        self._flows = []
        self.logbook = None

    def add_flow(self, wf):
        self._flows.append(wf)

    def remove_flow(self, wf):
        self._flows = [f for f in self._flows
                       if f.uuid != wf.uuid]

    def __contains__(self, wf):
        for self_wf in self.flows:
            if self_wf.flow_id == wf.flow_id:
                return True
        return False

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name

    @property
    def flows(self):
        return self._flows
