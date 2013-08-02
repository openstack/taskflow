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


# TODO(kchenweijie): THIS ENTIRE CLASS


class JobBoard(object):
    def __init__(self, name, jb_id=None):
        if jb_id:
            self._uuid = jb_id
        else:
            self._uuid = uuidutils.generate_uuid()

        self._name = name
        self._jobs = []

    def add_job(self, job):
        self._jobs.append(job)

    def get_job(self, job_id):
        return self._jobs[job_id]

    def remove_job(self, job):
        self._jobs = [j for j in self._jobs
                      if j.uuid != job.uuid]

    def __contains__(self, job):
        for self_job in self.jobs:
            if self_job.job_id == job.job_id:
                return True
        return False

    @property
    def name(self):
        return self._name

    @property
    def uuid(self):
        return self._uuid

    @property
    def jobs(self):
        return self._jobs
