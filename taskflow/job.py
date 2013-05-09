# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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

import abc
import uuid

CLAIMED = 'claimed'
UNCLAIMED = 'unclaimed'


class Job(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name, type, context):
        self.name = name
        # TBD - likely more details about this job
        self.details = None
        self.state = UNCLAIMED
        self.owner = None
        self.tracking_id = str(uuid.uuid4())
        self.context = context

    def uri(self):
        return "%s://%s/%s" % (self.type, self.name,
                               self.tracking_id)

    @abc.abstractproperty
    def type(self):
        # Returns which type of job this is.
        #
        # For example, a 'run_instance' job, or a 'delete_instance' job could
        # be possible types.
        raise NotImplementedError()

    @abc.abstractmethod
    def claim(self, owner):
        # This can be used to transition this job from unclaimed to claimed.
        #
        # This must be done in a way that likely uses some type of locking or
        # ownership transfer so that only a single entity gets this job to work
        # on. This will avoid multi-job ownership, which can lead to
        # inconsistent state.
        raise NotImplementedError()

    @abc.abstractmethod
    def consume(self):
        # This can be used to transition this job from active to finished.
        #
        # During said transition the job and any details of it may be removed
        # from some backing storage (if applicable).
        raise NotImplementedError()
