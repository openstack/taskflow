# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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


from taskflow import exceptions


class Storage(object):
    """Manages task results"""

    # TODO(imelnikov): this should be implemented on top of logbook

    def __init__(self):
        self._task_results = {}

    def save(self, uuid, data):
        """Put result for task with id 'uuid' to storage"""
        self._task_results[uuid] = data

    def get(self, uuid):
        """Get result for task with id 'uuid' to storage"""
        try:
            return self._task_results[uuid]
        except KeyError:
            raise exceptions.NotFound("Result for task %r is not known"
                                      % uuid)

    def reset(self, uuid):
        """Remove result for task with id 'uuid' from storage"""
        try:
            del self._task_results[uuid]
        except KeyError:
            pass
