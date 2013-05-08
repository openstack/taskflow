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


# Useful to know when other tasks are being activated and finishing.
STARTING = 'STARTING'
COMPLETED = 'COMPLETED'
ERRORED = 'ERRORED'


class Failure(object):
    """When a task failure occurs the following object will be given to revert
       and can be used to interrogate what caused the failure."""

    def __init__(self, task, name, workflow, exception):
        self.task = task
        self.name = name
        self.workflow = workflow
        self.exception = exception
