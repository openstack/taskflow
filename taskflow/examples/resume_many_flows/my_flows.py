# -*- coding: utf-8 -*-

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

import os

from taskflow.patterns import linear_flow as lf
from taskflow import task


class UnfortunateTask(task.Task):
    def execute(self):
        print('executing %s' % self)
        boom = os.environ.get('BOOM')
        if boom:
            print('> Critical error: boom = %s' % boom)
            raise SystemExit()
        else:
            print('> this time not exiting')


class TestTask(task.Task):
    def execute(self):
        print('executing %s' % self)


def flow_factory():
    return lf.Flow('example').add(
        TestTask(name='first'),
        UnfortunateTask(name='boom'),
        TestTask(name='second'))
