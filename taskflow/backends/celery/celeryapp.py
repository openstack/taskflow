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

import logging
import traceback as tb

from celery.signals import task_failure
from celery.signals import task_success

LOG = logging.getLogger(__name__)


@task_failure.connect
def task_error_handler(signal=None, sender=None, task_id=None,
                       exception=None, args=None, kwargs=None,
                       traceback=None, einfo=None):
    """ If a task errors out, log all error info """
    LOG.error('Task %s, id: %s, called with args: %s, and kwargs: %s'
              'failed with exception: %s' % (sender.name, task_id,
                                             args, kwargs, exception))
    LOG.error('Trackeback: %s' % (tb.print_tb(traceback), ))
    wf = sender.name.split('.')[0]
    task = ('.').join(n for n in (sender.name.split('.')[1:]) if n)
    # TODO(jlucci): Auto-initiate rollback from failed task


@task_success.connect
def task_success_handler(singal=None, sender=None, result=None):
    """ Save task results to WF """
    wf = sender.name.split('.')[0]
    task = ('.').join(n for n in (sender.name.split('.')[1:]) if n)
