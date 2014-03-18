# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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

from taskflow import states
from taskflow.utils import misc

LOG = logging.getLogger(__name__)

SAVE_RESULT_STATES = (states.SUCCESS, states.FAILURE)


class RetryAction(object):
    def __init__(self, storage, notifier):
        self._storage = storage
        self._notifier = notifier

    def _get_retry_args(self, retry):
        kwargs = self._storage.fetch_mapped_args(retry.rebind)
        kwargs['history'] = self._storage.get_retry_history(retry.name)
        return kwargs

    def _change_state(self, retry, state, result=None):
        old_state = self._storage.get_task_state(retry.name)
        if not states.check_task_transition(old_state, state):
            return False
        if state in SAVE_RESULT_STATES:
            self._storage.save(retry.name, result, state)
        elif state == states.REVERTED:
            self.storage.cleanup_retry_history(retry.name, state)
        else:
            self._storage.set_task_state(retry.name, state)

        retry_uuid = self._storage.get_task_uuid(retry.name)
        details = dict(retry_name=retry.name,
                       retry_uuid=retry_uuid,
                       result=result)
        self._notifier.notify(state, details)
        return True

    def execute(self, retry):
        if not self._change_state(retry, states.RUNNING):
            return
        kwargs = self._get_retry_args(retry)
        try:
            result = retry.execute(**kwargs)
        except Exception:
            result = misc.Failure()
            self._change_state(retry, states.FAILURE, result=result)
        else:
            self._change_state(retry, states.SUCCESS, result=result)

    def revert(self, retry):
        if not self._change_state(retry, states.REVERTING):
            return
        kwargs = self._get_retry_args(retry)
        kwargs['flow_failures'] = self._storage.get_failures()
        try:
            retry.revert(**kwargs)
        except Exception:
            self._change_state(retry, states.FAILURE)
        else:
            self._change_state(retry, states.REVERTED)