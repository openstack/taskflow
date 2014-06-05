# -*- coding: utf-8 -*-

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

from taskflow.engines.action_engine import executor as ex
from taskflow import exceptions
from taskflow import states
from taskflow.utils import async_utils
from taskflow.utils import misc

LOG = logging.getLogger(__name__)

SAVE_RESULT_STATES = (states.SUCCESS, states.FAILURE)


class RetryAction(object):
    def __init__(self, storage, notifier):
        self._storage = storage
        self._notifier = notifier

    def _get_retry_args(self, retry):
        kwargs = self._storage.fetch_mapped_args(retry.rebind,
                                                 atom_name=retry.name)
        kwargs['history'] = self._storage.get_retry_history(retry.name)
        return kwargs

    def change_state(self, retry, state, result=None):
        old_state = self._storage.get_atom_state(retry.name)
        if old_state == state:
            return state != states.PENDING
        if state in SAVE_RESULT_STATES:
            self._storage.save(retry.name, result, state)
        elif state == states.REVERTED:
            self._storage.cleanup_retry_history(retry.name, state)
        else:
            self._storage.set_atom_state(retry.name, state)
        retry_uuid = self._storage.get_atom_uuid(retry.name)
        details = dict(retry_name=retry.name,
                       retry_uuid=retry_uuid,
                       result=result)
        self._notifier.notify(state, details)
        return True

    def execute(self, retry):
        if not self.change_state(retry, states.RUNNING):
            raise exceptions.InvalidState("Retry controller %s is in invalid "
                                          "state and can't be executed" %
                                          retry.name)
        kwargs = self._get_retry_args(retry)
        try:
            result = retry.execute(**kwargs)
        except Exception:
            result = misc.Failure()
            self.change_state(retry, states.FAILURE, result=result)
        else:
            self.change_state(retry, states.SUCCESS, result=result)
        return async_utils.make_completed_future((retry, ex.EXECUTED, result))

    def revert(self, retry):
        if not self.change_state(retry, states.REVERTING):
            raise exceptions.InvalidState("Retry controller %s is in invalid "
                                          "state and can't be reverted" %
                                          retry.name)
        kwargs = self._get_retry_args(retry)
        kwargs['flow_failures'] = self._storage.get_failures()
        try:
            result = retry.revert(**kwargs)
        except Exception:
            result = misc.Failure()
            self.change_state(retry, states.FAILURE)
        else:
            self.change_state(retry, states.REVERTED)
        return async_utils.make_completed_future((retry, ex.REVERTED, result))

    def on_failure(self, retry, atom, last_failure):
        self._storage.save_retry_failure(retry.name, atom.name, last_failure)
        kwargs = self._get_retry_args(retry)
        return retry.on_failure(**kwargs)
