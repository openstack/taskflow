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

from taskflow.engines.action_engine.actions import base
from taskflow import logging
from taskflow import retry as retry_atom
from taskflow import states
from taskflow.types import failure

LOG = logging.getLogger(__name__)


class RetryAction(base.Action):
    """An action that handles executing, state changes, ... of retry atoms."""

    def __init__(self, storage, notifier, retry_executor):
        super(RetryAction, self).__init__(storage, notifier)
        self._retry_executor = retry_executor

    def _get_retry_args(self, retry, revert=False, addons=None):
        if revert:
            arguments = self._storage.fetch_mapped_args(
                retry.revert_rebind,
                atom_name=retry.name,
                optional_args=retry.revert_optional
            )
        else:
            arguments = self._storage.fetch_mapped_args(
                retry.rebind,
                atom_name=retry.name,
                optional_args=retry.optional
            )
        history = self._storage.get_retry_history(retry.name)
        arguments[retry_atom.EXECUTE_REVERT_HISTORY] = history
        if addons:
            arguments.update(addons)
        return arguments

    def change_state(self, retry, state, result=base.Action.NO_RESULT):
        old_state = self._storage.get_atom_state(retry.name)
        if state in self.SAVE_RESULT_STATES:
            save_result = None
            if result is not self.NO_RESULT:
                save_result = result
            self._storage.save(retry.name, save_result, state)
            # TODO(harlowja): combine this with the save to avoid a call
            # back into the persistence layer...
            if state == states.REVERTED:
                self._storage.cleanup_retry_history(retry.name, state)
        else:
            if state == old_state:
                # NOTE(imelnikov): nothing really changed, so we should not
                # write anything to storage and run notifications.
                return
            self._storage.set_atom_state(retry.name, state)
        retry_uuid = self._storage.get_atom_uuid(retry.name)
        details = {
            'retry_name': retry.name,
            'retry_uuid': retry_uuid,
            'old_state': old_state,
        }
        if result is not self.NO_RESULT:
            details['result'] = result
        self._notifier.notify(state, details)

    def schedule_execution(self, retry):
        self.change_state(retry, states.RUNNING)
        return self._retry_executor.execute_retry(
            retry, self._get_retry_args(retry))

    def complete_reversion(self, retry, result):
        if isinstance(result, failure.Failure):
            self.change_state(retry, states.REVERT_FAILURE, result=result)
        else:
            self.change_state(retry, states.REVERTED, result=result)

    def complete_execution(self, retry, result):
        if isinstance(result, failure.Failure):
            self.change_state(retry, states.FAILURE, result=result)
        else:
            self.change_state(retry, states.SUCCESS, result=result)

    def schedule_reversion(self, retry):
        self.change_state(retry, states.REVERTING)
        arg_addons = {
            retry_atom.REVERT_FLOW_FAILURES: self._storage.get_failures(),
        }
        return self._retry_executor.revert_retry(
            retry, self._get_retry_args(retry, addons=arg_addons, revert=True))

    def on_failure(self, retry, atom, last_failure):
        self._storage.save_retry_failure(retry.name, atom.name, last_failure)
        arguments = self._get_retry_args(retry)
        return retry.on_failure(**arguments)
