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
from taskflow import retry as rt
from taskflow import states
from taskflow.types import failure
from taskflow.types import futures

LOG = logging.getLogger(__name__)

SAVE_RESULT_STATES = (states.SUCCESS, states.FAILURE)


class RetryAction(object):
    def __init__(self, storage, notifier, walker_factory):
        self._storage = storage
        self._notifier = notifier
        self._walker_factory = walker_factory
        self._executor = futures.SynchronousExecutor()

    def _get_retry_args(self, retry, addons=None):
        scope_walker = self._walker_factory(retry)
        kwargs = self._storage.fetch_mapped_args(retry.rebind,
                                                 atom_name=retry.name,
                                                 scope_walker=scope_walker)
        history = self._storage.get_retry_history(retry.name)
        kwargs[rt.EXECUTE_REVERT_HISTORY] = history
        if addons:
            kwargs.update(addons)
        return kwargs

    def change_state(self, retry, state, result=None):
        if state in SAVE_RESULT_STATES:
            self._storage.save(retry.name, result, state)
        elif state == states.REVERTED:
            self._storage.cleanup_retry_history(retry.name, state)
        else:
            old_state = self._storage.get_atom_state(retry.name)
            if state == old_state:
                # NOTE(imelnikov): nothing really changed, so we should not
                # write anything to storage and run notifications
                return
            self._storage.set_atom_state(retry.name, state)
        retry_uuid = self._storage.get_atom_uuid(retry.name)
        details = dict(retry_name=retry.name,
                       retry_uuid=retry_uuid,
                       result=result)
        self._notifier.notify(state, details)

    def execute(self, retry):

        def _execute_retry(kwargs):
            try:
                result = retry.execute(**kwargs)
            except Exception:
                result = failure.Failure()
            return (retry, ex.EXECUTED, result)

        def _on_done_callback(fut):
            result = fut.result()[-1]
            if isinstance(result, failure.Failure):
                self.change_state(retry, states.FAILURE, result=result)
            else:
                self.change_state(retry, states.SUCCESS, result=result)

        self.change_state(retry, states.RUNNING)
        fut = self._executor.submit(_execute_retry,
                                    self._get_retry_args(retry))
        fut.add_done_callback(_on_done_callback)
        return fut

    def revert(self, retry):

        def _execute_retry(kwargs):
            try:
                result = retry.revert(**kwargs)
            except Exception:
                result = failure.Failure()
            return (retry, ex.REVERTED, result)

        def _on_done_callback(fut):
            result = fut.result()[-1]
            if isinstance(result, failure.Failure):
                self.change_state(retry, states.FAILURE)
            else:
                self.change_state(retry, states.REVERTED)

        self.change_state(retry, states.REVERTING)
        arg_addons = {
            rt.REVERT_FLOW_FAILURES: self._storage.get_failures(),
        }
        fut = self._executor.submit(_execute_retry,
                                    self._get_retry_args(retry,
                                                         addons=arg_addons))
        fut.add_done_callback(_on_done_callback)
        return fut

    def on_failure(self, retry, atom, last_failure):
        self._storage.save_retry_failure(retry.name, atom.name, last_failure)
        kwargs = self._get_retry_args(retry)
        return retry.on_failure(**kwargs)
