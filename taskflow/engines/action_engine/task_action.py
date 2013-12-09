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

import contextlib
import logging

from taskflow.openstack.common import excutils
from taskflow import states
from taskflow.utils import misc

LOG = logging.getLogger(__name__)

SAVE_RESULT_STATES = (states.SUCCESS, states.FAILURE)


@contextlib.contextmanager
def _autobind(task, bind_name, bind_func, **kwargs):
    try:
        task.bind(bind_name, bind_func, **kwargs)
        yield task
    finally:
        task.unbind(bind_name, bind_func)


class TaskAction(object):
    def __init__(self, storage, notifier):
        self._storage = storage
        self._notifier = notifier

    def _change_state(self, task, state, result=None, progress=None):
        old_state = self._storage.get_task_state(task.name)
        if not states.check_task_transition(old_state, state):
            return False
        if state in SAVE_RESULT_STATES:
            self._storage.save(task.name, result, state)
        else:
            self._storage.set_task_state(task.name, state)
        if progress is not None:
            self._storage.set_task_progress(task.name, progress)

        task_uuid = self._storage.get_task_uuid(task.name)
        details = dict(task_name=task.name,
                       task_uuid=task_uuid,
                       result=result)
        self._notifier.notify(state, details)
        if progress is not None:
            task.update_progress(progress)
        return True

    def _on_update_progress(self, task, event_data, progress, **kwargs):
        """Should be called when task updates its progress"""
        try:
            self._storage.set_task_progress(task.name, progress, kwargs)
        except Exception:
            # Update progress callbacks should never fail, so capture and log
            # the emitted exception instead of raising it.
            LOG.exception("Failed setting task progress for %s to %0.3f",
                          task, progress)

    def execute(self, task):
        if not self._change_state(task, states.RUNNING, progress=0.0):
            return
        with _autobind(task, 'update_progress', self._on_update_progress):
            try:
                kwargs = self._storage.fetch_mapped_args(task.rebind)
                result = task.execute(**kwargs)
            except Exception:
                failure = misc.Failure()
                self._change_state(task, states.FAILURE, result=failure)
                failure.reraise()

        self._change_state(task, states.SUCCESS, result=result, progress=1.0)

    def revert(self, task):
        if not self._change_state(task, states.REVERTING, progress=0.0):
            return
        with _autobind(task, 'update_progress', self._on_update_progress):
            kwargs = self._storage.fetch_mapped_args(task.rebind)
            kwargs['result'] = self._storage.get(task.name)
            kwargs['flow_failures'] = self._storage.get_failures()
            try:
                task.revert(**kwargs)
            except Exception:
                with excutils.save_and_reraise_exception():
                    self._change_state(task, states.FAILURE)
        self._change_state(task, states.REVERTED, progress=1.0)
