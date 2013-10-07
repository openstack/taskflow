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

import threading

from concurrent import futures

from taskflow.engines.action_engine import graph_action
from taskflow.engines.action_engine import task_action
from taskflow.engines import base

from taskflow import exceptions as exc
from taskflow.openstack.common import excutils
from taskflow import states
from taskflow import storage as t_storage

from taskflow.utils import flow_utils
from taskflow.utils import lock_utils
from taskflow.utils import misc
from taskflow.utils import threading_utils


class ActionEngine(base.EngineBase):
    """Generic action-based engine.

    Converts the flow to recursive structure of actions.
    """
    _graph_action = None

    def __init__(self, flow, flow_detail, backend, conf):
        super(ActionEngine, self).__init__(flow, flow_detail, backend, conf)
        self._failures = []
        self._root = None
        self._lock = threading.RLock()
        self._state_lock = threading.RLock()
        self.notifier = misc.TransitionNotifier()
        self.task_notifier = misc.TransitionNotifier()

    def _revert(self, current_failure=None):
        self._change_state(states.REVERTING)
        try:
            state = self._root.revert(self)
        except Exception:
            with excutils.save_and_reraise_exception():
                self._change_state(states.FAILURE)

        self._change_state(state)
        if state == states.SUSPENDED:
            return
        misc.Failure.reraise_if_any(self._failures)
        if current_failure:
            current_failure.reraise()

    def _reset(self):
        self._failures = []

    def suspend(self):
        self._change_state(states.SUSPENDING)

    def get_graph(self):
        self.compile()
        return self._root.graph

    @lock_utils.locked
    def run(self):
        if self.storage.get_flow_state() != states.SUSPENDED:
            self.compile()
            self._reset()

            external_provides = set(self.storage.fetch_all().keys())
            missing = self._flow.requires - external_provides
            if missing:
                raise exc.MissingDependencies(self._flow, sorted(missing))
            self._run()
        elif self._failures:
            self._revert()
        else:
            self._run()

    def _run(self):
        self._change_state(states.RUNNING)
        try:
            state = self._root.execute(self)
        except Exception:
            self._change_state(states.FAILURE)
            self._revert(misc.Failure())
        else:
            self._change_state(state)

    @lock_utils.locked(lock='_state_lock')
    def _change_state(self, state):
        old_state = self.storage.get_flow_state()
        if not states.check_flow_transition(old_state, state):
            return
        self.storage.set_flow_state(state)
        details = dict(engine=self, old_state=old_state)
        self.notifier.notify(state, details)

    def on_task_state_change(self, task_action, state, result=None):
        if isinstance(result, misc.Failure):
            self._failures.append(result)
        details = dict(engine=self,
                       task_name=task_action.name,
                       task_uuid=task_action.uuid,
                       result=result)
        self.task_notifier.notify(state, details)

    def _translate_flow_to_action(self):
        assert self._graph_action is not None, ('Graph action class must be'
                                                ' specified')
        task_graph = flow_utils.flatten(self._flow)
        ga = self._graph_action(task_graph)
        for n in task_graph.nodes_iter():
            ga.add(n, task_action.TaskAction(n, self))
        return ga

    def compile(self):
        if self._root is None:
            self._root = self._translate_flow_to_action()

    @property
    def is_running(self):
        return self.storage.get_flow_state() == states.RUNNING

    @property
    def is_reverting(self):
        return self.storage.get_flow_state() == states.REVERTING


class SingleThreadedActionEngine(ActionEngine):
    # This one attempts to run in a serial manner.
    _graph_action = graph_action.SequentialGraphAction
    _storage_cls = t_storage.Storage


class MultiThreadedActionEngine(ActionEngine):
    # This one attempts to run in a parallel manner.
    _graph_action = graph_action.ParallelGraphAction
    _storage_cls = t_storage.ThreadSafeStorage

    def __init__(self, flow, flow_detail, backend, conf):
        super(MultiThreadedActionEngine, self).__init__(
            flow, flow_detail, backend, conf)
        self._executor = conf.get('executor', None)

    @lock_utils.locked
    def run(self):
        if self._executor is None:
            self._executor = futures.ThreadPoolExecutor(
                threading_utils.get_optimal_thread_count())
            owns_executor = True
        else:
            owns_executor = False

        try:
            ActionEngine.run(self)
        finally:
            # Don't forget to shutdown the executor!!
            if owns_executor:
                try:
                    self._executor.shutdown(wait=True)
                finally:
                    self._executor = None

    @property
    def executor(self):
        return self._executor
