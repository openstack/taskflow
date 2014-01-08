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

from taskflow.engines.action_engine import executor
from taskflow.engines.action_engine import graph_action
from taskflow.engines.action_engine import graph_analyzer
from taskflow.engines.action_engine import task_action
from taskflow.engines import base

from taskflow import exceptions as exc
from taskflow.openstack.common import excutils
from taskflow import states
from taskflow import storage as t_storage

from taskflow.utils import flow_utils
from taskflow.utils import lock_utils
from taskflow.utils import misc
from taskflow.utils import reflection


class ActionEngine(base.EngineBase):
    """Generic action-based engine.

    This engine flattens the flow (and any subflows) into a execution graph
    which contains the full runtime definition to be executed and then uses
    this graph in combination with the action classes & storage to attempt to
    run your flow (and any subflows & contained tasks) to completion.

    During this process it is permissible and valid to have a task or multiple
    tasks in the execution graph fail, which will cause the process of
    reversion to commence. See the valid states in the states module to learn
    more about what other states the tasks & flow being ran can go through.
    """
    _graph_action_cls = graph_action.FutureGraphAction
    _graph_analyzer_cls = graph_analyzer.GraphAnalyzer
    _task_action_cls = task_action.TaskAction
    _task_executor_cls = executor.SerialTaskExecutor

    def __init__(self, flow, flow_detail, backend, conf):
        super(ActionEngine, self).__init__(flow, flow_detail, backend, conf)
        self._analyzer = None
        self._root = None
        self._lock = threading.RLock()
        self._state_lock = threading.RLock()
        self.notifier = misc.TransitionNotifier()
        self.task_notifier = misc.TransitionNotifier()
        self._task_executor = self._task_executor_cls()
        self._task_action = self._task_action_cls(self.storage,
                                                  self._task_executor,
                                                  self.task_notifier)

    def _revert(self, current_failure=None):
        self._change_state(states.REVERTING)
        try:
            state = self._root.revert()
        except Exception:
            with excutils.save_and_reraise_exception():
                self._change_state(states.FAILURE)

        self._change_state(state)
        if state == states.SUSPENDED:
            return
        failures = self.storage.get_failures()
        misc.Failure.reraise_if_any(failures.values())
        if current_failure:
            current_failure.reraise()

    def __str__(self):
        return "%s: %s" % (reflection.get_class_name(self), id(self))

    def suspend(self):
        """Attempts to suspend the engine.

        If the engine is currently running tasks then this will attempt to
        suspend future work from being started (currently active tasks can
        not currently be preempted) and move the engine into a suspend state
        which can then later be resumed from.
        """
        self._change_state(states.SUSPENDING)

    @property
    def execution_graph(self):
        self.compile()
        return self._analyzer.execution_graph

    @lock_utils.locked
    def run(self):
        """Runs the flow in the engine to completion."""
        if self.storage.get_flow_state() == states.REVERTED:
            self._reset()
        self.compile()
        external_provides = set(self.storage.fetch_all().keys())
        missing = self._flow.requires - external_provides
        if missing:
            raise exc.MissingDependencies(self._flow, sorted(missing))
        self._task_executor.start()
        try:
            if self.storage.has_failures():
                self._revert()
            else:
                self._run()
        finally:
            self._task_executor.stop()

    def _run(self):
        self._change_state(states.RUNNING)
        try:
            state = self._root.execute()
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
        try:
            flow_uuid = self._flow.uuid
        except AttributeError:
            # NOTE(harlowja): if the flow was just a single task, then it will
            # not itself have a uuid, but the constructed flow_detail will.
            if self._flow_detail is not None:
                flow_uuid = self._flow_detail.uuid
            else:
                flow_uuid = None
        details = dict(engine=self,
                       flow_name=self._flow.name,
                       flow_uuid=flow_uuid,
                       old_state=old_state)
        self.notifier.notify(state, details)

    def _reset(self):
        for name, uuid in self.storage.reset_tasks():
            details = dict(engine=self,
                           task_name=name,
                           task_uuid=uuid,
                           result=None)
            self.task_notifier.notify(states.PENDING, details)
        self._change_state(states.PENDING)

    @lock_utils.locked
    def compile(self):
        """Compiles the contained flow into a structure which the engine can
        use to run or if this can not be done then an exception is thrown
        indicating why this compilation could not be achieved.
        """
        if self._root is not None:
            return

        self._change_state(states.RESUMING)  # does nothing in PENDING state
        task_graph = flow_utils.flatten(self._flow)
        if task_graph.number_of_nodes() == 0:
            raise exc.EmptyFlow("Flow %s is empty." % self._flow.name)
        self._analyzer = self._graph_analyzer_cls(task_graph,
                                                  self.storage)
        self._root = self._graph_action_cls(self._analyzer,
                                            self.storage,
                                            self._task_action)
        for task in task_graph.nodes_iter():
            task_version = misc.get_version_string(task)
            self.storage.ensure_task(task.name, task_version, task.save_as)

        self._change_state(states.SUSPENDED)  # does nothing in PENDING state


class SingleThreadedActionEngine(ActionEngine):
    """Engine that runs tasks in serial manner"""
    _storage_cls = t_storage.Storage


class MultiThreadedActionEngine(ActionEngine):
    """Engine that runs tasks in parallel manner"""

    _storage_cls = t_storage.ThreadSafeStorage

    def _task_executor_cls(self):
        return executor.ParallelTaskExecutor(self._executor)

    def __init__(self, flow, flow_detail, backend, conf):
        self._executor = conf.get('executor', None)
        super(MultiThreadedActionEngine, self).__init__(
            flow, flow_detail, backend, conf)
