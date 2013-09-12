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

import multiprocessing
import threading

from concurrent import futures

from taskflow.engines.action_engine import parallel_action
from taskflow.engines.action_engine import seq_action
from taskflow.engines.action_engine import task_action

from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf

from taskflow.persistence import utils as p_utils

from taskflow import decorators
from taskflow import exceptions as exc
from taskflow import states
from taskflow import storage as t_storage
from taskflow import task

from taskflow.utils import misc


class ActionEngine(object):
    """Generic action-based engine.

    Converts the flow to recursive structure of actions.
    """

    def __init__(self, flow, storage):
        self._failures = []
        self._root = None
        self._flow = flow
        self._lock = threading.RLock()
        self.notifier = misc.TransitionNotifier()
        self.task_notifier = misc.TransitionNotifier()
        self.storage = storage

    def _revert(self, current_failure):
        self._change_state(states.REVERTING)
        self._root.revert(self)
        self._change_state(states.REVERTED)
        self._change_state(states.FAILURE)
        if self._failures:
            if len(self._failures) == 1:
                self._failures[0].reraise()
            else:
                exc_infos = [f.exc_info for f in self._failures]
                raise exc.LinkedException.link(exc_infos)
        else:
            current_failure.reraise()

    def _reset(self):
        self._failures = []

    @decorators.locked
    def run(self):
        self.compile()
        self._reset()

        external_provides = set(self.storage.fetch_all().keys())
        missing = self._flow.requires - external_provides
        if missing:
            raise exc.MissingDependencies(self._flow, sorted(missing))

        self._change_state(states.RUNNING)
        try:
            self._root.execute(self)
        except Exception:
            self._revert(misc.Failure())
        else:
            self._change_state(states.SUCCESS)

    def _change_state(self, state):
        self.storage.set_flow_state(state)
        details = dict(engine=self)
        self.notifier.notify(state, details)

    def on_task_state_change(self, task_action, state, result=None):
        if isinstance(result, misc.Failure):
            self._failures.append(result)
        details = dict(engine=self,
                       task_name=task_action.name,
                       task_uuid=task_action.uuid,
                       result=result)
        self.task_notifier.notify(state, details)

    @decorators.locked
    def compile(self):
        if self._root is None:
            translator = self.translator_cls(self)
            self._root = translator.translate(self._flow)


class Translator(object):

    def __init__(self, engine):
        self.engine = engine

    def _factory_map(self):
        return []

    def translate(self, pattern):
        """Translates the pattern into an engine runnable action"""
        if isinstance(pattern, task.BaseTask):
            # Wrap the task into something more useful.
            return task_action.TaskAction(pattern, self.engine)

        # Decompose the flow into something more useful:
        for cls, factory in self._factory_map():
            if isinstance(pattern, cls):
                return factory(pattern)

        raise TypeError('Unknown pattern type: %s (type %s)'
                        % (pattern, type(pattern)))


class SingleThreadedTranslator(Translator):

    def _factory_map(self):
        return [(lf.Flow, self._translate_sequential),
                (uf.Flow, self._translate_sequential)]

    def _translate_sequential(self, pattern):
        action = seq_action.SequentialAction()
        for p in pattern:
            action.add(self.translate(p))
        return action


class SingleThreadedActionEngine(ActionEngine):
    translator_cls = SingleThreadedTranslator

    def __init__(self, flow, flow_detail=None, book=None, backend=None):
        if flow_detail is None:
            flow_detail = p_utils.create_flow_detail(flow,
                                                     book=book,
                                                     backend=backend)
        ActionEngine.__init__(self, flow,
                              storage=t_storage.Storage(flow_detail, backend))


class MultiThreadedTranslator(Translator):

    def _factory_map(self):
        return [(lf.Flow, self._translate_sequential),
                # unordered can be run in parallel
                (uf.Flow, self._translate_parallel)]

    def _translate_sequential(self, pattern):
        action = seq_action.SequentialAction()
        for p in pattern:
            action.add(self.translate(p))
        return action

    def _translate_parallel(self, pattern):
        action = parallel_action.ParallelAction()
        for p in pattern:
            action.add(self.translate(p))
        return action


class MultiThreadedActionEngine(ActionEngine):
    translator_cls = MultiThreadedTranslator

    def __init__(self, flow, flow_detail=None, book=None, backend=None,
                 executor=None):
        if flow_detail is None:
            flow_detail = p_utils.create_flow_detail(flow,
                                                     book=book,
                                                     backend=backend)
        ActionEngine.__init__(self, flow,
                              storage=t_storage.ThreadSafeStorage(flow_detail,
                                                                  backend))
        if executor is not None:
            self._executor = executor
            self._owns_executor = False
            self._thread_count = -1
        else:
            self._executor = None
            self._owns_executor = True
            # TODO(harlowja): allow this to be configurable??
            try:
                self._thread_count = multiprocessing.cpu_count() + 1
            except NotImplementedError:
                # NOTE(harlowja): apparently may raise so in this case we will
                # just setup two threads since its hard to know what else we
                # should do in this situation.
                self._thread_count = 2

    @decorators.locked
    def run(self):
        if self._owns_executor:
            if self._executor is not None:
                # The previous shutdown failed, something is very wrong.
                raise exc.InvalidStateException("The previous shutdown() of"
                                                " the executor powering this"
                                                " engine failed. Something is"
                                                " very very wrong.")
            self._executor = futures.ThreadPoolExecutor(self._thread_count)
        try:
            ActionEngine.run(self)
        finally:
            # Don't forget to shutdown the executor!!
            if self._owns_executor and self._executor is not None:
                self._executor.shutdown(wait=True)
                self._executor = None

    @property
    def executor(self):
        return self._executor
