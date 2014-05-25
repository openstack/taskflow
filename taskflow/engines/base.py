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


import abc

import six

from taskflow.utils import misc


@six.add_metaclass(abc.ABCMeta)
class EngineBase(object):
    """Base for all engines implementations.

    :ivar notifier: A notification object that will dispatch events that
                    occur related to the flow the engine contains.
    :ivar task_notifier: A notification object that will dispatch events that
                         occur related to the tasks the engine contains.
    """

    def __init__(self, flow, flow_detail, backend, conf):
        self._flow = flow
        self._flow_detail = flow_detail
        self._backend = backend
        if not conf:
            self._conf = {}
        else:
            self._conf = dict(conf)
        self.notifier = misc.Notifier()
        self.task_notifier = misc.Notifier()

    @misc.cachedproperty
    def storage(self):
        """The storage unit for this flow."""
        return self._storage_factory(self._flow_detail, self._backend)

    @abc.abstractproperty
    def _storage_factory(self):
        """Storage factory that will be used to generate storage objects."""

    @abc.abstractmethod
    def compile(self):
        """Compiles the contained flow into a structure which the engine can
        use to run or if this can not be done then an exception is thrown
        indicating why this compilation could not be achieved.
        """

    @abc.abstractmethod
    def prepare(self):
        """Performs any pre-run, but post-compilation actions.

        NOTE(harlowja): During preparation it is currently assumed that the
        underlying storage will be initialized, all final dependencies
        will be verified, the tasks will be reset and the engine will enter
        the PENDING state.
        """

    @abc.abstractmethod
    def run(self):
        """Runs the flow in the engine to completion (or die trying)."""

    @abc.abstractmethod
    def suspend(self):
        """Attempts to suspend the engine.

        If the engine is currently running tasks then this will attempt to
        suspend future work from being started (currently active tasks can
        not currently be preempted) and move the engine into a suspend state
        which can then later be resumed from.
        """
