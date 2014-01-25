# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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
    """Base for all engines implementations."""

    def __init__(self, flow, flow_detail, backend, conf):
        self._flow = flow
        self._flow_detail = flow_detail
        self._backend = backend
        if not conf:
            self._conf = {}
        else:
            self._conf = dict(conf)
        self._storage = None
        self.notifier = misc.TransitionNotifier()
        self.task_notifier = misc.TransitionNotifier()

    @property
    def storage(self):
        """The storage unit for this flow."""
        if self._storage is None:
            self._storage = self._storage_cls(self._flow_detail, self._backend)
        return self._storage

    @abc.abstractproperty
    def _storage_cls(self):
        """Storage class that will be used to generate storage objects."""

    @abc.abstractmethod
    def compile(self):
        """Compiles the contained flow into a structure which the engine can
        use to run or if this can not be done then an exception is thrown
        indicating why this compilation could not be achieved.
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
