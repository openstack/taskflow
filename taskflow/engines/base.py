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

from debtcollector import moves
import six

from taskflow.types import notifier
from taskflow.utils import misc


@six.add_metaclass(abc.ABCMeta)
class Engine(object):
    """Base for all engines implementations.

    :ivar notifier: A notification object that will dispatch events that
                    occur related to the flow the engine contains.
    :ivar task_notifier: A notification object that will dispatch events that
                         occur related to the tasks the engine contains.
                         occur related to the tasks the engine
                         contains (deprecated).
    :ivar atom_notifier: A notification object that will dispatch events that
                         occur related to the atoms the engine contains.
    """

    def __init__(self, flow, flow_detail, backend, options):
        self._flow = flow
        self._flow_detail = flow_detail
        self._backend = backend
        self._options = misc.safe_copy_dict(options)
        self._notifier = notifier.Notifier()
        self._atom_notifier = notifier.Notifier()

    @property
    def notifier(self):
        """The flow notifier."""
        return self._notifier

    @property
    @moves.moved_property('atom_notifier', version="0.6",
                          removal_version="2.0")
    def task_notifier(self):
        """The task notifier.

        .. deprecated:: 0.6

            The property is **deprecated** and is present for
            backward compatibility **only**. In order to access this
            property going forward the :py:attr:`.atom_notifier` should
            be used instead.
        """
        return self._atom_notifier

    @property
    def atom_notifier(self):
        """The atom notifier."""
        return self._atom_notifier

    @property
    def options(self):
        """The options that were passed to this engine on construction."""
        return self._options

    @abc.abstractproperty
    def storage(self):
        """The storage unit for this engine."""

    @abc.abstractmethod
    def compile(self):
        """Compiles the contained flow into a internal representation.

        This internal representation is what the engine will *actually* use to
        run. If this compilation can not be accomplished then an exception
        is expected to be thrown with a message indicating why the compilation
        could not be achieved.
        """

    @abc.abstractmethod
    def reset(self):
        """Reset back to the ``PENDING`` state.

        If a flow had previously ended up (from a prior engine
        :py:func:`.run`) in the ``FAILURE``, ``SUCCESS`` or ``REVERTED``
        states (or for some reason it ended up in an intermediary state) it
        can be desireable to make it possible to run it again. Calling this
        method enables that to occur (without causing a state transition
        failure, which would typically occur if :py:meth:`.run` is called
        directly without doing a reset).
        """

    @abc.abstractmethod
    def prepare(self):
        """Performs any pre-run, but post-compilation actions.

        NOTE(harlowja): During preparation it is currently assumed that the
        underlying storage will be initialized, the atoms will be reset and
        the engine will enter the ``PENDING`` state.
        """

    @abc.abstractmethod
    def validate(self):
        """Performs any pre-run, post-prepare validation actions.

        NOTE(harlowja): During validation all final dependencies
        will be verified and ensured. This will by default check that all
        atoms have satisfiable requirements (satisfied by some other
        provider).
        """

    @abc.abstractmethod
    def run(self):
        """Runs the flow in the engine to completion (or die trying)."""

    @abc.abstractmethod
    def suspend(self):
        """Attempts to suspend the engine.

        If the engine is currently running atoms then this will attempt to
        suspend future work from being started (currently active atoms can
        not currently be preempted) and move the engine into a suspend state
        which can then later be resumed from.
        """


# TODO(harlowja): remove in 0.7 or later...
EngineBase = moves.moved_class(Engine, 'EngineBase', __name__,
                               version="0.6", removal_version="2.0")
