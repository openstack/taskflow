# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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

from taskflow import states


class Action(object, metaclass=abc.ABCMeta):
    """An action that handles executing, state changes, ... of atoms."""

    NO_RESULT = object()
    """
    Sentinel use to represent lack of any result (none can be a valid result)
    """

    #: States that are expected to have a result to save...
    SAVE_RESULT_STATES = (states.SUCCESS, states.FAILURE,
                          states.REVERTED, states.REVERT_FAILURE)

    def __init__(self, storage, notifier):
        self._storage = storage
        self._notifier = notifier

    @abc.abstractmethod
    def schedule_execution(self, atom):
        """Schedules atom execution."""

    @abc.abstractmethod
    def schedule_reversion(self, atom):
        """Schedules atom reversion."""

    @abc.abstractmethod
    def complete_reversion(self, atom, result):
        """Completes atom reversion."""

    @abc.abstractmethod
    def complete_execution(self, atom, result):
        """Completes atom execution."""
