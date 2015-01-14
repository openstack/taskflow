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

import six

from taskflow import states


#: Sentinel use to represent no-result (none can be a valid result...)
NO_RESULT = object()

#: States that are expected to/may have a result to save...
SAVE_RESULT_STATES = (states.SUCCESS, states.FAILURE)


@six.add_metaclass(abc.ABCMeta)
class Action(object):
    """An action that handles executing, state changes, ... of atoms."""

    def __init__(self, storage, notifier, walker_factory):
        self._storage = storage
        self._notifier = notifier
        self._walker_factory = walker_factory

    @abc.abstractmethod
    def handles(self, atom):
        """Checks if this action handles the provided atom."""
