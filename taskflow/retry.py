# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
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
import logging

import six

from taskflow import atom

LOG = logging.getLogger(__name__)

# Retry actions
REVERT = "REVERT"
REVERT_ALL = "REVERT_ALL"
RETRY = "RETRY"


@six.add_metaclass(abc.ABCMeta)
class Retry(atom.Atom):
    """A base class for retry that controls subflow execution.
       Retry can be executed multiple times and reverted. On subflow
       failure it makes a decision about what should be done with the flow
       (retry, revert to the previous retry, revert the whole flow, etc.).
    """

    default_provides = None

    def __init__(self, name=None, provides=None, requires=None,
                 auto_extract=True, rebind=None):
        if provides is None:
            provides = self.default_provides
        super(Retry, self).__init__(name, provides)
        self._build_arg_mapping(self.execute, requires, rebind, auto_extract,
                                ignore_list=['history'])

    @property
    def name(self):
        return self._name

    def set_name(self, name):
        self._name = name

    @abc.abstractmethod
    def execute(self, history, *args, **kwargs):
        """Activate a given retry which will produce data required to
           start or restart a subflow using previously provided values and a
           history of subflow failures from previous runs.
           Retry can provide same values multiple times (after each run),
           the latest value will be used by tasks. Old values will be saved to
           the history of retry that is a list of tuples (result, failures)
           where failures is a dictionary of failures by task names.
           This allows to make retries of subflow with different parameters.
        """

    def revert(self, history, *args, **kwargs):
        """Revert this retry using the given context, all results
           that had been provided by previous tries and all errors caused
           a reversion. This method will be called only if a subflow must be
           reverted without the retry. It won't be called on subflow retry, but
           all subflow's tasks will be reverted before the retry.
        """

    @abc.abstractmethod
    def on_failure(self, history, *args, **kwargs):
        """On subflow failure makes a decision about the future flow
           execution using information about all previous failures.
           Returns retry action constant:
           'RERTY' when subflow must be reverted and restarted again (maybe
           with new parameters).
           'REVERT' when this subflow must be completely reverted and parent
           subflow should make a decision about the flow execution.
           'REVERT_ALL' in a case when the whole flow must be reverted and
           marked as FAILURE.
        """


class AlwaysRevert(Retry):
    """Retry that always reverts subflow."""

    def on_failure(self, *args, **kwargs):
        return REVERT

    def execute(self, *args, **kwargs):
        pass
