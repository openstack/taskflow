# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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
import weakref

from oslo_utils import reflection
import six

from taskflow.engines.action_engine import executor as ex
from taskflow import logging
from taskflow import retry as retry_atom
from taskflow import states as st
from taskflow import task as task_atom
from taskflow.types import failure

LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class _Strategy(object):
    """Local/internal helper strategy base object"""

    def __init__(self, runtime):
        self._runtime = runtime

    def __str__(self):
        base = reflection.get_class_name(self, fully_qualified=False)
        return base + "(strategy=%s)" % (self.strategy.name)


class _RevertAndRetry(_Strategy):
    """Sets the *associated* subflow for revert to be later retried."""

    strategy = retry_atom.RETRY

    def __init__(self, runtime, retry):
        super(_RevertAndRetry, self).__init__(runtime)
        self._retry = retry

    def apply(self):
        tweaked = self._runtime.reset_nodes([self._retry], state=None,
                                            intention=st.RETRY)
        tweaked.extend(self._runtime.reset_subgraph(self._retry, state=None,
                                                    intention=st.REVERT))
        return tweaked


class _RevertAll(_Strategy):
    """Sets *all* nodes/atoms to the ``REVERT`` intention."""

    strategy = retry_atom.REVERT_ALL

    def __init__(self, runtime):
        super(_RevertAll, self).__init__(runtime)
        self._analyzer = runtime.analyzer

    def apply(self):
        return self._runtime.reset_nodes(self._analyzer.iterate_all_nodes(),
                                         state=None, intention=st.REVERT)


class _Revert(_Strategy):
    """Sets atom and *associated* nodes to the ``REVERT`` intention."""

    strategy = retry_atom.REVERT

    def __init__(self, runtime, atom):
        super(_Revert, self).__init__(runtime)
        self._atom = atom

    def apply(self):
        tweaked = self._runtime.reset_nodes([self._atom], state=None,
                                            intention=st.REVERT)
        tweaked.extend(self._runtime.reset_subgraph(self._atom, state=None,
                                                    intention=st.REVERT))
        return tweaked


class Completer(object):
    """Completes atoms using actions to complete them."""

    def __init__(self, runtime):
        self._runtime = weakref.proxy(runtime)
        self._analyzer = runtime.analyzer
        self._retry_action = runtime.retry_action
        self._storage = runtime.storage
        self._task_action = runtime.task_action
        self._undefined_resolver = _RevertAll(self._runtime)

    def _complete_task(self, task, event, result):
        """Completes the given task, processes task failure."""
        if event == ex.EXECUTED:
            self._task_action.complete_execution(task, result)
        else:
            self._task_action.complete_reversion(task, result)

    def resume(self):
        """Resumes nodes in the contained graph.

        This is done to allow any previously completed or failed nodes to
        be analyzed, there results processed and any potential nodes affected
        to be adjusted as needed.

        This should return a set of nodes which should be the initial set of
        nodes that were previously not finished (due to a RUNNING or REVERTING
        attempt not previously finishing).
        """
        for node in self._analyzer.iterate_all_nodes():
            if self._analyzer.get_state(node) == st.FAILURE:
                self._process_atom_failure(node, self._storage.get(node.name))
        for retry in self._analyzer.iterate_retries(st.RETRYING):
            self._runtime.retry_subflow(retry)
        unfinished_nodes = set()
        for node in self._analyzer.iterate_all_nodes():
            if self._analyzer.get_state(node) in (st.RUNNING, st.REVERTING):
                unfinished_nodes.add(node)
        return unfinished_nodes

    def complete(self, node, event, result):
        """Performs post-execution completion of a node.

        Returns whether the result should be saved into an accumulator of
        failures or whether this should not be done.
        """
        if isinstance(node, task_atom.BaseTask):
            self._complete_task(node, event, result)
        if isinstance(result, failure.Failure):
            if event == ex.EXECUTED:
                self._process_atom_failure(node, result)
            else:
                return True
        return False

    def _determine_resolution(self, atom, failure):
        """Determines which resolution strategy to activate/apply."""
        retry = self._analyzer.find_atom_retry(atom)
        if retry is not None:
            # Ask retry controller what to do in case of failure.
            strategy = self._retry_action.on_failure(retry, atom, failure)
            if strategy == retry_atom.RETRY:
                return _RevertAndRetry(self._runtime, retry)
            elif strategy == retry_atom.REVERT:
                # Ask parent retry and figure out what to do...
                parent_resolver = self._determine_resolution(retry, failure)
                # Ok if the parent resolver says something not REVERT, and
                # it isn't just using the undefined resolver, assume the
                # parent knows best.
                if parent_resolver is not self._undefined_resolver:
                    if parent_resolver.strategy != retry_atom.REVERT:
                        return parent_resolver
                return _Revert(self._runtime, retry)
            elif strategy == retry_atom.REVERT_ALL:
                return _RevertAll(self._runtime)
            else:
                raise ValueError("Unknown atom failure resolution"
                                 " action/strategy '%s'" % strategy)
        else:
            return self._undefined_resolver

    def _process_atom_failure(self, atom, failure):
        """Processes atom failure & applies resolution strategies.

        On atom failure this will find the atoms associated retry controller
        and ask that controller for the strategy to perform to resolve that
        failure. After getting a resolution strategy decision this method will
        then adjust the needed other atoms intentions, and states, ... so that
        the failure can be worked around.
        """
        resolver = self._determine_resolution(atom, failure)
        LOG.debug("Applying resolver '%s' to resolve failure '%s'"
                  " of atom '%s'", resolver, failure, atom)
        tweaked = resolver.apply()
        # Only show the tweaked node list when blather is on, otherwise
        # just show the amount/count of nodes tweaks...
        if LOG.isEnabledFor(logging.BLATHER):
            LOG.blather("Modified/tweaked %s nodes while applying"
                        " resolver '%s'", tweaked, resolver)
        else:
            LOG.debug("Modified/tweaked %s nodes while applying"
                      " resolver '%s'", len(tweaked), resolver)
