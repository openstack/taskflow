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
from oslo_utils import strutils
import six

from taskflow.engines.action_engine import compiler as co
from taskflow.engines.action_engine import executor as ex
from taskflow import logging
from taskflow import retry as retry_atom
from taskflow import states as st

LOG = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class Strategy(object):
    """Failure resolution strategy base class."""

    strategy = None

    def __init__(self, runtime):
        self._runtime = runtime

    @abc.abstractmethod
    def apply(self):
        """Applies some algorithm to resolve some detected failure."""

    def __str__(self):
        base = reflection.get_class_name(self, fully_qualified=False)
        if self.strategy is not None:
            strategy_name = self.strategy.name
        else:
            strategy_name = "???"
        return base + "(strategy=%s)" % (strategy_name)


class RevertAndRetry(Strategy):
    """Sets the *associated* subflow for revert to be later retried."""

    strategy = retry_atom.RETRY

    def __init__(self, runtime, retry):
        super(RevertAndRetry, self).__init__(runtime)
        self._retry = retry

    def apply(self):
        tweaked = self._runtime.reset_atoms([self._retry], state=None,
                                            intention=st.RETRY)
        tweaked.extend(self._runtime.reset_subgraph(self._retry, state=None,
                                                    intention=st.REVERT))
        return tweaked


class RevertAll(Strategy):
    """Sets *all* nodes/atoms to the ``REVERT`` intention."""

    strategy = retry_atom.REVERT_ALL

    def __init__(self, runtime):
        super(RevertAll, self).__init__(runtime)
        self._analyzer = runtime.analyzer

    def apply(self):
        return self._runtime.reset_atoms(
            self._analyzer.iterate_nodes(co.ATOMS),
            state=None, intention=st.REVERT)


class Revert(Strategy):
    """Sets atom and *associated* nodes to the ``REVERT`` intention."""

    strategy = retry_atom.REVERT

    def __init__(self, runtime, atom):
        super(Revert, self).__init__(runtime)
        self._atom = atom

    def apply(self):
        tweaked = self._runtime.reset_atoms([self._atom], state=None,
                                            intention=st.REVERT)
        tweaked.extend(self._runtime.reset_subgraph(self._atom, state=None,
                                                    intention=st.REVERT))
        return tweaked


class Completer(object):
    """Completes atoms using actions to complete them."""

    def __init__(self, runtime):
        self._runtime = weakref.proxy(runtime)
        self._analyzer = runtime.analyzer
        self._storage = runtime.storage
        self._undefined_resolver = RevertAll(self._runtime)
        self._defer_reverts = strutils.bool_from_string(
            self._runtime.options.get('defer_reverts', False))
        self._resolve = not strutils.bool_from_string(
            self._runtime.options.get('never_resolve', False))

    def resume(self):
        """Resumes atoms in the contained graph.

        This is done to allow any previously completed or failed atoms to
        be analyzed, there results processed and any potential atoms affected
        to be adjusted as needed.

        This should return a set of atoms which should be the initial set of
        atoms that were previously not finished (due to a RUNNING or REVERTING
        attempt not previously finishing).
        """
        atoms = list(self._analyzer.iterate_nodes(co.ATOMS))
        atom_states = self._storage.get_atoms_states(atom.name
                                                     for atom in atoms)
        if self._resolve:
            for atom in atoms:
                atom_state, _atom_intention = atom_states[atom.name]
                if atom_state == st.FAILURE:
                    self._process_atom_failure(
                        atom, self._storage.get(atom.name))
            for retry in self._analyzer.iterate_retries(st.RETRYING):
                retry_affected_atoms_it = self._runtime.retry_subflow(retry)
                for atom, state, intention in retry_affected_atoms_it:
                    if state:
                        atom_states[atom.name] = (state, intention)
        unfinished_atoms = set()
        for atom in atoms:
            atom_state, _atom_intention = atom_states[atom.name]
            if atom_state in (st.RUNNING, st.REVERTING):
                unfinished_atoms.add(atom)
                LOG.trace("Resuming atom '%s' since it was left in"
                          " state %s", atom, atom_state)
        return unfinished_atoms

    def complete_failure(self, node, outcome, failure):
        """Performs post-execution completion of a nodes failure.

        Returns whether the result should be saved into an accumulator of
        failures or whether this should not be done.
        """
        if outcome == ex.EXECUTED and self._resolve:
            self._process_atom_failure(node, failure)
            # We resolved something, carry on...
            return False
        else:
            # Reverting failed (or resolving was turned off), always
            # retain the failure...
            return True

    def complete(self, node, outcome, result):
        """Performs post-execution completion of a node result."""
        handler = self._runtime.fetch_action(node)
        if outcome == ex.EXECUTED:
            handler.complete_execution(node, result)
        else:
            handler.complete_reversion(node, result)

    def _determine_resolution(self, atom, failure):
        """Determines which resolution strategy to activate/apply."""
        retry = self._analyzer.find_retry(atom)
        if retry is not None:
            # Ask retry controller what to do in case of failure.
            handler = self._runtime.fetch_action(retry)
            strategy = handler.on_failure(retry, atom, failure)
            if strategy == retry_atom.RETRY:
                return RevertAndRetry(self._runtime, retry)
            elif strategy == retry_atom.REVERT:
                # Ask parent retry and figure out what to do...
                parent_resolver = self._determine_resolution(retry, failure)
                # In the future, this will be the only behavior. REVERT
                # should defer to the parent retry if it exists, or use the
                # default REVERT_ALL if it doesn't.
                if self._defer_reverts:
                    return parent_resolver
                # Ok if the parent resolver says something not REVERT, and
                # it isn't just using the undefined resolver, assume the
                # parent knows best.
                if parent_resolver is not self._undefined_resolver:
                    if parent_resolver.strategy != retry_atom.REVERT:
                        return parent_resolver
                return Revert(self._runtime, retry)
            elif strategy == retry_atom.REVERT_ALL:
                return RevertAll(self._runtime)
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
        # Only show the tweaked node list when trace is on, otherwise
        # just show the amount/count of nodes tweaks...
        if LOG.isEnabledFor(logging.TRACE):
            LOG.trace("Modified/tweaked %s nodes while applying"
                      " resolver '%s'", tweaked, resolver)
        else:
            LOG.debug("Modified/tweaked %s nodes while applying"
                      " resolver '%s'", len(tweaked), resolver)
