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

import contextlib

import fasteners
from oslo_utils import reflection
from oslo_utils import uuidutils
import six

from taskflow import exceptions
from taskflow import logging
from taskflow.persistence.backends import impl_memory
from taskflow.persistence import logbook
from taskflow import retry
from taskflow import states
from taskflow import task
from taskflow.types import failure
from taskflow.utils import misc

LOG = logging.getLogger(__name__)
STATES_WITH_RESULTS = (states.SUCCESS, states.REVERTING, states.FAILURE)

# TODO(harlowja): do this better (via a singleton or something else...)
_TRANSIENT_PROVIDER = object()

# NOTE(harlowja): Perhaps the container is a dictionary-like object and that
# key does not exist (key error), or the container is a tuple/list and a
# non-numeric key is being requested (index error), or there was no container
# and an attempt to index into none/other unsubscriptable type is being
# requested (type error).
#
# Overall this (along with the item_from* functions) try to handle the vast
# majority of wrong indexing operations on the wrong/invalid types so that we
# can fail extraction during lookup or emit warning on result reception...
_EXTRACTION_EXCEPTIONS = (IndexError, KeyError, ValueError, TypeError)

# Atom detail metadata key used to inject atom non-transient injected args.
META_INJECTED = 'injected'

# Atom detail metadata key(s) used to set atom progress (with any details).
META_PROGRESS = 'progress'
META_PROGRESS_DETAILS = 'progress_details'


class _Provider(object):
    """A named symbol provider that produces a output at the given index."""

    def __init__(self, name, index):
        self.name = name
        self.index = index

    def __repr__(self):
        # TODO(harlowja): clean this up...
        if self.name is _TRANSIENT_PROVIDER:
            base = "<TransientProvider"
        else:
            base = "<Provider '%s'" % (self.name)
        if self.index is None:
            base += ">"
        else:
            base += " @ index %r>" % (self.index)
        return base

    def __hash__(self):
        return hash((self.name, self.index))

    def __eq__(self, other):
        return (self.name, self.index) == (other.name, other.index)


def _item_from(container, index):
    """Attempts to fetch a index/key from a given container."""
    if index is None:
        return container
    return container[index]


def _item_from_single(provider, container, looking_for):
    """Returns item from a *single* provider."""
    try:
        return _item_from(container, provider.index)
    except _EXTRACTION_EXCEPTIONS:
        exceptions.raise_with_cause(
            exceptions.NotFound,
            "Unable to find result %r, expected to be able to find it"
            " created by %s but was unable to perform successful"
            " extraction" % (looking_for, provider))


def _item_from_first_of(providers, looking_for):
    """Returns item from the *first* successful container extraction."""
    for (provider, container) in providers:
        try:
            return (provider, _item_from(container, provider.index))
        except _EXTRACTION_EXCEPTIONS:
            pass
    providers = [p[0] for p in providers]
    raise exceptions.NotFound(
        "Unable to find result %r, expected to be able to find it"
        " created by one of %s but was unable to perform successful"
        " extraction" % (looking_for, providers))


class Storage(object):
    """Interface between engines and logbook and its backend (if any).

    This class provides a simple interface to save atoms of a given flow and
    associated activity and results to persistence layer (logbook,
    atom_details, flow_details) for use by engines. This makes it easier to
    interact with the underlying storage & backend mechanism through this
    interface rather than accessing those objects directly.

    NOTE(harlowja): if no backend is provided then a in-memory backend will
    be automatically used and the provided flow detail object will be placed
    into it for the duration of this objects existence.
    """

    injector_name = '_TaskFlow_INJECTOR'
    """Injector task detail name.

    This task detail is a **special** detail that will be automatically
    created and saved to store **persistent** injected values (name conflicts
    with it must be avoided) that are *global* to the flow being executed.
    """

    def __init__(self, flow_detail, backend=None, scope_fetcher=None):
        self._result_mappings = {}
        self._reverse_mapping = {}
        if backend is None:
            # Err on the likely-hood that most people don't make there
            # objects able to be deepcopyable (resources, locks and such
            # can't be deepcopied)...
            backend = impl_memory.MemoryBackend({'deep_copy': False})
            with contextlib.closing(backend.get_connection()) as conn:
                conn.update_flow_details(flow_detail, ignore_missing=True)
        self._backend = backend
        self._flowdetail = flow_detail
        self._transients = {}
        self._injected_args = {}
        self._lock = fasteners.ReaderWriterLock()
        self._ensure_matchers = [
            ((task.BaseTask,), (logbook.TaskDetail, 'Task')),
            ((retry.Retry,), (logbook.RetryDetail, 'Retry')),
        ]
        if scope_fetcher is None:
            scope_fetcher = lambda atom_name: None
        self._scope_fetcher = scope_fetcher

        # NOTE(imelnikov): failure serialization looses information,
        # so we cache failures here, in atom name -> failure mapping.
        self._failures = {}
        for ad in self._flowdetail:
            if ad.failure is not None:
                self._failures[ad.name] = ad.failure

        self._atom_name_to_uuid = dict((ad.name, ad.uuid)
                                       for ad in self._flowdetail)

        try:
            source, _clone = self._atomdetail_by_name(
                self.injector_name, expected_type=logbook.TaskDetail)
        except exceptions.NotFound:
            pass
        else:
            names_iter = six.iterkeys(source.results)
            self._set_result_mapping(source.name,
                                     dict((name, name) for name in names_iter))

    def _with_connection(self, functor, *args, **kwargs):
        # Run the given functor with a backend connection as its first
        # argument (providing the additional positional arguments and keyword
        # arguments as subsequent arguments).
        with contextlib.closing(self._backend.get_connection()) as conn:
            return functor(conn, *args, **kwargs)

    def ensure_atom(self, atom):
        """Ensure that there is an atomdetail in storage for the given atom.

        Returns uuid for the atomdetail that is/was created.
        """
        match = misc.match_type(atom, self._ensure_matchers)
        if not match:
            raise TypeError("Unknown atom '%s' (%s) requested to ensure"
                            % (atom, type(atom)))
        else:
            detail_cls, kind = match
            atom_id = self._ensure_atom_detail(kind, detail_cls, atom.name,
                                               misc.get_version_string(atom),
                                               atom.save_as)
            return atom_id

    def _ensure_atom_detail(self, kind, detail_cls,
                            atom_name, atom_version, result_mapping):
        """Ensures there is a atomdetail that corresponds to the given atom.

        If atom does not exist, adds a record for it. Added atom will have
        PENDING state. Sets result mapping for the atom from result_mapping
        argument.

        Returns uuid for the atomdetails corresponding to the atom with
        given name.
        """
        if not atom_name:
            raise ValueError("%s name must be non-empty" % (kind))
        with self._lock.write_lock():
            try:
                atom_id = self._atom_name_to_uuid[atom_name]
            except KeyError:
                atom_id = uuidutils.generate_uuid()
                self._create_atom_detail(detail_cls, atom_name,
                                         atom_id, atom_version=atom_version)
            else:
                ad = self._flowdetail.find(atom_id)
                if not isinstance(ad, detail_cls):
                    raise exceptions.Duplicate(
                        "Atom detail '%s' already exists in flow"
                        " detail '%s'" % (atom_name, self._flowdetail.name))
            self._set_result_mapping(atom_name, result_mapping)
        return atom_id

    def _create_atom_detail(self, detail_cls, name, uuid, atom_version=None):
        """Add the atom detail to flow detail.

        Atom becomes known to storage by that name and uuid.
        Atom state is set to PENDING.
        """
        ad = detail_cls(name, uuid)
        ad.state = states.PENDING
        ad.version = atom_version
        # Add the atom detail to the clone, which upon success will be
        # updated into the contained flow detail; if it does not get saved
        # then no update will happen.
        source, clone = self._fetch_flowdetail(clone=True)
        clone.add(ad)
        self._with_connection(self._save_flow_detail, source, clone)
        self._atom_name_to_uuid[ad.name] = ad.uuid
        return ad

    @property
    def flow_name(self):
        """The flow detail name this storage unit is associated with."""
        # This never changes (so no read locking needed).
        return self._flowdetail.name

    @property
    def flow_uuid(self):
        """The flow detail uuid this storage unit is associated with."""
        # This never changes (so no read locking needed).
        return self._flowdetail.uuid

    @property
    def backend(self):
        """The backend this storage unit is associated with."""
        # This never changes (so no read locking needed).
        return self._backend

    def _save_flow_detail(self, conn, original_flow_detail, flow_detail):
        # NOTE(harlowja): we need to update our contained flow detail if
        # the result of the update actually added more (aka another process
        # added item to the flow detail).
        original_flow_detail.update(conn.update_flow_details(flow_detail))
        return original_flow_detail

    def _fetch_flowdetail(self, clone=False):
        source = self._flowdetail
        if clone:
            return (source, source.copy())
        else:
            return (source, source)

    def _atomdetail_by_name(self, atom_name, expected_type=None, clone=False):
        try:
            ad = self._flowdetail.find(self._atom_name_to_uuid[atom_name])
        except KeyError:
            exceptions.raise_with_cause(exceptions.NotFound,
                                        "Unknown atom name: %s" % atom_name)
        else:
            # TODO(harlowja): we need to figure out how to get away from doing
            # these kinds of type checks in general (since they likely mean
            # we aren't doing something right).
            if expected_type and not isinstance(ad, expected_type):
                raise TypeError("Atom %s is not of the expected type: %s"
                                % (atom_name,
                                   reflection.get_class_name(expected_type)))
            if clone:
                return (ad, ad.copy())
            else:
                return (ad, ad)

    def _save_atom_detail(self, conn, original_atom_detail, atom_detail):
        # NOTE(harlowja): we need to update our contained atom detail if
        # the result of the update actually added more (aka another process
        # is also modifying the task detail), since python is by reference
        # and the contained atom detail will reflect the old state if we don't
        # do this update.
        original_atom_detail.update(conn.update_atom_details(atom_detail))
        return original_atom_detail

    @fasteners.read_locked
    def get_atom_uuid(self, atom_name):
        """Gets an atoms uuid given a atoms name."""
        source, _clone = self._atomdetail_by_name(atom_name)
        return source.uuid

    @fasteners.write_locked
    def set_atom_state(self, atom_name, state):
        """Sets an atoms state."""
        source, clone = self._atomdetail_by_name(atom_name, clone=True)
        clone.state = state
        self._with_connection(self._save_atom_detail, source, clone)

    @fasteners.read_locked
    def get_atom_state(self, atom_name):
        """Gets the state of an atom given an atoms name."""
        source, _clone = self._atomdetail_by_name(atom_name)
        return source.state

    @fasteners.write_locked
    def set_atom_intention(self, atom_name, intention):
        """Sets the intention of an atom given an atoms name."""
        source, clone = self._atomdetail_by_name(atom_name, clone=True)
        clone.intention = intention
        self._with_connection(self._save_atom_detail, source, clone)

    @fasteners.read_locked
    def get_atom_intention(self, atom_name):
        """Gets the intention of an atom given an atoms name."""
        source, _clone = self._atomdetail_by_name(atom_name)
        return source.intention

    @fasteners.read_locked
    def get_atoms_states(self, atom_names):
        """Gets all atoms states given a set of names."""
        return dict((name, (self.get_atom_state(name),
                            self.get_atom_intention(name)))
                    for name in atom_names)

    @fasteners.write_locked
    def _update_atom_metadata(self, atom_name, update_with,
                              expected_type=None):
        source, clone = self._atomdetail_by_name(atom_name,
                                                 expected_type=expected_type,
                                                 clone=True)
        if update_with:
            clone.meta.update(update_with)
            self._with_connection(self._save_atom_detail, source, clone)

    def update_atom_metadata(self, atom_name, update_with):
        """Updates a atoms associated metadata.

        This update will take a provided dictionary or a list of (key, value)
        pairs to include in the updated metadata (newer keys will overwrite
        older keys) and after merging saves the updated data into the
        underlying persistence layer.
        """
        self._update_atom_metadata(atom_name, update_with)

    def set_task_progress(self, task_name, progress, details=None):
        """Set a tasks progress.

        :param task_name: task name
        :param progress: tasks progress (0.0 <-> 1.0)
        :param details: any task specific progress details
        """
        update_with = {
            META_PROGRESS: progress,
        }
        if details is not None:
            # NOTE(imelnikov): as we can update progress without
            # updating details (e.g. automatically from engine)
            # we save progress value with details, too.
            if details:
                update_with[META_PROGRESS_DETAILS] = {
                    'at_progress': progress,
                    'details': details,
                }
            else:
                update_with[META_PROGRESS_DETAILS] = None
        self._update_atom_metadata(task_name, update_with,
                                   expected_type=logbook.TaskDetail)

    @fasteners.read_locked
    def get_task_progress(self, task_name):
        """Get the progress of a task given a tasks name.

        :param task_name: tasks name
        :returns: current task progress value
        """
        source, _clone = self._atomdetail_by_name(
            task_name, expected_type=logbook.TaskDetail)
        try:
            return source.meta[META_PROGRESS]
        except KeyError:
            return 0.0

    @fasteners.read_locked
    def get_task_progress_details(self, task_name):
        """Get the progress details of a task given a tasks name.

        :param task_name: task name
        :returns: None if progress_details not defined, else progress_details
                 dict
        """
        source, _clone = self._atomdetail_by_name(
            task_name, expected_type=logbook.TaskDetail)
        try:
            return source.meta[META_PROGRESS_DETAILS]
        except KeyError:
            return None

    def _check_all_results_provided(self, atom_name, container):
        """Warn if an atom did not provide some of its expected results.

        This may happen if atom returns shorter tuple or list or dict
        without all needed keys. It may also happen if atom returns
        result of wrong type.
        """
        result_mapping = self._result_mappings.get(atom_name)
        if not result_mapping:
            return
        for name, index in six.iteritems(result_mapping):
            try:
                _item_from(container, index)
            except _EXTRACTION_EXCEPTIONS:
                LOG.warning("Atom %s did not supply result "
                            "with index %r (name %s)", atom_name, index, name)

    @fasteners.write_locked
    def save(self, atom_name, data, state=states.SUCCESS):
        """Put result for atom with id 'uuid' to storage."""
        source, clone = self._atomdetail_by_name(atom_name, clone=True)
        clone.put(state, data)
        result = self._with_connection(self._save_atom_detail, source, clone)
        if state == states.FAILURE and isinstance(data, failure.Failure):
            # NOTE(imelnikov): failure serialization looses information,
            # so we cache failures here, in atom name -> failure mapping so
            # that we can later use the better version on fetch/get.
            self._failures[result.name] = data
        else:
            self._check_all_results_provided(result.name, data)

    @fasteners.write_locked
    def save_retry_failure(self, retry_name, failed_atom_name, failure):
        """Save subflow failure to retry controller history."""
        source, clone = self._atomdetail_by_name(
            retry_name, expected_type=logbook.RetryDetail, clone=True)
        try:
            failures = clone.last_failures
        except exceptions.NotFound:
            exceptions.raise_with_cause(exceptions.StorageFailure,
                                        "Unable to fetch most recent retry"
                                        " failures so new retry failure can"
                                        " be inserted")
        else:
            if failed_atom_name not in failures:
                failures[failed_atom_name] = failure
                self._with_connection(self._save_atom_detail, source, clone)

    @fasteners.write_locked
    def cleanup_retry_history(self, retry_name, state):
        """Cleanup history of retry atom with given name."""
        source, clone = self._atomdetail_by_name(
            retry_name, expected_type=logbook.RetryDetail, clone=True)
        clone.state = state
        clone.results = []
        self._with_connection(self._save_atom_detail, source, clone)

    @fasteners.read_locked
    def _get(self, atom_name, only_last=False):
        source, _clone = self._atomdetail_by_name(atom_name)
        if source.failure is not None:
            cached = self._failures.get(atom_name)
            if source.failure.matches(cached):
                # Try to give the version back that should have the backtrace
                # instead of one that has it stripped (since backtraces are not
                # serializable).
                return cached
            return source.failure
        if source.state not in STATES_WITH_RESULTS:
            raise exceptions.NotFound("Result for atom %s is not currently"
                                      " known" % atom_name)
        if only_last:
            return source.last_results
        else:
            return source.results

    def get(self, atom_name):
        """Gets the results for an atom with a given name from storage."""
        return self._get(atom_name)

    @fasteners.read_locked
    def get_failures(self):
        """Get list of failures that happened with this flow.

        No order guaranteed.
        """
        return self._failures.copy()

    def has_failures(self):
        """Returns True if there are failed tasks in the storage."""
        return bool(self._failures)

    @fasteners.write_locked
    def reset(self, atom_name, state=states.PENDING):
        """Reset atom with given name (if the atom is not in a given state)."""
        if atom_name == self.injector_name:
            return
        source, clone = self._atomdetail_by_name(atom_name, clone=True)
        if source.state == state:
            return
        clone.reset(state)
        result = self._with_connection(self._save_atom_detail, source, clone)
        self._failures.pop(result.name, None)

    def inject_atom_args(self, atom_name, pairs, transient=True):
        """Add values into storage for a specific atom only.

        :param transient: save the data in-memory only instead of persisting
                the data to backend storage (useful for resource-like objects
                or similar objects which can **not** be persisted)

        This method injects a dictionary/pairs of arguments for an atom so that
        when that atom is scheduled for execution it will have immediate access
        to these arguments.

        .. note::

            Injected atom arguments take precedence over arguments
            provided by predecessor atoms or arguments provided by injecting
            into the flow scope (using
            the :py:meth:`~taskflow.storage.Storage.inject` method).

        .. warning::

            It should be noted that injected atom arguments (that are scoped
            to the atom with the given name) *should* be serializable
            whenever possible. This is a **requirement** for the
            :doc:`worker based engine <workers>` which **must**
            serialize (typically using ``json``) all
            atom :py:meth:`~taskflow.atom.Atom.execute` and
            :py:meth:`~taskflow.atom.Atom.revert` arguments to
            be able to transmit those arguments to the target worker(s). If
            the use-case being applied/desired is to later use the worker
            based engine then it is highly recommended to ensure all injected
            atoms (even transient ones) are serializable to avoid issues
            that *may* appear later (when a object turned out to not actually
            be serializable).
        """
        if atom_name not in self._atom_name_to_uuid:
            raise exceptions.NotFound("Unknown atom name: %s" % atom_name)

        def save_transient():
            self._injected_args.setdefault(atom_name, {})
            self._injected_args[atom_name].update(pairs)

        def save_persistent():
            source, clone = self._atomdetail_by_name(atom_name, clone=True)
            injected = source.meta.get(META_INJECTED)
            if not injected:
                injected = {}
            injected.update(pairs)
            clone.meta[META_INJECTED] = injected
            self._with_connection(self._save_atom_detail, source, clone)

        with self._lock.write_lock():
            if transient:
                save_transient()
            else:
                save_persistent()

    @fasteners.write_locked
    def inject(self, pairs, transient=False):
        """Add values into storage.

        This method should be used to put flow parameters (requirements that
        are not satisfied by any atom in the flow) into storage.

        :param transient: save the data in-memory only instead of persisting
                the data to backend storage (useful for resource-like objects
                or similar objects which can **not** be persisted)

        .. warning::

            It should be noted that injected flow arguments (that are scoped
            to all atoms in this flow) *should* be serializable whenever
            possible. This is a **requirement** for
            the :doc:`worker based engine <workers>` which **must**
            serialize (typically using ``json``) all
            atom :py:meth:`~taskflow.atom.Atom.execute` and
            :py:meth:`~taskflow.atom.Atom.revert` arguments to
            be able to transmit those arguments to the target worker(s). If
            the use-case being applied/desired is to later use the worker
            based engine then it is highly recommended to ensure all injected
            atoms (even transient ones) are serializable to avoid issues
            that *may* appear later (when a object turned out to not actually
            be serializable).
        """

        def save_persistent():
            try:
                source, clone = self._atomdetail_by_name(
                    self.injector_name,
                    expected_type=logbook.TaskDetail,
                    clone=True)
            except exceptions.NotFound:
                source = self._create_atom_detail(logbook.TaskDetail,
                                                  self.injector_name,
                                                  uuidutils.generate_uuid())
                clone = source
                clone.results = dict(pairs)
                clone.state = states.SUCCESS
            else:
                clone.results.update(pairs)
            result = self._with_connection(self._save_atom_detail,
                                           source, clone)
            return (self.injector_name, six.iterkeys(result.results))

        def save_transient():
            self._transients.update(pairs)
            return (_TRANSIENT_PROVIDER, six.iterkeys(self._transients))

        if transient:
            provider_name, names = save_transient()
        else:
            provider_name, names = save_persistent()

        self._set_result_mapping(provider_name,
                                 dict((name, name) for name in names))

    def _set_result_mapping(self, provider_name, mapping):
        """Sets the result mapping for a given producer.

        The result saved with given name would be accessible by names
        defined in mapping. Mapping is a dict name => index. If index
        is None, the whole result will have this name; else, only
        part of it, result[index].
        """
        provider_mapping = self._result_mappings.setdefault(provider_name, {})
        if mapping:
            provider_mapping.update(mapping)
            # Ensure the reverse mapping/index is updated (for faster lookups).
            for name, index in six.iteritems(provider_mapping):
                entries = self._reverse_mapping.setdefault(name, [])
                provider = _Provider(provider_name, index)
                if provider not in entries:
                    entries.append(provider)

    @fasteners.read_locked
    def fetch(self, name, many_handler=None):
        """Fetch a named result."""
        def _many_handler(values):
            # By default we just return the first of many (unless provided
            # a different callback that can translate many results into
            # something more meaningful).
            return values[0]
        if many_handler is None:
            many_handler = _many_handler
        try:
            providers = self._reverse_mapping[name]
        except KeyError:
            exceptions.raise_with_cause(exceptions.NotFound,
                                        "Name %r is not mapped as a produced"
                                        " output by any providers" % name)
        values = []
        for provider in providers:
            if provider.name is _TRANSIENT_PROVIDER:
                values.append(_item_from_single(provider,
                                                self._transients, name))
            else:
                try:
                    container = self._get(provider.name, only_last=True)
                except exceptions.NotFound:
                    pass
                else:
                    values.append(_item_from_single(provider,
                                                    container, name))
        if not values:
            raise exceptions.NotFound("Unable to find result %r,"
                                      " searched %s" % (name, providers))
        else:
            return many_handler(values)

    @fasteners.read_locked
    def fetch_unsatisfied_args(self, atom_name, args_mapping,
                               scope_walker=None, optional_args=None):
        """Fetch unsatisfied atom arguments using an atoms argument mapping.

        NOTE(harlowja): this takes into account the provided scope walker
        atoms who should produce the required value at runtime, as well as
        the transient/persistent flow and atom specific injected arguments.
        It does **not** check if the providers actually have produced the
        needed values; it just checks that they are registered to produce
        it in the future.
        """

        def _fetch_providers(name):
            """Fetchs pair of (default providers, non-default providers)."""
            default_providers = []
            non_default_providers = []
            for p in self._reverse_mapping.get(name, []):
                if p.name in (_TRANSIENT_PROVIDER, self.injector_name):
                    default_providers.append(p)
                else:
                    non_default_providers.append(p)
            return default_providers, non_default_providers

        def _locate_providers(name, scope_walker=None):
            """Finds the accessible *potential* providers."""
            default_providers, non_default_providers = _fetch_providers(name)
            providers = []
            if non_default_providers:
                if scope_walker is not None:
                    scope_iter = iter(scope_walker)
                else:
                    scope_iter = iter([])
                for names in scope_iter:
                    for p in non_default_providers:
                        if p.name in names:
                            providers.append(p)
            for p in default_providers:
                if p.name is _TRANSIENT_PROVIDER:
                    results = self._transients
                else:
                    try:
                        results = self._get(p.name, only_last=True)
                    except exceptions.NotFound:
                        results = {}
                try:
                    _item_from_single(p, results, name)
                except exceptions.NotFound:
                    pass
                else:
                    providers.append(p)
            return providers

        source, _clone = self._atomdetail_by_name(atom_name)
        if scope_walker is None:
            scope_walker = self._scope_fetcher(atom_name)
        if optional_args is None:
            optional_args = []
        injected_sources = [
            self._injected_args.get(atom_name, {}),
            source.meta.get(META_INJECTED, {}),
        ]
        missing = set(six.iterkeys(args_mapping))
        for (bound_name, name) in six.iteritems(args_mapping):
            if LOG.isEnabledFor(logging.BLATHER):
                LOG.blather("Looking for %r <= %r for atom named: %s",
                            bound_name, name, atom_name)
            if bound_name in optional_args:
                LOG.blather("Argument %r is optional, skipping", bound_name)
                missing.discard(bound_name)
                continue
            maybe_providers = 0
            for source in injected_sources:
                if not source:
                    continue
                if name in source:
                    maybe_providers += 1
            providers = _locate_providers(name, scope_walker=scope_walker)
            maybe_providers += len(providers)
            if maybe_providers:
                LOG.blather("Atom %s will have %s potential providers"
                            " of %r <= %r", atom_name, maybe_providers,
                            bound_name, name)
                missing.discard(bound_name)
        return missing

    @fasteners.read_locked
    def fetch_all(self, many_handler=None):
        """Fetch all named results known so far."""
        def _many_handler(values):
            if len(values) > 1:
                return values
            return values[0]
        if many_handler is None:
            many_handler = _many_handler
        results = {}
        for name in six.iterkeys(self._reverse_mapping):
            try:
                results[name] = self.fetch(name, many_handler=many_handler)
            except exceptions.NotFound:
                pass
        return results

    @fasteners.read_locked
    def fetch_mapped_args(self, args_mapping,
                          atom_name=None, scope_walker=None,
                          optional_args=None):
        """Fetch arguments for an atom using an atoms argument mapping."""

        def _extract_first_from(name, sources):
            """Extracts/returns first occurence of key in list of dicts."""
            for i, source in enumerate(sources):
                if not source:
                    continue
                if name in source:
                    return (i, source[name])
            raise KeyError(name)

        def _get_results(looking_for, provider):
            """Gets the results saved for a given provider."""
            try:
                return self._get(provider.name, only_last=True)
            except exceptions.NotFound:
                exceptions.raise_with_cause(exceptions.NotFound,
                                            "Expected to be able to find"
                                            " output %r produced by %s but was"
                                            " unable to get at that providers"
                                            " results" % (looking_for,
                                                          provider))

        def _locate_providers(looking_for, possible_providers,
                              scope_walker=None):
            """Finds the accessible providers."""
            default_providers = []
            for p in possible_providers:
                if p.name is _TRANSIENT_PROVIDER:
                    default_providers.append((p, self._transients))
                if p.name == self.injector_name:
                    default_providers.append((p, _get_results(looking_for, p)))
            if default_providers:
                return default_providers
            if scope_walker is not None:
                scope_iter = iter(scope_walker)
            else:
                scope_iter = iter([])
            extractor = lambda p: p.name
            for names in scope_iter:
                # *Always* retain the scope ordering (if any matches
                # happen); instead of retaining the possible provider match
                # order (which isn't that important and may be different from
                # the scope requested ordering).
                providers = misc.look_for(names, possible_providers,
                                          extractor=extractor)
                if providers:
                    return [(p, _get_results(looking_for, p))
                            for p in providers]
            return []

        if optional_args is None:
            optional_args = []
        if atom_name:
            source, _clone = self._atomdetail_by_name(atom_name)
            injected_sources = [
                self._injected_args.get(atom_name, {}),
                source.meta.get(META_INJECTED, {}),
            ]
            if scope_walker is None:
                scope_walker = self._scope_fetcher(atom_name)
        else:
            injected_sources = []
        if not args_mapping:
            return {}
        mapped_args = {}
        for (bound_name, name) in six.iteritems(args_mapping):
            if LOG.isEnabledFor(logging.BLATHER):
                if atom_name:
                    LOG.blather("Looking for %r <= %r for atom named: %s",
                                bound_name, name, atom_name)
                else:
                    LOG.blather("Looking for %r <= %r", bound_name, name)
            try:
                source_index, value = _extract_first_from(name,
                                                          injected_sources)
                mapped_args[bound_name] = value
                if LOG.isEnabledFor(logging.BLATHER):
                    if source_index == 0:
                        LOG.blather("Matched %r <= %r to %r (from injected"
                                    " atom-specific transient"
                                    " values)", bound_name, name, value)
                    else:
                        LOG.blather("Matched %r <= %r to %r (from injected"
                                    " atom-specific persistent"
                                    " values)", bound_name, name, value)
            except KeyError:
                try:
                    possible_providers = self._reverse_mapping[name]
                except KeyError:
                    if bound_name in optional_args:
                        LOG.blather("Argument %r is optional, skipping",
                                    bound_name)
                        continue
                    raise exceptions.NotFound("Name %r is not mapped as a"
                                              " produced output by any"
                                              " providers" % name)
                # Reduce the possible providers to one that are allowed.
                providers = _locate_providers(name, possible_providers,
                                              scope_walker=scope_walker)
                if not providers:
                    raise exceptions.NotFound(
                        "Mapped argument %r <= %r was not produced"
                        " by any accessible provider (%s possible"
                        " providers were scanned)"
                        % (bound_name, name, len(possible_providers)))
                provider, value = _item_from_first_of(providers, name)
                mapped_args[bound_name] = value
                LOG.blather("Matched %r <= %r to %r (from %s)",
                            bound_name, name, value, provider)
        return mapped_args

    @fasteners.write_locked
    def set_flow_state(self, state):
        """Set flow details state and save it."""
        source, clone = self._fetch_flowdetail(clone=True)
        clone.state = state
        self._with_connection(self._save_flow_detail, source, clone)

    @fasteners.write_locked
    def update_flow_metadata(self, update_with):
        """Update flowdetails metadata and save it."""
        if update_with:
            source, clone = self._fetch_flowdetail(clone=True)
            clone.meta.update(update_with)
            self._with_connection(self._save_flow_detail, source, clone)

    @fasteners.read_locked
    def get_flow_state(self):
        """Get state from flow details."""
        source = self._flowdetail
        state = source.state
        if state is None:
            state = states.PENDING
        return state

    def _translate_into_history(self, ad):
        failure = None
        if ad.failure is not None:
            # NOTE(harlowja): Try to use our local cache to get a more
            # complete failure object that has a traceback (instead of the
            # one that is saved which will *typically* not have one)...
            cached = self._failures.get(ad.name)
            if ad.failure.matches(cached):
                failure = cached
            else:
                failure = ad.failure
        return retry.History(ad.results, failure=failure)

    @fasteners.read_locked
    def get_retry_history(self, retry_name):
        """Fetch a single retrys history."""
        source, _clone = self._atomdetail_by_name(
            retry_name, expected_type=logbook.RetryDetail)
        return self._translate_into_history(source)

    @fasteners.read_locked
    def get_retry_histories(self):
        """Fetch all retrys histories."""
        histories = []
        for ad in self._flowdetail:
            if isinstance(ad, logbook.RetryDetail):
                histories.append((ad.name,
                                  self._translate_into_history(ad)))
        return histories
