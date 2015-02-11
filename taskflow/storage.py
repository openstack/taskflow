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

from oslo_utils import reflection
from oslo_utils import uuidutils
import six

from taskflow import exceptions
from taskflow import logging
from taskflow.persistence import logbook
from taskflow import retry
from taskflow import states
from taskflow import task
from taskflow.types import failure
from taskflow.utils import lock_utils
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
        raise exceptions.NotFound(
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
    """

    injector_name = '_TaskFlow_INJECTOR'
    """Injector task detail name.

    This task detail is a **special** detail that will be automatically
    created and saved to store **persistent** injected values (name conflicts
    with it must be avoided) that are *global* to the flow being executed.
    """

    def __init__(self, flow_detail, backend=None):
        self._result_mappings = {}
        self._reverse_mapping = {}
        self._backend = backend
        self._flowdetail = flow_detail
        self._transients = {}
        self._injected_args = {}
        self._lock = lock_utils.ReaderWriterLock()

        # NOTE(imelnikov): failure serialization looses information,
        # so we cache failures here, in atom name -> failure mapping.
        self._failures = {}
        for ad in self._flowdetail:
            if ad.failure is not None:
                self._failures[ad.name] = ad.failure

        self._atom_name_to_uuid = dict((ad.name, ad.uuid)
                                       for ad in self._flowdetail)

        try:
            injector_td = self._atomdetail_by_name(
                self.injector_name,
                expected_type=logbook.TaskDetail)
        except exceptions.NotFound:
            pass
        else:
            names = six.iterkeys(injector_td.results)
            self._set_result_mapping(injector_td.name,
                                     dict((name, name) for name in names))

    def _with_connection(self, functor, *args, **kwargs):
        # NOTE(harlowja): Activate the given function with a backend
        # connection, if a backend is provided in the first place, otherwise
        # don't call the function.
        if self._backend is None:
            return
        with contextlib.closing(self._backend.get_connection()) as conn:
            functor(conn, *args, **kwargs)

    def ensure_atom(self, atom):
        """Ensure that there is an atomdetail in storage for the given atom.

        Returns uuid for the atomdetail that is/was created.
        """
        if isinstance(atom, task.BaseTask):
            return self._ensure_task(atom.name,
                                     misc.get_version_string(atom),
                                     atom.save_as)
        elif isinstance(atom, retry.Retry):
            return self._ensure_retry(atom.name,
                                      misc.get_version_string(atom),
                                      atom.save_as)
        else:
            raise TypeError("Object of type 'atom' expected not"
                            " '%s' (%s)" % (atom, type(atom)))

    def _ensure_task(self, task_name, task_version, result_mapping):
        """Ensures there is a taskdetail that corresponds to the task info.

        If task does not exist, adds a record for it. Added task will have
        PENDING state. Sets result mapping for the task from result_mapping
        argument.

        Returns uuid for the task details corresponding to the task with
        given name.
        """
        if not task_name:
            raise ValueError("Task name must be non-empty")
        with self._lock.write_lock():
            try:
                task_id = self._atom_name_to_uuid[task_name]
            except KeyError:
                task_id = uuidutils.generate_uuid()
                self._create_atom_detail(logbook.TaskDetail, task_name,
                                         task_id, task_version)
            else:
                ad = self._flowdetail.find(task_id)
                if not isinstance(ad, logbook.TaskDetail):
                    raise exceptions.Duplicate(
                        "Atom detail %s already exists in flow detail %s." %
                        (task_name, self._flowdetail.name))
            self._set_result_mapping(task_name, result_mapping)
        return task_id

    def _ensure_retry(self, retry_name, retry_version, result_mapping):
        """Ensures there is a retrydetail that corresponds to the retry info.

        If retry does not exist, adds a record for it. Added retry
        will have PENDING state. Sets result mapping for the retry from
        result_mapping argument. Initializes retry result as an empty
        collections of results and failures history.

        Returns uuid for the retry details corresponding to the retry
        with given name.
        """
        if not retry_name:
            raise ValueError("Retry name must be non-empty")
        with self._lock.write_lock():
            try:
                retry_id = self._atom_name_to_uuid[retry_name]
            except KeyError:
                retry_id = uuidutils.generate_uuid()
                self._create_atom_detail(logbook.RetryDetail, retry_name,
                                         retry_id, retry_version)
            else:
                ad = self._flowdetail.find(retry_id)
                if not isinstance(ad, logbook.RetryDetail):
                    raise exceptions.Duplicate(
                        "Atom detail %s already exists in flow detail %s." %
                        (retry_name, self._flowdetail.name))
            self._set_result_mapping(retry_name, result_mapping)
        return retry_id

    def _create_atom_detail(self, _detail_cls, name, uuid, task_version=None):
        """Add the atom detail to flow detail.

        Atom becomes known to storage by that name and uuid.
        Atom state is set to PENDING.
        """
        ad = _detail_cls(name, uuid)
        ad.state = states.PENDING
        ad.version = task_version
        self._flowdetail.add(ad)
        self._with_connection(self._save_flow_detail)
        self._atom_name_to_uuid[ad.name] = ad.uuid

    @property
    def flow_name(self):
        # This never changes (so no read locking needed).
        return self._flowdetail.name

    @property
    def flow_uuid(self):
        # This never changes (so no read locking needed).
        return self._flowdetail.uuid

    def _save_flow_detail(self, conn):
        # NOTE(harlowja): we need to update our contained flow detail if
        # the result of the update actually added more (aka another process
        # added item to the flow detail).
        self._flowdetail.update(conn.update_flow_details(self._flowdetail))

    def _atomdetail_by_name(self, atom_name, expected_type=None):
        try:
            ad = self._flowdetail.find(self._atom_name_to_uuid[atom_name])
        except KeyError:
            raise exceptions.NotFound("Unknown atom name: %s" % atom_name)
        else:
            # TODO(harlowja): we need to figure out how to get away from doing
            # these kinds of type checks in general (since they likely mean
            # we aren't doing something right).
            if expected_type and not isinstance(ad, expected_type):
                raise TypeError("Atom %s is not of the expected type: %s"
                                % (atom_name,
                                   reflection.get_class_name(expected_type)))
            return ad

    def _save_atom_detail(self, conn, atom_detail):
        # NOTE(harlowja): we need to update our contained atom detail if
        # the result of the update actually added more (aka another process
        # is also modifying the task detail), since python is by reference
        # and the contained atom detail will reflect the old state if we don't
        # do this update.
        atom_detail.update(conn.update_atom_details(atom_detail))

    def get_atom_uuid(self, atom_name):
        """Gets an atoms uuid given a atoms name."""
        with self._lock.read_lock():
            ad = self._atomdetail_by_name(atom_name)
            return ad.uuid

    def set_atom_state(self, atom_name, state):
        """Sets an atoms state."""
        with self._lock.write_lock():
            ad = self._atomdetail_by_name(atom_name)
            ad.state = state
            self._with_connection(self._save_atom_detail, ad)

    def get_atom_state(self, atom_name):
        """Gets the state of an atom given an atoms name."""
        with self._lock.read_lock():
            ad = self._atomdetail_by_name(atom_name)
            return ad.state

    def set_atom_intention(self, atom_name, intention):
        """Sets the intention of an atom given an atoms name."""
        ad = self._atomdetail_by_name(atom_name)
        ad.intention = intention
        self._with_connection(self._save_atom_detail, ad)

    def get_atom_intention(self, atom_name):
        """Gets the intention of an atom given an atoms name."""
        ad = self._atomdetail_by_name(atom_name)
        return ad.intention

    def get_atoms_states(self, atom_names):
        """Gets all atoms states given a set of names."""
        with self._lock.read_lock():
            return dict((name, (self.get_atom_state(name),
                                self.get_atom_intention(name)))
                        for name in atom_names)

    def _update_atom_metadata(self, atom_name, update_with,
                              expected_type=None):
        with self._lock.write_lock():
            ad = self._atomdetail_by_name(atom_name,
                                          expected_type=expected_type)
            if update_with:
                ad.meta.update(update_with)
                self._with_connection(self._save_atom_detail, ad)

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
            'progress': progress,
        }
        if details is not None:
            # NOTE(imelnikov): as we can update progress without
            # updating details (e.g. automatically from engine)
            # we save progress value with details, too.
            if details:
                update_with['progress_details'] = {
                    'at_progress': progress,
                    'details': details,
                }
            else:
                update_with['progress_details'] = None
        self._update_atom_metadata(task_name, update_with,
                                   expected_type=logbook.TaskDetail)

    def get_task_progress(self, task_name):
        """Get the progress of a task given a tasks name.

        :param task_name: tasks name
        :returns: current task progress value
        """
        with self._lock.read_lock():
            ad = self._atomdetail_by_name(task_name,
                                          expected_type=logbook.TaskDetail)
            try:
                return ad.meta['progress']
            except KeyError:
                return 0.0

    def get_task_progress_details(self, task_name):
        """Get the progress details of a task given a tasks name.

        :param task_name: task name
        :returns: None if progress_details not defined, else progress_details
                 dict
        """
        with self._lock.read_lock():
            ad = self._atomdetail_by_name(task_name,
                                          expected_type=logbook.TaskDetail)
            try:
                return ad.meta['progress_details']
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

    def save(self, atom_name, data, state=states.SUCCESS):
        """Put result for atom with id 'uuid' to storage."""
        with self._lock.write_lock():
            ad = self._atomdetail_by_name(atom_name)
            ad.put(state, data)
            if state == states.FAILURE and isinstance(data, failure.Failure):
                # NOTE(imelnikov): failure serialization looses information,
                # so we cache failures here, in atom name -> failure mapping.
                self._failures[ad.name] = data
            else:
                self._check_all_results_provided(ad.name, data)
            self._with_connection(self._save_atom_detail, ad)

    def save_retry_failure(self, retry_name, failed_atom_name, failure):
        """Save subflow failure to retry controller history."""
        with self._lock.write_lock():
            ad = self._atomdetail_by_name(retry_name,
                                          expected_type=logbook.RetryDetail)
            try:
                failures = ad.last_failures
            except exceptions.NotFound as e:
                raise exceptions.StorageFailure("Unable to fetch most recent"
                                                " retry failures so new retry"
                                                " failure can be inserted", e)
            else:
                if failed_atom_name not in failures:
                    failures[failed_atom_name] = failure
                    self._with_connection(self._save_atom_detail, ad)

    def cleanup_retry_history(self, retry_name, state):
        """Cleanup history of retry atom with given name."""
        with self._lock.write_lock():
            ad = self._atomdetail_by_name(retry_name,
                                          expected_type=logbook.RetryDetail)
            ad.state = state
            ad.results = []
            self._with_connection(self._save_atom_detail, ad)

    def _get(self, atom_name, only_last=False):
        with self._lock.read_lock():
            ad = self._atomdetail_by_name(atom_name)
            if ad.failure is not None:
                cached = self._failures.get(atom_name)
                if ad.failure.matches(cached):
                    return cached
                return ad.failure
            if ad.state not in STATES_WITH_RESULTS:
                raise exceptions.NotFound("Result for atom %s is not currently"
                                          " known" % atom_name)
            if only_last:
                return ad.last_results
            else:
                return ad.results

    def get(self, atom_name):
        """Gets the results for an atom with a given name from storage."""
        return self._get(atom_name)

    def get_failures(self):
        """Get list of failures that happened with this flow.

        No order guaranteed.
        """
        with self._lock.read_lock():
            return self._failures.copy()

    def has_failures(self):
        """Returns True if there are failed tasks in the storage."""
        with self._lock.read_lock():
            return bool(self._failures)

    def _reset_atom(self, ad, state):
        if ad.name == self.injector_name:
            return False
        if ad.state == state:
            return False
        ad.reset(state)
        self._failures.pop(ad.name, None)
        return True

    def reset(self, atom_name, state=states.PENDING):
        """Reset atom with given name (if the task is in a given state)."""
        with self._lock.write_lock():
            ad = self._atomdetail_by_name(atom_name)
            if self._reset_atom(ad, state):
                self._with_connection(self._save_atom_detail, ad)

    def inject_atom_args(self, atom_name, pairs):
        """Add *transient* values into storage for a specific atom only.

        This method injects a dictionary/pairs of arguments for an atom so that
        when that atom is scheduled for execution it will have immediate access
        to these arguments.

        NOTE(harlowja): injected atom arguments take precedence over arguments
        provided by predecessor atoms or arguments provided by injecting into
        the flow scope (using the inject() method).
        """
        if atom_name not in self._atom_name_to_uuid:
            raise exceptions.NotFound("Unknown atom name: %s" % atom_name)
        with self._lock.write_lock():
            self._injected_args.setdefault(atom_name, {})
            self._injected_args[atom_name].update(pairs)

    def inject(self, pairs, transient=False):
        """Add values into storage.

        This method should be used to put flow parameters (requirements that
        are not satisfied by any task in the flow) into storage.

        :param: transient save the data in-memory only instead of persisting
                the data to backend storage (useful for resource-like objects
                or similar objects which should *not* be persisted)
        """

        def save_persistent():
            try:
                ad = self._atomdetail_by_name(self.injector_name,
                                              expected_type=logbook.TaskDetail)
            except exceptions.NotFound:
                uuid = uuidutils.generate_uuid()
                self._create_atom_detail(logbook.TaskDetail,
                                         self.injector_name, uuid)
                ad = self._atomdetail_by_name(self.injector_name,
                                              expected_type=logbook.TaskDetail)
                ad.results = dict(pairs)
                ad.state = states.SUCCESS
            else:
                ad.results.update(pairs)
            self._with_connection(self._save_atom_detail, ad)
            return (self.injector_name, six.iterkeys(ad.results))

        def save_transient():
            self._transients.update(pairs)
            return (_TRANSIENT_PROVIDER, six.iterkeys(self._transients))

        with self._lock.write_lock():
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

    def fetch(self, name, many_handler=None):
        """Fetch a named result."""
        # By default we just return the first of many (unless provided
        # a different callback that can translate many results into something
        # more meaningful).
        if many_handler is None:
            many_handler = lambda values: values[0]
        with self._lock.read_lock():
            try:
                providers = self._reverse_mapping[name]
            except KeyError:
                raise exceptions.NotFound("Name %r is not mapped as a"
                                          " produced output by any"
                                          " providers" % name)
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

    def fetch_all(self):
        """Fetch all named results known so far.

        NOTE(harlowja): should be used for debugging and testing purposes.
        """
        def many_handler(values):
            if len(values) > 1:
                return values
            return values[0]
        with self._lock.read_lock():
            results = {}
            for name in six.iterkeys(self._reverse_mapping):
                try:
                    results[name] = self.fetch(name, many_handler=many_handler)
                except exceptions.NotFound:
                    pass
            return results

    def fetch_mapped_args(self, args_mapping,
                          atom_name=None, scope_walker=None,
                          optional_args=None):
        """Fetch arguments for an atom using an atoms argument mapping."""

        def _get_results(looking_for, provider):
            """Gets the results saved for a given provider."""
            try:
                return self._get(provider.name, only_last=True)
            except exceptions.NotFound as e:
                raise exceptions.NotFound(
                    "Expected to be able to find output %r produced"
                    " by %s but was unable to get at that providers"
                    " results" % (looking_for, provider), e)

        def _locate_providers(looking_for, possible_providers):
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
            for atom_names in scope_iter:
                if not atom_names:
                    continue
                providers = []
                for p in possible_providers:
                    if p.name in atom_names:
                        providers.append((p, _get_results(looking_for, p)))
                if providers:
                    return providers
            return []

        with self._lock.read_lock():
            if optional_args is None:
                optional_args = []

            if atom_name and atom_name not in self._atom_name_to_uuid:
                raise exceptions.NotFound("Unknown atom name: %s" % atom_name)
            if not args_mapping:
                return {}

            # The order of lookup is the following:
            #
            # 1. Injected atom specific arguments.
            # 2. Transient injected arguments.
            # 3. Non-transient injected arguments.
            # 4. First scope visited group that produces the named result.
            #    a). The first of that group that actually provided the name
            #        result is selected (if group size is greater than one).
            #
            # Otherwise: blowup! (this will also happen if reading or
            # extracting an expected result fails, since it is better to fail
            # on lookup then provide invalid data from the wrong provider)
            if atom_name:
                injected_args = self._injected_args.get(atom_name, {})
            else:
                injected_args = {}
            mapped_args = {}
            for (bound_name, name) in six.iteritems(args_mapping):
                if LOG.isEnabledFor(logging.BLATHER):
                    if atom_name:
                        LOG.blather("Looking for %r <= %r for atom named: %s",
                                    bound_name, name, atom_name)
                    else:
                        LOG.blather("Looking for %r <= %r", bound_name, name)
                if name in injected_args:
                    value = injected_args[name]
                    mapped_args[bound_name] = value
                    LOG.blather("Matched %r <= %r to %r (from injected"
                                " values)", bound_name, name, value)
                else:
                    try:
                        possible_providers = self._reverse_mapping[name]
                    except KeyError:
                        if bound_name in optional_args:
                            continue
                        raise exceptions.NotFound("Name %r is not mapped as a"
                                                  " produced output by any"
                                                  " providers" % name)
                    # Reduce the possible providers to one that are allowed.
                    providers = _locate_providers(name, possible_providers)
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

    def set_flow_state(self, state):
        """Set flow details state and save it."""
        with self._lock.write_lock():
            self._flowdetail.state = state
            self._with_connection(self._save_flow_detail)

    def get_flow_state(self):
        """Get state from flow details."""
        with self._lock.read_lock():
            state = self._flowdetail.state
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

    def get_retry_history(self, retry_name):
        """Fetch a single retrys history."""
        with self._lock.read_lock():
            ad = self._atomdetail_by_name(retry_name,
                                          expected_type=logbook.RetryDetail)
            return self._translate_into_history(ad)

    def get_retry_histories(self):
        """Fetch all retrys histories."""
        histories = []
        with self._lock.read_lock():
            for ad in self._flowdetail:
                if isinstance(ad, logbook.RetryDetail):
                    histories.append((ad.name,
                                      self._translate_into_history(ad)))
        return histories
