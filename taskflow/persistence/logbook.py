# -*- coding: utf-8 -*-

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#    Copyright (C) 2013 Rackspace Hosting All Rights Reserved.
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
import copy

from oslo_utils import timeutils
from oslo_utils import uuidutils
import six

from taskflow import exceptions as exc
from taskflow import logging
from taskflow import states
from taskflow.types import failure as ft

LOG = logging.getLogger(__name__)


# Internal helpers...


def _copy_function(deep_copy):
    if deep_copy:
        return copy.deepcopy
    else:
        return lambda x: x


def _safe_marshal_time(when):
    if not when:
        return None
    return timeutils.marshall_now(now=when)


def _safe_unmarshal_time(when):
    if not when:
        return None
    return timeutils.unmarshall_time(when)


def _was_failure(state, result):
    return state == states.FAILURE and isinstance(result, ft.Failure)


def _fix_meta(data):
    # Handle the case where older schemas allowed this to be non-dict by
    # correcting this case by replacing it with a dictionary when a non-dict
    # is found.
    meta = data.get('meta')
    if not isinstance(meta, dict):
        meta = {}
    return meta


class LogBook(object):
    """A collection of flow details and associated metadata.

    Typically this class contains a collection of flow detail entries
    for a given engine (or job) so that those entities can track what 'work'
    has been completed for resumption, reverting and miscellaneous tracking
    purposes.

    The data contained within this class need **not** be persisted to the
    backend storage in real time. The data in this class will only be
    guaranteed to be persisted when a save occurs via some backend
    connection.

    NOTE(harlowja): the naming of this class is analogous to a ship's log or a
    similar type of record used in detailing work that has been completed (or
    work that has not been completed).

    :ivar created_at: A ``datetime.datetime`` object of when this logbook
                      was created.
    :ivar updated_at: A ``datetime.datetime`` object of when this logbook
                      was last updated at.
    :ivar meta: A dictionary of meta-data associated with this logbook.
    """
    def __init__(self, name, uuid=None):
        if uuid:
            self._uuid = uuid
        else:
            self._uuid = uuidutils.generate_uuid()
        self._name = name
        self._flowdetails_by_id = {}
        self.created_at = timeutils.utcnow()
        self.updated_at = None
        self.meta = {}

    def add(self, fd):
        """Adds a new flow detail into this logbook.

        NOTE(harlowja): if an existing flow detail exists with the same
        uuid the existing one will be overwritten with the newly provided
        one.

        Does not *guarantee* that the details will be immediately saved.
        """
        self._flowdetails_by_id[fd.uuid] = fd
        self.updated_at = timeutils.utcnow()

    def find(self, flow_uuid):
        """Locate the flow detail corresponding to the given uuid.

        :returns: the flow detail with that uuid
        :rtype: :py:class:`.FlowDetail` (or ``None`` if not found)
        """
        return self._flowdetails_by_id.get(flow_uuid, None)

    def merge(self, lb, deep_copy=False):
        """Merges the current object state with the given ones state.

        If ``deep_copy`` is provided as truthy then the
        local object will use ``copy.deepcopy`` to replace this objects
        local attributes with the provided objects attributes (**only** if
        there is a difference between this objects attributes and the
        provided attributes). If ``deep_copy`` is falsey (the default) then a
        reference copy will occur instead when a difference is detected.

        NOTE(harlowja): If the provided object is this object itself
        then **no** merging is done. Also note that this does **not** merge
        the flow details contained in either.

        :returns: this logbook (freshly merged with the incoming object)
        :rtype: :py:class:`.LogBook`
        """
        if lb is self:
            return self
        copy_fn = _copy_function(deep_copy)
        if self.meta != lb.meta:
            self.meta = copy_fn(lb.meta)
        if lb.created_at != self.created_at:
            self.created_at = copy_fn(lb.created_at)
        if lb.updated_at != self.updated_at:
            self.updated_at = copy_fn(lb.updated_at)
        return self

    def to_dict(self, marshal_time=False):
        """Translates the internal state of this object to a ``dict``.

        NOTE(harlowja): The returned ``dict`` does **not** include any
        contained flow details.

        :returns: this logbook in ``dict`` form
        """
        if not marshal_time:
            marshal_fn = lambda x: x
        else:
            marshal_fn = _safe_marshal_time
        return {
            'name': self.name,
            'meta': self.meta,
            'uuid': self.uuid,
            'updated_at': marshal_fn(self.updated_at),
            'created_at': marshal_fn(self.created_at),
        }

    @classmethod
    def from_dict(cls, data, unmarshal_time=False):
        """Translates the given ``dict`` into an instance of this class.

        NOTE(harlowja): the ``dict`` provided should come from a prior
        call to :meth:`.to_dict`.

        :returns: a new logbook
        :rtype: :py:class:`.LogBook`
        """
        if not unmarshal_time:
            unmarshal_fn = lambda x: x
        else:
            unmarshal_fn = _safe_unmarshal_time
        obj = cls(data['name'], uuid=data['uuid'])
        obj.updated_at = unmarshal_fn(data['updated_at'])
        obj.created_at = unmarshal_fn(data['created_at'])
        obj.meta = _fix_meta(data)
        return obj

    @property
    def uuid(self):
        """The unique identifer of this logbook."""
        return self._uuid

    @property
    def name(self):
        """The name of this logbook."""
        return self._name

    def __iter__(self):
        for fd in six.itervalues(self._flowdetails_by_id):
            yield fd

    def __len__(self):
        return len(self._flowdetails_by_id)

    def copy(self, retain_contents=True):
        """Copies this logbook.

        Creates a shallow copy of this logbook. If this logbook contains
        flow details and ``retain_contents`` is truthy (the default) then
        the flow details container will be shallow copied (the flow details
        contained there-in will **not** be copied). If ``retain_contents`` is
        falsey then the copied logbook will have **no** contained flow
        details (but it will have the rest of the local objects attributes
        copied).

        :returns: a new logbook
        :rtype: :py:class:`.LogBook`
        """
        clone = copy.copy(self)
        if not retain_contents:
            clone._flowdetails_by_id = {}
        else:
            clone._flowdetails_by_id = self._flowdetails_by_id.copy()
        if self.meta:
            clone.meta = self.meta.copy()
        return clone


class FlowDetail(object):
    """A collection of atom details and associated metadata.

    Typically this class contains a collection of atom detail entries that
    represent the atoms in a given flow structure (along with any other needed
    metadata relevant to that flow).

    The data contained within this class need **not** be persisted to the
    backend storage in real time. The data in this class will only be
    guaranteed to be persisted when a save (or update) occurs via some backend
    connection.

    :ivar state: The state of the flow associated with this flow detail.
    :ivar meta: A dictionary of meta-data associated with this flow detail.
    """
    def __init__(self, name, uuid):
        self._uuid = uuid
        self._name = name
        self._atomdetails_by_id = {}
        self.state = None
        self.meta = {}

    def update(self, fd):
        """Updates the objects state to be the same as the given one.

        This will assign the private and public attributes of the given
        flow detail directly to this object (replacing any existing
        attributes in this object; even if they are the **same**).

        NOTE(harlowja): If the provided object is this object itself
        then **no** update is done.

        :returns: this flow detail
        :rtype: :py:class:`.FlowDetail`
        """
        if fd is self:
            return self
        self._atomdetails_by_id = fd._atomdetails_by_id
        self.state = fd.state
        self.meta = fd.meta
        return self

    def merge(self, fd, deep_copy=False):
        """Merges the current object state with the given one's state.

        If ``deep_copy`` is provided as truthy then the
        local object will use ``copy.deepcopy`` to replace this objects
        local attributes with the provided objects attributes (**only** if
        there is a difference between this objects attributes and the
        provided attributes). If ``deep_copy`` is falsey (the default) then a
        reference copy will occur instead when a difference is detected.

        NOTE(harlowja): If the provided object is this object itself
        then **no** merging is done. Also this does **not** merge the atom
        details contained in either.

        :returns: this flow detail (freshly merged with the incoming object)
        :rtype: :py:class:`.FlowDetail`
        """
        if fd is self:
            return self
        copy_fn = _copy_function(deep_copy)
        if self.meta != fd.meta:
            self.meta = copy_fn(fd.meta)
        if self.state != fd.state:
            # NOTE(imelnikov): states are just strings, no need to copy.
            self.state = fd.state
        return self

    def copy(self, retain_contents=True):
        """Copies this flow detail.

        Creates a shallow copy of this flow detail. If this detail contains
        flow details and ``retain_contents`` is truthy (the default) then
        the atom details container will be shallow copied (the atom details
        contained there-in will **not** be copied). If ``retain_contents`` is
        falsey then the copied flow detail will have **no** contained atom
        details (but it will have the rest of the local objects attributes
        copied).

        :returns: a new flow detail
        :rtype: :py:class:`.FlowDetail`
        """
        clone = copy.copy(self)
        if not retain_contents:
            clone._atomdetails_by_id = {}
        else:
            clone._atomdetails_by_id = self._atomdetails_by_id.copy()
        if self.meta:
            clone.meta = self.meta.copy()
        return clone

    def to_dict(self):
        """Translates the internal state of this object to a ``dict``.

        NOTE(harlowja): The returned ``dict`` does **not** include any
        contained atom details.

        :returns: this flow detail in ``dict`` form
        """
        return {
            'name': self.name,
            'meta': self.meta,
            'state': self.state,
            'uuid': self.uuid,
        }

    @classmethod
    def from_dict(cls, data):
        """Translates the given ``dict`` into an instance of this class.

        NOTE(harlowja): the ``dict`` provided should come from a prior
        call to :meth:`.to_dict`.

        :returns: a new flow detail
        :rtype: :py:class:`.FlowDetail`
        """
        obj = cls(data['name'], data['uuid'])
        obj.state = data.get('state')
        obj.meta = _fix_meta(data)
        return obj

    def add(self, ad):
        """Adds a new atom detail into this flow detail.

        NOTE(harlowja): if an existing atom detail exists with the same
        uuid the existing one will be overwritten with the newly provided
        one.

        Does not *guarantee* that the details will be immediately saved.
        """
        self._atomdetails_by_id[ad.uuid] = ad

    def find(self, ad_uuid):
        """Locate the atom detail corresponding to the given uuid.

        :returns: the atom detail with that uuid
        :rtype: :py:class:`.AtomDetail` (or ``None`` if not found)
        """
        return self._atomdetails_by_id.get(ad_uuid)

    @property
    def uuid(self):
        """The unique identifer of this flow detail."""
        return self._uuid

    @property
    def name(self):
        """The name of this flow detail."""
        return self._name

    def __iter__(self):
        for ad in six.itervalues(self._atomdetails_by_id):
            yield ad

    def __len__(self):
        return len(self._atomdetails_by_id)


@six.add_metaclass(abc.ABCMeta)
class AtomDetail(object):
    """A collection of atom specific runtime information and metadata.

    This is a base **abstract** class that contains attributes that are used
    to connect a atom to the persistence layer before, during, or after it is
    running. It includes any results it may have produced, any state that it
    may be in (for example ``FAILURE``), any exception that occurred when
    running, and any associated stacktrace that may have occurring during an
    exception being thrown. It may also contain any other metadata that
    should also be stored along-side the details about the connected atom.

    The data contained within this class need **not** be persisted to the
    backend storage in real time. The data in this class will only be
    guaranteed to be persisted when a save (or update) occurs via some backend
    connection.

    :ivar state: The state of the atom associated with this atom detail.
    :ivar intention: The execution strategy of the atom associated
                     with this atom detail (used by an engine/others to
                     determine if the associated atom needs to be
                     executed, reverted, retried and so-on).
    :ivar meta: A dictionary of meta-data associated with this atom detail.
    :ivar version: A version tuple or string that represents the
                   atom version this atom detail is associated with (typically
                   used for introspection and any data migration
                   strategies).
    :ivar results: Any results the atom produced from either its
                   ``execute`` method or from other sources.
    :ivar failure: If the atom failed (possibly due to its ``execute``
                   method raising) this will be a
                   :py:class:`~taskflow.types.failure.Failure` object that
                   represents that failure (if there was no failure this
                   will be set to none).
    """

    def __init__(self, name, uuid):
        self._uuid = uuid
        self._name = name
        self.state = None
        self.intention = states.EXECUTE
        self.results = None
        self.failure = None
        self.meta = {}
        self.version = None

    @property
    def last_results(self):
        """Gets the atoms last result.

        If the atom has produced many results (for example if it has been
        retried, reverted, executed and ...) this returns the last one of
        many results.
        """
        return self.results

    def update(self, ad):
        """Updates the object's state to be the same as the given one.

        This will assign the private and public attributes of the given
        atom detail directly to this object (replacing any existing
        attributes in this object; even if they are the **same**).

        NOTE(harlowja): If the provided object is this object itself
        then **no** update is done.

        :returns: this atom detail
        :rtype: :py:class:`.AtomDetail`
        """
        if ad is self:
            return self
        self.state = ad.state
        self.intention = ad.intention
        self.meta = ad.meta
        self.failure = ad.failure
        self.results = ad.results
        self.version = ad.version
        return self

    @abc.abstractmethod
    def merge(self, other, deep_copy=False):
        """Merges the current object state with the given ones state.

        If ``deep_copy`` is provided as truthy then the
        local object will use ``copy.deepcopy`` to replace this objects
        local attributes with the provided objects attributes (**only** if
        there is a difference between this objects attributes and the
        provided attributes). If ``deep_copy`` is falsey (the default) then a
        reference copy will occur instead when a difference is detected.

        NOTE(harlowja): If the provided object is this object itself
        then **no** merging is done. Do note that **no** results are merged
        in this method. That operation **must** to be the responsibilty of
        subclasses to implement and override this abstract method
        and provide that merging themselves as they see fit.

        :returns: this atom detail (freshly merged with the incoming object)
        :rtype: :py:class:`.AtomDetail`
        """
        copy_fn = _copy_function(deep_copy)
        # NOTE(imelnikov): states and intentions are just strings,
        # so there is no need to copy them (strings are immutable in python).
        self.state = other.state
        self.intention = other.intention
        if self.failure != other.failure:
            # NOTE(imelnikov): we can't just deep copy Failures, as they
            # contain tracebacks, which are not copyable.
            if other.failure:
                if deep_copy:
                    self.failure = other.failure.copy()
                else:
                    self.failure = other.failure
            else:
                self.failure = None
        if self.meta != other.meta:
            self.meta = copy_fn(other.meta)
        if self.version != other.version:
            self.version = copy_fn(other.version)
        return self

    @abc.abstractmethod
    def put(self, state, result):
        """Puts a result (acquired in the given state) into this detail."""

    def to_dict(self):
        """Translates the internal state of this object to a ``dict``.

        :returns: this atom detail in ``dict`` form
        """
        if self.failure:
            failure = self.failure.to_dict()
        else:
            failure = None
        return {
            'failure': failure,
            'meta': self.meta,
            'name': self.name,
            'results': self.results,
            'state': self.state,
            'version': self.version,
            'intention': self.intention,
            'uuid': self.uuid,
        }

    @classmethod
    def from_dict(cls, data):
        """Translates the given ``dict`` into an instance of this class.

        NOTE(harlowja): the ``dict`` provided should come from a prior
        call to :meth:`.to_dict`.

        :returns: a new atom detail
        :rtype: :py:class:`.AtomDetail`
        """
        obj = cls(data['name'], data['uuid'])
        obj.state = data.get('state')
        obj.intention = data.get('intention')
        obj.results = data.get('results')
        obj.version = data.get('version')
        obj.meta = _fix_meta(data)
        failure = data.get('failure')
        if failure:
            obj.failure = ft.Failure.from_dict(failure)
        return obj

    @property
    def uuid(self):
        """The unique identifer of this atom detail."""
        return self._uuid

    @property
    def name(self):
        """The name of this atom detail."""
        return self._name

    @abc.abstractmethod
    def reset(self, state):
        """Resets this atom detail and sets ``state`` attribute value."""

    @abc.abstractmethod
    def copy(self):
        """Copies this atom detail."""


class TaskDetail(AtomDetail):
    """A task detail (an atom detail typically associated with a |tt| atom).

    .. |tt| replace:: :py:class:`~taskflow.task.BaseTask`
    """

    def reset(self, state):
        """Resets this task detail and sets ``state`` attribute value.

        This sets any previously set ``results`` and ``failure`` attributes
        back to ``None`` and sets the state to the provided one, as well as
        setting this task details ``intention`` attribute to ``EXECUTE``.
        """
        self.results = None
        self.failure = None
        self.state = state
        self.intention = states.EXECUTE

    def put(self, state, result):
        """Puts a result (acquired in the given state) into this detail.

        If the result is a :py:class:`~taskflow.types.failure.Failure` object
        then the ``failure`` attribute will be set (and the ``results``
        attribute will be set to ``None``); if the result is not a
        :py:class:`~taskflow.types.failure.Failure` object then the
        ``results`` attribute will be set (and the ``failure`` attribute
        will be set to ``None``). In either case the ``state``
        attribute will be set to the provided state.
        """
        self.state = state
        if _was_failure(state, result):
            self.failure = result
            self.results = None
        else:
            self.results = result
            self.failure = None

    def merge(self, other, deep_copy=False):
        """Merges the current task detail with the given one.

        NOTE(harlowja): This merge does **not** copy and replace
        the ``results`` attribute if it differs. Instead the current
        objects ``results`` attribute directly becomes (via assignment) the
        other objects ``results`` attribute. Also note that if the provided
        object is this object itself then **no** merging is done.

        See: https://bugs.launchpad.net/taskflow/+bug/1452978 for
        what happens if this is copied at a deeper level (for example by
        using ``copy.deepcopy`` or by using ``copy.copy``).

        :returns: this task detail (freshly merged with the incoming object)
        :rtype: :py:class:`.TaskDetail`
        """
        if not isinstance(other, TaskDetail):
            raise exc.NotImplementedError("Can only merge with other"
                                          " task details")
        if other is self:
            return self
        super(TaskDetail, self).merge(other, deep_copy=deep_copy)
        if self.results != other.results:
            self.results = other.results
        return self

    def copy(self):
        """Copies this task detail.

        Creates a shallow copy of this task detail (any meta-data and
        version information that this object maintains is shallow
        copied via ``copy.copy``).

        NOTE(harlowja): This copy does **not** perform ``copy.copy`` on
        the ``results`` attribute of this object (before assigning to the
        copy). Instead the current objects ``results`` attribute directly
        becomes (via assignment) the copied objects ``results`` attribute.

        See: https://bugs.launchpad.net/taskflow/+bug/1452978 for
        what happens if this is copied at a deeper level (for example by
        using ``copy.deepcopy`` or by using ``copy.copy``).

        :returns: a new task detail
        :rtype: :py:class:`.TaskDetail`
        """
        clone = copy.copy(self)
        clone.results = self.results
        if self.meta:
            clone.meta = self.meta.copy()
        if self.version:
            clone.version = copy.copy(self.version)
        return clone


class RetryDetail(AtomDetail):
    """A retry detail (an atom detail typically associated with a |rt| atom).

    .. |rt| replace:: :py:class:`~taskflow.retry.Retry`
    """

    def __init__(self, name, uuid):
        super(RetryDetail, self).__init__(name, uuid)
        self.results = []

    def reset(self, state):
        """Resets this retry detail and sets ``state`` attribute value.

        This sets any previously added ``results`` back to an empty list
        and resets the ``failure`` attribute back to ``None`` and sets the
        state to the provided one, as well as setting this atom
        details ``intention`` attribute to ``EXECUTE``.
        """
        self.results = []
        self.failure = None
        self.state = state
        self.intention = states.EXECUTE

    def copy(self):
        """Copies this retry detail.

        Creates a shallow copy of this retry detail (any meta-data and
        version information that this object maintains is shallow
        copied via ``copy.copy``).

        NOTE(harlowja): This copy does **not** copy
        the incoming objects ``results`` attribute. Instead this
        objects ``results`` attribute list is iterated over and a new list
        is constructed with each ``(data, failures)`` element in that list
        having its ``failures`` (a dictionary of each named
        :py:class:`~taskflow.types.failure.Failure` object that
        occured) copied but its ``data`` is left untouched. After
        this is done that new list becomes (via assignment) the cloned
        objects ``results`` attribute.

        See: https://bugs.launchpad.net/taskflow/+bug/1452978 for
        what happens if the ``data`` in ``results`` is copied at a
        deeper level (for example by using ``copy.deepcopy`` or by
        using ``copy.copy``).

        :returns: a new retry detail
        :rtype: :py:class:`.RetryDetail`
        """
        clone = copy.copy(self)
        results = []
        # NOTE(imelnikov): we can't just deep copy Failures, as they
        # contain tracebacks, which are not copyable.
        for (data, failures) in self.results:
            copied_failures = {}
            for (key, failure) in six.iteritems(failures):
                copied_failures[key] = failure
            results.append((data, copied_failures))
        clone.results = results
        if self.meta:
            clone.meta = self.meta.copy()
        if self.version:
            clone.version = copy.copy(self.version)
        return clone

    @property
    def last_results(self):
        """The last result that was produced."""
        try:
            return self.results[-1][0]
        except IndexError:
            exc.raise_with_cause(exc.NotFound, "Last results not found")

    @property
    def last_failures(self):
        """The last failure dictionary that was produced.

        NOTE(harlowja): This is **not** the same as the
        local ``failure`` attribute as the obtained failure dictionary in
        the ``results`` attribute (which is what this returns) is from
        associated atom failures (which is different from the directly
        related failure of the retry unit associated with this
        atom detail).
        """
        try:
            return self.results[-1][1]
        except IndexError:
            exc.raise_with_cause(exc.NotFound, "Last failures not found")

    def put(self, state, result):
        """Puts a result (acquired in the given state) into this detail.

        If the result is a :py:class:`~taskflow.types.failure.Failure` object
        then the ``failure`` attribute will be set; if the result is not a
        :py:class:`~taskflow.types.failure.Failure` object then the
        ``results`` attribute will be appended to (and the ``failure``
        attribute will be set to ``None``). In either case the ``state``
        attribute will be set to the provided state.
        """
        # Do not clean retry history (only on reset does this happen).
        self.state = state
        if _was_failure(state, result):
            self.failure = result
        else:
            self.results.append((result, {}))
            self.failure = None

    @classmethod
    def from_dict(cls, data):
        """Translates the given ``dict`` into an instance of this class."""

        def decode_results(results):
            if not results:
                return []
            new_results = []
            for (data, failures) in results:
                new_failures = {}
                for (key, data) in six.iteritems(failures):
                    new_failures[key] = ft.Failure.from_dict(data)
                new_results.append((data, new_failures))
            return new_results

        obj = super(RetryDetail, cls).from_dict(data)
        obj.results = decode_results(obj.results)
        return obj

    def to_dict(self):
        """Translates the internal state of this object to a ``dict``."""

        def encode_results(results):
            if not results:
                return []
            new_results = []
            for (data, failures) in results:
                new_failures = {}
                for (key, failure) in six.iteritems(failures):
                    new_failures[key] = failure.to_dict()
                new_results.append((data, new_failures))
            return new_results

        base = super(RetryDetail, self).to_dict()
        base['results'] = encode_results(base.get('results'))
        return base

    def merge(self, other, deep_copy=False):
        """Merges the current retry detail with the given one.

        NOTE(harlowja): This merge does **not** deep copy
        the incoming objects ``results`` attribute (if it differs). Instead
        the incoming objects ``results`` attribute list is **always** iterated
        over and a new list is constructed with
        each ``(data, failures)`` element in that list having
        its ``failures`` (a dictionary of each named
        :py:class:`~taskflow.types.failure.Failure` objects that
        occurred) copied but its ``data`` is left untouched. After
        this is done that new list becomes (via assignment) this
        objects ``results`` attribute. Also note that if the provided object
        is this object itself then **no** merging is done.

        See: https://bugs.launchpad.net/taskflow/+bug/1452978 for
        what happens if the ``data`` in ``results`` is copied at a
        deeper level (for example by using ``copy.deepcopy`` or by
        using ``copy.copy``).

        :returns: this retry detail (freshly merged with the incoming object)
        :rtype: :py:class:`.RetryDetail`
        """
        if not isinstance(other, RetryDetail):
            raise exc.NotImplementedError("Can only merge with other"
                                          " retry details")
        if other is self:
            return self
        super(RetryDetail, self).merge(other, deep_copy=deep_copy)
        results = []
        # NOTE(imelnikov): we can't just deep copy Failures, as they
        # contain tracebacks, which are not copyable.
        for (data, failures) in other.results:
            copied_failures = {}
            for (key, failure) in six.iteritems(failures):
                if deep_copy:
                    copied_failures[key] = failure.copy()
                else:
                    copied_failures[key] = failure
            results.append((data, copied_failures))
        self.results = results
        return self


_DETAIL_TO_NAME = {
    RetryDetail: 'RETRY_DETAIL',
    TaskDetail: 'TASK_DETAIL',
}
_NAME_TO_DETAIL = dict((name, cls)
                       for (cls, name) in six.iteritems(_DETAIL_TO_NAME))
ATOM_TYPES = list(six.iterkeys(_NAME_TO_DETAIL))


def atom_detail_class(atom_type):
    try:
        return _NAME_TO_DETAIL[atom_type]
    except KeyError:
        raise TypeError("Unknown atom type '%s'" % (atom_type))


def atom_detail_type(atom_detail):
    try:
        return _DETAIL_TO_NAME[type(atom_detail)]
    except KeyError:
        raise TypeError("Unknown atom '%s' (%s)"
                        % (atom_detail, type(atom_detail)))
