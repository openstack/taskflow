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
    """A container of flow details, a name and associated metadata.

    Typically this class contains a collection of flow detail entries
    for a given engine (or job) so that those entities can track what 'work'
    has been completed for resumption, reverting and miscellaneous tracking
    purposes.

    The data contained within this class need *not* be backed by the backend
    storage in real time. The data in this class will only be guaranteed to be
    persisted when a save occurs via some backend connection.

    NOTE(harlowja): the naming of this class is analogous to a ships log or a
    similar type of record used in detailing work that been completed (or work
    that has not been completed).
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
        """Adds a new entry to the underlying logbook.

        Does not *guarantee* that the details will be immediately saved.
        """
        self._flowdetails_by_id[fd.uuid] = fd
        self.updated_at = timeutils.utcnow()

    def find(self, flow_uuid):
        return self._flowdetails_by_id.get(flow_uuid, None)

    def merge(self, lb, deep_copy=False):
        """Merges the current object state with the given ones state.

        NOTE(harlowja): Does not merge the flow details contained in either.
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
        """Translates the internal state of this object to a dictionary.

        NOTE(harlowja): Does not include the contained flow details.
        """
        if not marshal_time:
            marshal_fn = lambda x: x
        else:
            marshal_fn = _safe_marshal_time
        data = {
            'name': self.name,
            'meta': self.meta,
            'uuid': self.uuid,
            'updated_at': marshal_fn(self.updated_at),
            'created_at': marshal_fn(self.created_at),
        }
        return data

    @classmethod
    def from_dict(cls, data, unmarshal_time=False):
        """Translates the given dictionary into an instance of this class."""
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
        return self._uuid

    @property
    def name(self):
        return self._name

    def __iter__(self):
        for fd in six.itervalues(self._flowdetails_by_id):
            yield fd

    def __len__(self):
        return len(self._flowdetails_by_id)

    def copy(self, retain_contents=True):
        """Copies/clones this log book."""
        clone = copy.copy(self)
        if not retain_contents:
            clone._flowdetails_by_id = {}
        else:
            clone._flowdetails_by_id = self._flowdetails_by_id.copy()
        if self.meta:
            clone.meta = self.meta.copy()
        return clone


class FlowDetail(object):
    """A container of atom details, a name and associated metadata.

    Typically this class contains a collection of atom detail entries that
    represent the atoms in a given flow structure (along with any other needed
    metadata relevant to that flow).

    The data contained within this class need *not* be backed by the backend
    storage in real time. The data in this class will only be guaranteed to be
    persisted when a save/update occurs via some backend connection.
    """
    def __init__(self, name, uuid):
        self._uuid = uuid
        self._name = name
        self._atomdetails_by_id = {}
        self.state = None
        self.meta = {}

    def update(self, fd):
        """Updates the objects state to be the same as the given one."""
        if fd is self:
            return self
        self._atomdetails_by_id = fd._atomdetails_by_id
        self.state = fd.state
        self.meta = fd.meta
        return self

    def merge(self, fd, deep_copy=False):
        """Merges the current object state with the given ones state.

        NOTE(harlowja): Does not merge the atom details contained in either.
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
        """Copies/clones this flow detail."""
        clone = copy.copy(self)
        if not retain_contents:
            clone._atomdetails_by_id = {}
        else:
            clone._atomdetails_by_id = self._atomdetails_by_id.copy()
        if self.meta:
            clone.meta = self.meta.copy()
        return clone

    def to_dict(self):
        """Translates the internal state of this object to a dictionary.

        NOTE(harlowja): Does not include the contained atom details.
        """
        return {
            'name': self.name,
            'meta': self.meta,
            'state': self.state,
            'uuid': self.uuid,
        }

    @classmethod
    def from_dict(cls, data):
        """Translates the given data into an instance of this class."""
        obj = cls(data['name'], data['uuid'])
        obj.state = data.get('state')
        obj.meta = _fix_meta(data)
        return obj

    def add(self, ad):
        self._atomdetails_by_id[ad.uuid] = ad

    def find(self, ad_uuid):
        return self._atomdetails_by_id.get(ad_uuid)

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name

    def __iter__(self):
        for ad in six.itervalues(self._atomdetails_by_id):
            yield ad

    def __len__(self):
        return len(self._atomdetails_by_id)


@six.add_metaclass(abc.ABCMeta)
class AtomDetail(object):
    """A base container of atom specific runtime information and metadata.

    This is a base class that contains attributes that are used to connect
    a atom to the persistence layer during, after, or before it is running
    including any results it may have produced, any state that it may be
    in (failed for example), any exception that occurred when running and any
    associated stacktrace that may have occurring during that exception being
    thrown and any other metadata that should be stored along-side the details
    about the connected atom.

    The data contained within this class need *not* backed by the backend
    storage in real time. The data in this class will only be guaranteed to be
    persisted when a save/update occurs via some backend connection.
    """
    def __init__(self, name, uuid):
        self._uuid = uuid
        self._name = name
        # TODO(harlowja): decide if these should be passed in and therefore
        # immutable or let them be assigned?
        #
        # The state the atom was last in.
        self.state = None
        # The intention of action that would be applied to the atom.
        self.intention = states.EXECUTE
        # The results it may have produced (useful for reverting).
        self.results = None
        # An Failure object that holds exception the atom may have thrown
        # (or part of it), useful for knowing what failed.
        self.failure = None
        self.meta = {}
        # The version of the atom this atom details was associated with which
        # is quite useful for determining what versions of atoms this detail
        # information can be associated with.
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
        """Updates the objects state to be the same as the given one."""
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
        """Merges the current object state with the given ones state."""
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
    def to_dict(self):
        """Translates the internal state of this object to a dictionary."""

    @abc.abstractmethod
    def put(self, state, result):
        """Puts a result (acquired in the given state) into this detail."""

    def _to_dict_shared(self):
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

    def _from_dict_shared(self, data):
        self.state = data.get('state')
        self.intention = data.get('intention')
        self.results = data.get('results')
        self.version = data.get('version')
        self.meta = _fix_meta(data)
        failure = data.get('failure')
        if failure:
            self.failure = ft.Failure.from_dict(failure)

    @property
    def uuid(self):
        return self._uuid

    @property
    def name(self):
        return self._name

    @abc.abstractmethod
    def reset(self, state):
        """Resets detail results and failures."""


class TaskDetail(AtomDetail):
    """This class represents a task detail for flow task object."""

    def __init__(self, name, uuid):
        super(TaskDetail, self).__init__(name, uuid)

    def reset(self, state):
        self.results = None
        self.failure = None
        self.state = state
        self.intention = states.EXECUTE

    def put(self, state, result):
        self.state = state
        if _was_failure(state, result):
            self.failure = result
            self.results = None
        else:
            self.results = result
            self.failure = None

    @classmethod
    def from_dict(cls, data):
        """Translates the given data into an instance of this class."""
        obj = cls(data['name'], data['uuid'])
        obj._from_dict_shared(data)
        return obj

    def to_dict(self):
        """Translates the internal state of this object to a dictionary."""
        return self._to_dict_shared()

    def merge(self, other, deep_copy=False):
        """Merges the current object state with the given ones state."""
        if not isinstance(other, TaskDetail):
            raise exc.NotImplementedError("Can only merge with other"
                                          " task details")
        if other is self:
            return self
        super(TaskDetail, self).merge(other, deep_copy=deep_copy)
        copy_fn = _copy_function(deep_copy)
        if self.results != other.results:
            self.results = copy_fn(other.results)
        return self

    def copy(self):
        """Copies/clones this task detail."""
        clone = copy.copy(self)
        clone.results = copy.copy(self.results)
        if self.meta:
            clone.meta = self.meta.copy()
        if self.version:
            clone.version = copy.copy(self.version)
        return clone


class RetryDetail(AtomDetail):
    """This class represents a retry detail for retry controller object."""
    def __init__(self, name, uuid):
        super(RetryDetail, self).__init__(name, uuid)
        self.results = []

    def reset(self, state):
        self.results = []
        self.failure = None
        self.state = state
        self.intention = states.EXECUTE

    def copy(self):
        """Copies/clones this retry detail."""
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
        try:
            return self.results[-1][0]
        except IndexError as e:
            raise exc.NotFound("Last results not found", e)

    @property
    def last_failures(self):
        try:
            return self.results[-1][1]
        except IndexError as e:
            raise exc.NotFound("Last failures not found", e)

    def put(self, state, result):
        # Do not clean retry history (only on reset does this happen).
        self.state = state
        if _was_failure(state, result):
            self.failure = result
        else:
            self.results.append((result, {}))
            self.failure = None

    @classmethod
    def from_dict(cls, data):
        """Translates the given data into an instance of this class."""

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

        obj = cls(data['name'], data['uuid'])
        obj._from_dict_shared(data)
        obj.results = decode_results(obj.results)
        return obj

    def to_dict(self):
        """Translates the internal state of this object to a dictionary."""

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

        base = self._to_dict_shared()
        base['results'] = encode_results(base.get('results'))
        return base

    def merge(self, other, deep_copy=False):
        """Merges the current object state with the given ones state."""
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
