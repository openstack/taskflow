# -*- coding: utf-8 -*-

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
import collections
import itertools

try:
    from collections import OrderedDict  # noqa
except ImportError:
    from ordereddict import OrderedDict  # noqa

from oslo_utils import reflection
import six
from six.moves import zip as compat_zip

from taskflow.types import sets
from taskflow.utils import misc


# Helper types tuples...
_sequence_types = (list, tuple, collections.Sequence)
_set_types = (set, collections.Set)


def _save_as_to_mapping(save_as):
    """Convert save_as to mapping name => index.

    Result should follow storage convention for mappings.
    """
    # TODO(harlowja): we should probably document this behavior & convention
    # outside of code so that it's more easily understandable, since what an
    # atom returns is pretty crucial for other later operations.
    if save_as is None:
        return OrderedDict()
    if isinstance(save_as, six.string_types):
        # NOTE(harlowja): this means that your atom will only return one item
        # instead of a dictionary-like object or a indexable object (like a
        # list or tuple).
        return OrderedDict([(save_as, None)])
    elif isinstance(save_as, _sequence_types):
        # NOTE(harlowja): this means that your atom will return a indexable
        # object, like a list or tuple and the results can be mapped by index
        # to that tuple/list that is returned for others to use.
        return OrderedDict((key, num) for num, key in enumerate(save_as))
    elif isinstance(save_as, _set_types):
        # NOTE(harlowja): in the case where a set is given we will not be
        # able to determine the numeric ordering in a reliable way (since it
        # may be an unordered set) so the only way for us to easily map the
        # result of the atom will be via the key itself.
        return OrderedDict((key, key) for key in save_as)
    else:
        raise TypeError('Atom provides parameter '
                        'should be str, set or tuple/list, not %r' % save_as)


def _build_rebind_dict(args, rebind_args):
    """Build a argument remapping/rebinding dictionary.

    This dictionary allows an atom to declare that it will take a needed
    requirement bound to a given name with another name instead (mapping the
    new name onto the required name).
    """
    if rebind_args is None:
        return OrderedDict()
    elif isinstance(rebind_args, (list, tuple)):
        rebind = OrderedDict(compat_zip(args, rebind_args))
        if len(args) < len(rebind_args):
            rebind.update((a, a) for a in rebind_args[len(args):])
        return rebind
    elif isinstance(rebind_args, dict):
        return rebind_args
    else:
        raise TypeError("Invalid rebind value '%s' (%s)"
                        % (rebind_args, type(rebind_args)))


def _build_arg_mapping(atom_name, reqs, rebind_args, function, do_infer,
                       ignore_list=None):
    """Builds an input argument mapping for a given function.

    Given a function, its requirements and a rebind mapping this helper
    function will build the correct argument mapping for the given function as
    well as verify that the final argument mapping does not have missing or
    extra arguments (where applicable).
    """

    # Build a list of required arguments based on function signature.
    req_args = reflection.get_callable_args(function, required_only=True)
    all_args = reflection.get_callable_args(function, required_only=False)

    # Remove arguments that are part of ignore list.
    if ignore_list:
        for arg in ignore_list:
            if arg in req_args:
                req_args.remove(arg)
    else:
        ignore_list = []

    # Build the required names.
    required = OrderedDict()

    # Add required arguments to required mappings if inference is enabled.
    if do_infer:
        required.update((a, a) for a in req_args)

    # Add additional manually provided requirements to required mappings.
    if reqs:
        if isinstance(reqs, six.string_types):
            required.update({reqs: reqs})
        else:
            required.update((a, a) for a in reqs)

    # Update required mappings values based on rebinding of arguments names.
    required.update(_build_rebind_dict(req_args, rebind_args))

    # Determine if there are optional arguments that we may or may not take.
    if do_infer:
        opt_args = sets.OrderedSet(all_args)
        opt_args = opt_args - set(itertools.chain(six.iterkeys(required),
                                                  iter(ignore_list)))
        optional = OrderedDict((a, a) for a in opt_args)
    else:
        optional = OrderedDict()

    # Check if we are given some extra arguments that we aren't able to accept.
    if not reflection.accepts_kwargs(function):
        extra_args = sets.OrderedSet(six.iterkeys(required))
        extra_args -= all_args
        if extra_args:
            raise ValueError('Extra arguments given to atom %s: %s'
                             % (atom_name, list(extra_args)))

    # NOTE(imelnikov): don't use set to preserve order in error message
    missing_args = [arg for arg in req_args if arg not in required]
    if missing_args:
        raise ValueError('Missing arguments for atom %s: %s'
                         % (atom_name, missing_args))
    return required, optional


@six.add_metaclass(abc.ABCMeta)
class Atom(object):
    """An unit of work that causes a flow to progress (in some manner).

    An atom is a named object that operates with input data to perform
    some action that furthers the overall flows progress. It usually also
    produces some of its own named output as a result of this process.

    :param name: Meaningful name for this atom, should be something that is
                 distinguishable and understandable for notification,
                 debugging, storing and any other similar purposes.
    :param provides: A set, string or list of items that
                     this will be providing (or could provide) to others, used
                     to correlate and associate the thing/s this atom
                     produces, if it produces anything at all.
    :param inject: An *immutable* input_name => value dictionary which
                   specifies  any initial inputs that should be automatically
                   injected into the atoms scope before the atom execution
                   commences (this allows for providing atom *local* values
                   that do not need to be provided by other atoms/dependents).
    :ivar version: An *immutable* version that associates version information
                   with this atom. It can be useful in resuming older versions
                   of atoms. Standard major, minor versioning concepts
                   should apply.
    :ivar save_as: An *immutable* output ``resource`` name
                   :py:class:`.OrderedDict` this atom produces that other
                   atoms may depend on this atom providing. The format is
                   output index (or key when a dictionary is returned from
                   the execute method) to stored argument name.
    :ivar rebind: An *immutable* input ``resource`` :py:class:`.OrderedDict`
                  that can be used to alter the inputs given to this atom. It
                  is typically used for mapping a prior atoms output into
                  the names that this atom expects (in a way this is like
                  remapping a namespace of another atom into the namespace
                  of this atom).
    :ivar inject: See parameter ``inject``.
    :ivar name: See parameter ``name``.
    :ivar requires: A :py:class:`~taskflow.types.sets.OrderedSet` of inputs
                    this atom requires to function.
    :ivar optional: A :py:class:`~taskflow.types.sets.OrderedSet` of inputs
                    that are optional for this atom to function.
    :ivar provides: A :py:class:`~taskflow.types.sets.OrderedSet` of outputs
                    this atom produces.
    """

    def __init__(self, name=None, provides=None, inject=None):
        self.name = name
        self.version = (1, 0)
        self.inject = inject
        self.save_as = _save_as_to_mapping(provides)
        self.requires = sets.OrderedSet()
        self.optional = sets.OrderedSet()
        self.provides = sets.OrderedSet(self.save_as)
        self.rebind = OrderedDict()

    def _build_arg_mapping(self, executor, requires=None, rebind=None,
                           auto_extract=True, ignore_list=None):
        required, optional = _build_arg_mapping(self.name, requires, rebind,
                                                executor, auto_extract,
                                                ignore_list=ignore_list)
        rebind = OrderedDict()
        for (arg_name, bound_name) in itertools.chain(six.iteritems(required),
                                                      six.iteritems(optional)):
            rebind.setdefault(arg_name, bound_name)
        self.rebind = rebind
        self.requires = sets.OrderedSet(six.itervalues(required))
        self.optional = sets.OrderedSet(six.itervalues(optional))
        if self.inject:
            inject_keys = frozenset(six.iterkeys(self.inject))
            self.requires -= inject_keys
            self.optional -= inject_keys

    @abc.abstractmethod
    def execute(self, *args, **kwargs):
        """Executes this atom."""

    @abc.abstractmethod
    def revert(self, *args, **kwargs):
        """Reverts this atom (undoing any :meth:`execute` side-effects)."""

    def __str__(self):
        return "%s==%s" % (self.name, misc.get_version_string(self))

    def __repr__(self):
        return '<%s %s>' % (reflection.get_class_name(self), self)
