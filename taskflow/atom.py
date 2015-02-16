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

from oslo_utils import reflection
import six

from taskflow import exceptions
from taskflow.utils import misc


def _save_as_to_mapping(save_as):
    """Convert save_as to mapping name => index.

    Result should follow storage convention for mappings.
    """
    # TODO(harlowja): we should probably document this behavior & convention
    # outside of code so that it's more easily understandable, since what an
    # atom returns is pretty crucial for other later operations.
    if save_as is None:
        return {}
    if isinstance(save_as, six.string_types):
        # NOTE(harlowja): this means that your atom will only return one item
        # instead of a dictionary-like object or a indexable object (like a
        # list or tuple).
        return {save_as: None}
    elif isinstance(save_as, (tuple, list)):
        # NOTE(harlowja): this means that your atom will return a indexable
        # object, like a list or tuple and the results can be mapped by index
        # to that tuple/list that is returned for others to use.
        return dict((key, num) for num, key in enumerate(save_as))
    elif isinstance(save_as, set):
        # NOTE(harlowja): in the case where a set is given we will not be
        # able to determine the numeric ordering in a reliable way (since it is
        # a unordered set) so the only way for us to easily map the result of
        # the atom will be via the key itself.
        return dict((key, key) for key in save_as)
    raise TypeError('Atom provides parameter '
                    'should be str, set or tuple/list, not %r' % save_as)


def _build_rebind_dict(args, rebind_args):
    """Build a argument remapping/rebinding dictionary.

    This dictionary allows an atom to declare that it will take a needed
    requirement bound to a given name with another name instead (mapping the
    new name onto the required name).
    """
    if rebind_args is None:
        return {}
    elif isinstance(rebind_args, (list, tuple)):
        rebind = dict(zip(args, rebind_args))
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

    # build a list of required arguments based on function signature
    req_args = reflection.get_callable_args(function, required_only=True)
    all_args = reflection.get_callable_args(function, required_only=False)

    # remove arguments that are part of ignore_list
    if ignore_list:
        for arg in ignore_list:
            if arg in req_args:
                req_args.remove(arg)
    else:
        ignore_list = []

    required = {}
    # add reqs to required mappings
    if reqs:
        if isinstance(reqs, six.string_types):
            required.update({reqs: reqs})
        else:
            required.update((a, a) for a in reqs)

    # add req_args to required mappings if do_infer is set
    if do_infer:
        required.update((a, a) for a in req_args)

    # update required mappings based on rebind_args
    required.update(_build_rebind_dict(req_args, rebind_args))

    if do_infer:
        opt_args = set(all_args) - set(required) - set(ignore_list)
        optional = dict((a, a) for a in opt_args)
    else:
        optional = {}

    if not reflection.accepts_kwargs(function):
        extra_args = set(required) - set(all_args)
        if extra_args:
            extra_args_str = ', '.join(sorted(extra_args))
            raise ValueError('Extra arguments given to atom %s: %s'
                             % (atom_name, extra_args_str))

    # NOTE(imelnikov): don't use set to preserve order in error message
    missing_args = [arg for arg in req_args if arg not in required]
    if missing_args:
        raise ValueError('Missing arguments for atom %s: %s'
                         % (atom_name, ' ,'.join(missing_args)))
    return required, optional


@six.add_metaclass(abc.ABCMeta)
class Atom(object):
    """An abstract flow atom that causes a flow to progress (in some manner).

    An atom is a named object that operates with input flow data to perform
    some action that furthers the overall flows progress. It usually also
    produces some of its own named output as a result of this process.

    :ivar version: An *immutable* version that associates version information
                   with this atom. It can be useful in resuming older versions
                   of atoms. Standard major, minor versioning concepts
                   should apply.
    :ivar save_as: An *immutable* output ``resource`` name dictionary this atom
                   produces that other atoms may depend on this atom providing.
                   The format is output index (or key when a dictionary
                   is returned from the execute method) to stored argument
                   name.
    :ivar rebind: An *immutable* input ``resource`` mapping dictionary that
                  can be used to alter the inputs given to this atom. It is
                  typically used for mapping a prior atoms output into
                  the names that this atom expects (in a way this is like
                  remapping a namespace of another atom into the namespace
                  of this atom).
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
                  commences (this allows for providing atom *local* values that
                  do not need to be provided by other atoms/dependents).
    :ivar inject: See parameter ``inject``.
    :ivar requires: Any inputs this atom requires to function (if applicable).
                    NOTE(harlowja): there can be no intersection between what
                    this atom requires and what it produces (since this would
                    be an impossible dependency to satisfy).
    :ivar optional: Any inputs that are optional for this atom's execute
                    method.

    """

    def __init__(self, name=None, provides=None, inject=None):
        self._name = name
        self.save_as = _save_as_to_mapping(provides)
        self.version = (1, 0)
        self.inject = inject
        self.requires = frozenset()
        self.optional = frozenset()

    def _build_arg_mapping(self, executor, requires=None, rebind=None,
                           auto_extract=True, ignore_list=None):
        req_arg, opt_arg = _build_arg_mapping(self.name, requires, rebind,
                                              executor, auto_extract,
                                              ignore_list)

        self.rebind = {}
        if opt_arg:
            self.rebind.update(opt_arg)
        if req_arg:
            self.rebind.update(req_arg)
        self.requires = frozenset(req_arg.values())
        self.optional = frozenset(opt_arg.values())
        if self.inject:
            inject_set = set(six.iterkeys(self.inject))
            self.requires -= inject_set
            self.optional -= inject_set

        out_of_order = self.provides.intersection(self.requires)
        if out_of_order:
            raise exceptions.DependencyFailure(
                "Atom %(item)s provides %(oo)s that are required "
                "by this atom"
                % dict(item=self.name, oo=sorted(out_of_order)))

    @abc.abstractmethod
    def execute(self, *args, **kwargs):
        """Executes this atom."""

    @abc.abstractmethod
    def revert(self, *args, **kwargs):
        """Reverts this atom (undoing any :meth:`execute` side-effects)."""

    @property
    def name(self):
        """A non-unique name for this atom (human readable)."""
        return self._name

    def __str__(self):
        return "%s==%s" % (self.name, misc.get_version_string(self))

    def __repr__(self):
        return '<%s %s>' % (reflection.get_class_name(self), self)

    @property
    def provides(self):
        """Any outputs this atom produces.

        NOTE(harlowja): there can be no intersection between what this atom
        requires and what it produces (since this would be an impossible
        dependency to satisfy).
        """
        return set(self.save_as)
