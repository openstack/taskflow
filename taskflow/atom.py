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

from oslo_utils import reflection
import six
from six.moves import zip as compat_zip

from taskflow.types import sets
from taskflow.utils import misc


# Helper types tuples...
_sequence_types = (list, tuple, collections.Sequence)
_set_types = (set, collections.Set)

# the default list of revert arguments to ignore when deriving
# revert argument mapping from the revert method signature
_default_revert_args = ('result', 'flow_failures')


def _save_as_to_mapping(save_as):
    """Convert save_as to mapping name => index.

    Result should follow storage convention for mappings.
    """
    # TODO(harlowja): we should probably document this behavior & convention
    # outside of code so that it's more easily understandable, since what an
    # atom returns is pretty crucial for other later operations.
    if save_as is None:
        return collections.OrderedDict()
    if isinstance(save_as, six.string_types):
        # NOTE(harlowja): this means that your atom will only return one item
        # instead of a dictionary-like object or a indexable object (like a
        # list or tuple).
        return collections.OrderedDict([(save_as, None)])
    elif isinstance(save_as, _sequence_types):
        # NOTE(harlowja): this means that your atom will return a indexable
        # object, like a list or tuple and the results can be mapped by index
        # to that tuple/list that is returned for others to use.
        return collections.OrderedDict((key, num)
                                       for num, key in enumerate(save_as))
    elif isinstance(save_as, _set_types):
        # NOTE(harlowja): in the case where a set is given we will not be
        # able to determine the numeric ordering in a reliable way (since it
        # may be an unordered set) so the only way for us to easily map the
        # result of the atom will be via the key itself.
        return collections.OrderedDict((key, key) for key in save_as)
    else:
        raise TypeError('Atom provides parameter '
                        'should be str, set or tuple/list, not %r' % save_as)


def _build_rebind_dict(req_args, rebind_args):
    """Build a argument remapping/rebinding dictionary.

    This dictionary allows an atom to declare that it will take a needed
    requirement bound to a given name with another name instead (mapping the
    new name onto the required name).
    """
    if rebind_args is None:
        return collections.OrderedDict()
    elif isinstance(rebind_args, (list, tuple)):
        # Attempt to map the rebound argument names position by position to
        # the required argument names (if they are the same length then
        # this determines how to remap the required argument names to the
        # rebound ones).
        rebind = collections.OrderedDict(compat_zip(req_args, rebind_args))
        if len(req_args) < len(rebind_args):
            # Extra things were rebound, that may be because of *args
            # or **kwargs (or some other reason); so just keep all of them
            # using 1:1 rebinding...
            rebind.update((a, a) for a in rebind_args[len(req_args):])
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
    required = collections.OrderedDict()

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
        optional = collections.OrderedDict((a, a) for a in opt_args)
    else:
        optional = collections.OrderedDict()

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
    :param rebind: A dict of key/value pairs used to define argument
                   name conversions for inputs to this atom's ``execute``
                   method.
    :param revert_rebind: The same as ``rebind`` but for the ``revert``
                          method. If unpassed, ``rebind`` will be used
                          instead.
    :param requires: A set or list of required inputs for this atom's
                     ``execute`` method.
    :param revert_requires: A set or list of required inputs for this atom's
                            ``revert`` method. If unpassed, ``requires`` will
                            be used.
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
    :ivar revert_rebind: The same as ``rebind`` but for the revert method. This
                         should only differ from ``rebind`` if the ``revert``
                         method has a different signature from ``execute`` or
                         a different ``revert_rebind`` value was received.
    :ivar inject: See parameter ``inject``.
    :ivar Atom.name: See parameter ``name``.
    :ivar optional: A :py:class:`~taskflow.types.sets.OrderedSet` of inputs
                    that are optional for this atom to ``execute``.
    :ivar revert_optional: The ``revert`` version of ``optional``.
    :ivar provides: A :py:class:`~taskflow.types.sets.OrderedSet` of outputs
                    this atom produces.
    """

    priority = 0
    """A numeric priority that instances of this class will have when running,
    used when there are multiple *parallel* candidates to execute and/or
    revert. During this situation the candidate list will be stably sorted
    based on this priority attribute which will result in atoms with higher
    priorities executing (or reverting) before atoms with lower
    priorities (higher being defined as a number bigger, or greater tha
    an atom with a lower priority number). By default all atoms have the same
    priority (zero).

    For example when the following is combined into a
    graph (where each node in the denoted graph is some task)::

        a -> b
        b -> c
        b -> e
        b -> f

    When ``b`` finishes there will then be three candidates that can run
    ``(c, e, f)`` and they may run in any order. What this priority does is
    sort those three by their priority before submitting them to be
    worked on (so that instead of say a random run order they will now be
    ran by there sorted order). This is also true when reverting (in that the
    sort order of the potential nodes will be used to determine the
    submission order).
    """

    default_provides = None

    def __init__(self, name=None, provides=None, requires=None,
                 auto_extract=True, rebind=None, inject=None,
                 ignore_list=None, revert_rebind=None, revert_requires=None):

        if provides is None:
            provides = self.default_provides

        self.name = name
        self.version = (1, 0)
        self.inject = inject
        self.save_as = _save_as_to_mapping(provides)
        self.provides = sets.OrderedSet(self.save_as)

        if ignore_list is None:
            ignore_list = []

        self.rebind, exec_requires, self.optional = self._build_arg_mapping(
            self.execute,
            requires=requires,
            rebind=rebind, auto_extract=auto_extract,
            ignore_list=ignore_list
        )

        revert_ignore = ignore_list + list(_default_revert_args)
        revert_mapping = self._build_arg_mapping(
            self.revert,
            requires=revert_requires or requires,
            rebind=revert_rebind or rebind,
            auto_extract=auto_extract,
            ignore_list=revert_ignore
        )
        (self.revert_rebind, addl_requires,
         self.revert_optional) = revert_mapping

        # TODO(bnemec): This should be documented as an ivar, but can't be due
        # to https://github.com/sphinx-doc/sphinx/issues/2549
        #: A :py:class:`~taskflow.types.sets.OrderedSet` of inputs this atom
        #: requires to function.
        self.requires = exec_requires.union(addl_requires)

    def _build_arg_mapping(self, executor, requires=None, rebind=None,
                           auto_extract=True, ignore_list=None):

        required, optional = _build_arg_mapping(self.name, requires, rebind,
                                                executor, auto_extract,
                                                ignore_list=ignore_list)
        # Form the real rebind mapping, if a key name is the same as the
        # key value, then well there is no rebinding happening, otherwise
        # there will be.
        rebind = collections.OrderedDict()
        for (arg_name, bound_name) in itertools.chain(six.iteritems(required),
                                                      six.iteritems(optional)):
            rebind.setdefault(arg_name, bound_name)
        requires = sets.OrderedSet(six.itervalues(required))
        optional = sets.OrderedSet(six.itervalues(optional))
        if self.inject:
            inject_keys = frozenset(six.iterkeys(self.inject))
            requires -= inject_keys
            optional -= inject_keys
        return rebind, requires, optional

    def pre_execute(self):
        """Code to be run prior to executing the atom.

        A common pattern for initializing the state of the system prior to
        running atoms is to define some code in a base class that all your
        atoms inherit from.  In that class, you can define a ``pre_execute``
        method and it will always be invoked just prior to your atoms running.
        """

    @abc.abstractmethod
    def execute(self, *args, **kwargs):
        """Activate a given atom which will perform some operation and return.

        This method can be used to perform an action on a given set of input
        requirements (passed in via ``*args`` and ``**kwargs``) to accomplish
        some type of operation. This operation may provide some named
        outputs/results as a result of it executing for later reverting (or for
        other atoms to depend on).

        NOTE(harlowja): the result (if any) that is returned should be
        persistable so that it can be passed back into this atom if
        reverting is triggered (especially in the case where reverting
        happens in a different python process or on a remote machine) and so
        that the result can be transmitted to other atoms (which may be local
        or remote).

        :param args: positional arguments that atom requires to execute.
        :param kwargs: any keyword arguments that atom requires to execute.
        """

    def post_execute(self):
        """Code to be run after executing the atom.

        A common pattern for cleaning up global state of the system after the
        execution of atoms is to define some code in a base class that all your
        atoms inherit from.  In that class, you can define a ``post_execute``
        method and it will always be invoked just after your atoms execute,
        regardless of whether they succeeded or not.

        This pattern is useful if you have global shared database sessions
        that need to be cleaned up, for example.
        """

    def pre_revert(self):
        """Code to be run prior to reverting the atom.

        This works the same as :meth:`.pre_execute`, but for the revert phase.
        """

    def revert(self, *args, **kwargs):
        """Revert this atom.

        This method should undo any side-effects caused by previous execution
        of the atom using the result of the :py:meth:`execute` method and
        information on the failure which triggered reversion of the flow the
        atom is contained in (if applicable).

        :param args: positional arguments that the atom required to execute.
        :param kwargs: any keyword arguments that the atom required to
                       execute; the special key ``'result'`` will contain
                       the :py:meth:`execute` result (if any) and
                       the ``**kwargs`` key ``'flow_failures'`` will contain
                       any failure information.
        """

    def post_revert(self):
        """Code to be run after reverting the atom.

        This works the same as :meth:`.post_execute`, but for the revert phase.
        """

    def __str__(self):
        return "%s==%s" % (self.name, misc.get_version_string(self))

    def __repr__(self):
        return '<%s %s>' % (reflection.get_class_name(self), self)
