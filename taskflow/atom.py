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

import logging

import six

from taskflow.utils import misc
from taskflow.utils import reflection

LOG = logging.getLogger(__name__)


def _save_as_to_mapping(save_as):
    """Convert save_as to mapping name => index.

    Result should follow storage convention for mappings.
    """
    # TODO(harlowja): we should probably document this behavior & convention
    # outside of code so that its more easily understandable, since what an
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
    raise TypeError('Task provides parameter '
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
        raise TypeError('Invalid rebind value: %s' % rebind_args)


def _build_arg_mapping(task_name, reqs, rebind_args, function, do_infer):
    """Given a function, its requirements and a rebind mapping this helper
    function will build the correct argument mapping for the given function as
    well as verify that the final argument mapping does not have missing or
    extra arguments (where applicable).
    """
    task_args = reflection.get_callable_args(function, required_only=True)
    result = {}
    if reqs:
        result.update((a, a) for a in reqs)
    if do_infer:
        result.update((a, a) for a in task_args)
    result.update(_build_rebind_dict(task_args, rebind_args))

    if not reflection.accepts_kwargs(function):
        all_args = reflection.get_callable_args(function, required_only=False)
        extra_args = set(result) - set(all_args)
        if extra_args:
            extra_args_str = ', '.join(sorted(extra_args))
            raise ValueError('Extra arguments given to task %s: %s'
                             % (task_name, extra_args_str))

    # NOTE(imelnikov): don't use set to preserve order in error message
    missing_args = [arg for arg in task_args if arg not in result]
    if missing_args:
        raise ValueError('Missing arguments for task %s: %s'
                         % (task_name, ' ,'.join(missing_args)))
    return result


class Atom(object):
    """An abstract flow atom that causes a flow to progress (in some manner).

    An atom is a named object that operates with input flow data to perform
    some action that furthers the overall flows progress. It usually also
    produces some of its own named output as a result of this process.
    """

    def __init__(self, name=None, provides=None):
        self._name = name
        # An *immutable* output 'resource' name dict this atom
        # produces that other atoms may depend on this atom providing.
        #
        # Format is output index:arg_name
        self.save_as = _save_as_to_mapping(provides)
        # This identifies the version of the atom to be ran which
        # can be useful in resuming older versions of atoms. Standard
        # major, minor version semantics apply.
        self.version = (1, 0)

    def _build_arg_mapping(self, executor, requires=None, rebind=None,
                           auto_extract=True):
        self.rebind = _build_arg_mapping(self.name, requires, rebind,
                                         executor, auto_extract)

    @property
    def name(self):
        return self._name

    def __str__(self):
        return "%s==%s" % (self.name, misc.get_version_string(self))

    @property
    def provides(self):
        """Any outputs this atom produces."""
        return set(self.save_as)

    @property
    def requires(self):
        """Any inputs this atom requires to execute."""
        return set(self.rebind.values())
