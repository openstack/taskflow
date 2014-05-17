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

import collections

from taskflow import exceptions as exc
from taskflow.utils import flow_utils

# The result of a compilers compile() is this tuple (for now it is just a
# execution graph but in the future it may grow to include more attributes
# that help the runtime units execute in a more optimal/featureful manner).
Compilation = collections.namedtuple("Compilation", ["execution_graph"])


class PatternCompiler(object):
    """Compiles patterns & atoms (potentially nested) into an compilation
    unit with a *logically* equivalent directed acyclic graph representation.

    NOTE(harlowja): during this pattern translation process any nested flows
    will be converted into there equivalent subgraphs. This currently implies
    that contained atoms in those nested flows, post-translation will no longer
    be associated with there previously containing flow but instead will lose
    this identity and what will remain is the logical constraints that there
    contained flow mandated. In the future this may be changed so that this
    association is not lost via the compilation process (since it is sometime
    useful to retain part of this relationship).
    """
    def compile(self, root):
        graph = flow_utils.flatten(root)
        if graph.number_of_nodes() == 0:
            # Try to get a name attribute, otherwise just use the object
            # string representation directly if that attribute does not exist.
            name = getattr(root, 'name', root)
            raise exc.Empty("Root container '%s' (%s) is empty."
                            % (name, type(root)))
        return Compilation(graph)
