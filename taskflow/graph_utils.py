# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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

from taskflow import exceptions as exc


LOG = logging.getLogger(__name__)


def connect(graph, infer_key='infer', auto_reason='auto', discard_func=None):
    """Connects a graphs runners to other runners in the graph which provide
    outputs for each runners requirements."""

    if len(graph) == 0:
        return
    if discard_func:
        for (u, v, e_data) in graph.edges(data=True):
            if discard_func(u, v, e_data):
                graph.remove_edge(u, v)
    for (r, r_data) in graph.nodes_iter(data=True):
        requires = set(r.requires)

        # Find the ones that have already been attached manually.
        manual_providers = {}
        if requires:
            incoming = [e[0] for e in graph.in_edges_iter([r])]
            for r2 in incoming:
                fulfills = requires & r2.provides
                if fulfills:
                    LOG.debug("%s is a manual provider of %s for %s",
                              r2, fulfills, r)
                    for k in fulfills:
                        manual_providers[k] = r2
                        requires.remove(k)

        # Anything leftover that we must find providers for??
        auto_providers = {}
        if requires and r_data.get(infer_key):
            for r2 in graph.nodes_iter():
                if r is r2:
                    continue
                fulfills = requires & r2.provides
                if fulfills:
                    graph.add_edge(r2, r, reason=auto_reason)
                    LOG.debug("Connecting %s as a automatic provider for"
                              " %s for %s", r2, fulfills, r)
                    for k in fulfills:
                        auto_providers[k] = r2
                        requires.remove(k)
                if not requires:
                    break

        # Anything still leftover??
        if requires:
            # Ensure its in string format, since join will puke on
            # things that are not strings.
            missing = ", ".join(sorted([str(s) for s in requires]))
            raise exc.MissingDependencies(r, missing)
        else:
            r.providers = {}
            r.providers.update(auto_providers)
            r.providers.update(manual_providers)
