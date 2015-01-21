#!/usr/bin/env python

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

import optparse
import os
import sys

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir))
sys.path.insert(0, top_dir)

# To get this installed you may have to follow:
# https://code.google.com/p/pydot/issues/detail?id=93 (until fixed).
import pydot

from taskflow.engines.action_engine import runner
from taskflow.engines.worker_based import protocol
from taskflow import states
from taskflow.types import fsm


# This is just needed to get at the runner builder object (we will not
# actually be running it...).
class DummyRuntime(object):
    def __init__(self):
        self.analyzer = None
        self.completer = None
        self.scheduler = None
        self.storage = None


def clean_event(name):
    name = name.replace("_", " ")
    name = name.strip()
    return name


def make_machine(start_state, transitions, disallowed):
    machine = fsm.FSM(start_state)
    machine.add_state(start_state)
    for (start_state, end_state) in transitions:
        if start_state in disallowed or end_state in disallowed:
            continue
        if start_state not in machine:
            machine.add_state(start_state)
        if end_state not in machine:
            machine.add_state(end_state)
        # Make a fake event (not used anyway)...
        event = "on_%s" % (end_state)
        machine.add_transition(start_state, end_state, event.lower())
    return machine


def map_color(internal_states, state):
    if state in internal_states:
        return 'blue'
    if state == states.FAILURE:
        return 'red'
    if state == states.REVERTED:
        return 'darkorange'
    if state == states.SUCCESS:
        return 'green'
    return None


def main():
    parser = optparse.OptionParser()
    parser.add_option("-f", "--file", dest="filename",
                      help="write svg to FILE", metavar="FILE")
    parser.add_option("-t", "--tasks", dest="tasks",
                      action='store_true',
                      help="use task state transitions",
                      default=False)
    parser.add_option("-r", "--retries", dest="retries",
                      action='store_true',
                      help="use retry state transitions",
                      default=False)
    parser.add_option("-e", "--engines", dest="engines",
                      action='store_true',
                      help="use engine state transitions",
                      default=False)
    parser.add_option("-w", "--wbe-requests", dest="wbe_requests",
                      action='store_true',
                      help="use wbe request transitions",
                      default=False)
    parser.add_option("-T", "--format", dest="format",
                      help="output in given format",
                      default='svg')

    (options, args) = parser.parse_args()
    if options.filename is None:
        options.filename = 'states.%s' % options.format

    types = [
        options.engines,
        options.retries,
        options.tasks,
        options.wbe_requests,
    ]
    if sum([int(i) for i in types]) > 1:
        parser.error("Only one of task/retry/engines/wbe requests"
                     " may be specified.")

    internal_states = list()
    ordering = 'in'
    if options.tasks:
        source_type = "Tasks"
        source = make_machine(states.PENDING,
                              list(states._ALLOWED_TASK_TRANSITIONS),
                              [states.RETRYING])
    elif options.retries:
        source_type = "Retries"
        source = make_machine(states.PENDING,
                              list(states._ALLOWED_TASK_TRANSITIONS), [])
    elif options.engines:
        source_type = "Engines"
        r = runner.Runner(DummyRuntime(), None)
        source, memory = r.builder.build()
        internal_states.extend(runner._META_STATES)
        ordering = 'out'
    elif options.wbe_requests:
        source_type = "WBE requests"
        source = make_machine(protocol.WAITING,
                              list(protocol._ALLOWED_TRANSITIONS), [])
    else:
        source_type = "Flow"
        source = make_machine(states.PENDING,
                              list(states._ALLOWED_FLOW_TRANSITIONS), [])

    graph_name = "%s states" % source_type
    g = pydot.Dot(graph_name=graph_name, rankdir='LR',
                  nodesep='0.25', overlap='false',
                  ranksep="0.5", size="11x8.5",
                  splines='true', ordering=ordering)
    node_attrs = {
        'fontsize': '11',
    }
    nodes = {}
    for (start_state, on_event, end_state) in source:
        on_event = clean_event(on_event)
        if start_state not in nodes:
            start_node_attrs = node_attrs.copy()
            text_color = map_color(internal_states, start_state)
            if text_color:
                start_node_attrs['fontcolor'] = text_color
            nodes[start_state] = pydot.Node(start_state, **start_node_attrs)
            g.add_node(nodes[start_state])
        if end_state not in nodes:
            end_node_attrs = node_attrs.copy()
            text_color = map_color(internal_states, end_state)
            if text_color:
                end_node_attrs['fontcolor'] = text_color
            nodes[end_state] = pydot.Node(end_state, **end_node_attrs)
            g.add_node(nodes[end_state])
        if options.engines:
            edge_attrs = {
                'label': on_event,
            }
            if 'reverted' in on_event:
                edge_attrs['fontcolor'] = 'darkorange'
            if 'fail' in on_event:
                edge_attrs['fontcolor'] = 'red'
            if 'success' in on_event:
                edge_attrs['fontcolor'] = 'green'
        else:
            edge_attrs = {}
        g.add_edge(pydot.Edge(nodes[start_state], nodes[end_state],
                              **edge_attrs))

    start = pydot.Node("__start__", shape="point", width="0.1",
                       xlabel='start', fontcolor='green', **node_attrs)
    g.add_node(start)
    g.add_edge(pydot.Edge(start, nodes[source.start_state], style='dotted'))

    print("*" * len(graph_name))
    print(graph_name)
    print("*" * len(graph_name))
    print(source.pformat())
    print(g.to_string().strip())

    g.write(options.filename, format=options.format)
    print("Created %s at '%s'" % (options.format, options.filename))

    # To make the svg more pretty use the following:
    # $ xsltproc ../diagram-tools/notugly.xsl ./states.svg > pretty-states.svg
    # Get diagram-tools from https://github.com/vidarh/diagram-tools.git


if __name__ == '__main__':
    main()
