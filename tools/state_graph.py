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

import mock

import optparse
import os
import sys

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir))
sys.path.insert(0, top_dir)

from automaton.converters import pydot
from automaton import machines

from taskflow.engines.action_engine import builder
from taskflow.engines.worker_based import protocol
from taskflow import states


# This is just needed to get at the machine object (we will not
# actually be running it...).
class DummyRuntime(object):
    def __init__(self):
        self.analyzer = mock.MagicMock()
        self.completer = mock.MagicMock()
        self.scheduler = mock.MagicMock()
        self.storage = mock.MagicMock()


def make_machine(start_state, transitions, event_name_cb):
    machine = machines.FiniteMachine()
    machine.add_state(start_state)
    machine.default_start_state = start_state
    for (start_state, end_state) in transitions:
        if start_state not in machine:
            machine.add_state(start_state)
        if end_state not in machine:
            machine.add_state(end_state)
        event = event_name_cb(start_state, end_state)
        machine.add_transition(start_state, end_state, event)
    return machine


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
    parser.add_option("-j", "--jobs", dest="jobs",
                      action='store_true',
                      help="use job transitions",
                      default=False)
    parser.add_option("--flow", dest="flow",
                      action='store_true',
                      help="use flow transitions",
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
        options.jobs,
        options.flow,
    ]
    provided = sum([int(i) for i in types])
    if provided > 1:
        parser.error("Only one of task/retry/engines/wbe requests/jobs/flow"
                     " may be specified.")
    if provided == 0:
        parser.error("One of task/retry/engines/wbe requests/jobs/flow"
                     " must be specified.")

    event_name_cb = lambda start_state, end_state: "on_%s" % end_state.lower()
    internal_states = list()
    ordering = 'in'
    if options.tasks:
        source_type = "Tasks"
        source = make_machine(states.PENDING,
                              list(states._ALLOWED_TASK_TRANSITIONS),
                              event_name_cb)
    elif options.retries:
        source_type = "Retries"
        source = make_machine(states.PENDING,
                              list(states._ALLOWED_RETRY_TRANSITIONS),
                              event_name_cb)
    elif options.flow:
        source_type = "Flow"
        source = make_machine(states.PENDING,
                              list(states._ALLOWED_FLOW_TRANSITIONS),
                              event_name_cb)
    elif options.engines:
        source_type = "Engines"
        b = builder.MachineBuilder(DummyRuntime(), mock.MagicMock())
        source, memory = b.build()
        internal_states.extend(builder.META_STATES)
        ordering = 'out'
    elif options.wbe_requests:
        source_type = "WBE requests"
        source = make_machine(protocol.WAITING,
                              list(protocol._ALLOWED_TRANSITIONS),
                              event_name_cb)
    elif options.jobs:
        source_type = "Jobs"
        source = make_machine(states.UNCLAIMED,
                              list(states._ALLOWED_JOB_TRANSITIONS),
                              event_name_cb)

    graph_attrs = {
        'ordering': ordering,
    }
    graph_name = "%s states" % source_type

    def node_attrs_cb(state):
        node_color = None
        if state in internal_states:
            node_color = 'blue'
        if state in (states.FAILURE, states.REVERT_FAILURE):
            node_color = 'red'
        if state == states.REVERTED:
            node_color = 'darkorange'
        if state in (states.SUCCESS, states.COMPLETE):
            node_color = 'green'
        node_attrs = {}
        if node_color:
            node_attrs['fontcolor'] = node_color
        return node_attrs

    def edge_attrs_cb(start_state, on_event, end_state):
        edge_attrs = {}
        if options.engines:
            edge_attrs['label'] = on_event.replace("_", " ").strip()
            if 'reverted' in on_event:
                edge_attrs['fontcolor'] = 'darkorange'
            if 'fail' in on_event:
                edge_attrs['fontcolor'] = 'red'
            if 'success' in on_event:
                edge_attrs['fontcolor'] = 'green'
        return edge_attrs

    g = pydot.convert(source, graph_name, graph_attrs=graph_attrs,
                      node_attrs_cb=node_attrs_cb, edge_attrs_cb=edge_attrs_cb)
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
