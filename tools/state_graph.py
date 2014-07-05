#!/usr/bin/env python

import optparse
import os
import sys

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir))
sys.path.insert(0, top_dir)

import networkx as nx

# To get this installed you may have to follow:
# https://code.google.com/p/pydot/issues/detail?id=93 (until fixed).
import pydot

from taskflow import states


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
    parser.add_option("-T", "--format", dest="format",
                      help="output in given format",
                      default='svg')

    (options, args) = parser.parse_args()
    if options.filename is None:
        options.filename = 'states.%s' % options.format

    types = [options.engines, options.retries, options.tasks]
    if sum([int(i) for i in types]) > 1:
        parser.error("Only one of task/retry/engines may be specified.")

    disallowed = set()
    start_node = states.PENDING
    if options.tasks:
        source = list(states._ALLOWED_TASK_TRANSITIONS)
        source_type = "Tasks"
        disallowed.add(states.RETRYING)
    elif options.retries:
        source = list(states._ALLOWED_TASK_TRANSITIONS)
        source_type = "Retries"
    elif options.engines:
        # TODO(harlowja): place this in states.py
        source = [
            (states.RESUMING, states.SCHEDULING),
            (states.SCHEDULING, states.WAITING),
            (states.WAITING, states.ANALYZING),
            (states.ANALYZING, states.SCHEDULING),
            (states.ANALYZING, states.WAITING),
        ]
        for u in (states.SCHEDULING, states.ANALYZING):
            for v in (states.SUSPENDED, states.SUCCESS, states.REVERTED):
                source.append((u, v))
        source_type = "Engines"
        start_node = states.RESUMING
    else:
        source = list(states._ALLOWED_FLOW_TRANSITIONS)
        source_type = "Flow"

    transitions = nx.DiGraph()
    for (u, v) in source:
        if u not in disallowed:
            transitions.add_node(u)
        if v not in disallowed:
            transitions.add_node(v)
    for (u, v) in source:
        if not transitions.has_node(u) or not transitions.has_node(v):
            continue
        transitions.add_edge(u, v)

    graph_name = "%s states" % source_type
    g = pydot.Dot(graph_name=graph_name, rankdir='LR',
                  nodesep='0.25', overlap='false',
                  ranksep="0.5", size="11x8.5",
                  splines='true', ordering='in')
    node_attrs = {
        'fontsize': '11',
    }
    nodes = {}
    nodes_order = []
    edges_added = []
    for (u, v) in nx.bfs_edges(transitions, source=start_node):
        if u not in nodes:
            nodes[u] = pydot.Node(u, **node_attrs)
            g.add_node(nodes[u])
            nodes_order.append(u)
        if v not in nodes:
            nodes[v] = pydot.Node(v, **node_attrs)
            g.add_node(nodes[v])
            nodes_order.append(v)
    for u in nodes_order:
        for v in transitions.successors_iter(u):
            if (u, v) not in edges_added:
                g.add_edge(pydot.Edge(nodes[u], nodes[v]))
                edges_added.append((u, v))
    start = pydot.Node("__start__", shape="point", width="0.1",
                       xlabel='start', fontcolor='green', **node_attrs)
    g.add_node(start)
    g.add_edge(pydot.Edge(start, nodes[start_node], style='dotted'))

    print("*" * len(graph_name))
    print(graph_name)
    print("*" * len(graph_name))
    print(g.to_string().strip())

    g.write(options.filename, format=options.format)
    print("Created %s at '%s'" % (options.format, options.filename))

    # To make the svg more pretty use the following:
    # $ xsltproc ../diagram-tools/notugly.xsl ./states.svg > pretty-states.svg
    # Get diagram-tools from https://github.com/vidarh/diagram-tools.git


if __name__ == '__main__':
    main()
