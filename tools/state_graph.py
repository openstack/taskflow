#!/usr/bin/env python

import os
import sys

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir))
sys.path.insert(0, top_dir)

import optparse
import subprocess
import tempfile

import networkx as nx

from taskflow import states
from taskflow.utils import graph_utils as gu


def mini_exec(cmd, ok_codes=(0,)):
    stdout = subprocess.PIPE
    stderr = subprocess.PIPE
    proc = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, stdin=None)
    (stdout, stderr) = proc.communicate()
    rc = proc.returncode
    if rc not in ok_codes:
        raise RuntimeError("Could not run %s [%s]\nStderr: %s"
                           % (cmd, rc, stderr))
    return (stdout, stderr)


def make_svg(graph, output_filename, output_format):
    # NOTE(harlowja): requires pydot!
    gdot = gu.export_graph_to_dot(graph)
    if output_format == 'dot':
        output = gdot
    elif output_format in ('svg', 'svgz', 'png'):
        with tempfile.NamedTemporaryFile(suffix=".dot") as fh:
            fh.write(gdot)
            fh.flush()
            cmd = ['dot', '-T%s' % output_format, fh.name]
            output, _stderr = mini_exec(cmd)
    else:
        raise ValueError('Unknown format: %s' % output_filename)
    with open(output_filename, "wb") as fh:
        fh.write(output)


def main():
    parser = optparse.OptionParser()
    parser.add_option("-f", "--file", dest="filename",
                      help="write svg to FILE", metavar="FILE")
    parser.add_option("-t", "--tasks", dest="tasks",
                      action='store_true',
                      help="use task state transitions",
                      default=False)
    parser.add_option("-T", "--format", dest="format",
                      help="output in given format",
                      default='svg')

    (options, args) = parser.parse_args()
    if options.filename is None:
        options.filename = 'states.%s' % options.format

    g = nx.DiGraph(name="State transitions")
    if not options.tasks:
        source = states._ALLOWED_FLOW_TRANSITIONS
    else:
        source = states._ALLOWED_TASK_TRANSITIONS
    for (u, v) in source:
        if not g.has_node(u):
            g.add_node(u)
        if not g.has_node(v):
            g.add_node(v)
        g.add_edge(u, v)
    make_svg(g, options.filename, options.format)
    print("Created %s at '%s'" % (options.format, options.filename))


if __name__ == '__main__':
    main()
