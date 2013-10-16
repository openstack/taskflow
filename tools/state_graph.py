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
        raise RuntimeError("Could not run %s [%s]", cmd, rc)
    return (stdout, stderr)


def make_svg(graph, output_filename):
    # NOTE(harlowja): requires pydot!
    gdot = gu.export_graph_to_dot(graph)
    with tempfile.NamedTemporaryFile(suffix=".dot") as fh:
        fh.write(gdot)
        fh.flush()
        cmd = ['dot', '-Tsvg', fh.name]
        stdout, _stderr = mini_exec(cmd)
        with open(output_filename, "wb") as fh:
            fh.write(stdout)
    # NOTE(harlowja): if u want a png instead u can run the following which
    # requires ImageMagick and its little helper `convert` program.
    #
    # $ convert input.svg output.png


def main():
    parser = optparse.OptionParser()
    parser.add_option("-f", "--file", dest="filename",
                      help="write svg to FILE", metavar="FILE",
                      default="states.svg")
    parser.add_option("-t", "--tasks", dest="tasks",
                      action='store_true',
                      help="use task state transitions",
                      default=False)
    (options, args) = parser.parse_args()
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
    make_svg(g, options.filename)
    print("Created svg at '%s'" % (options.filename))


if __name__ == '__main__':
    main()
