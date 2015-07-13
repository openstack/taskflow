#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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

"""
Profile a simple engine build/load/compile/prepare/validate/run.
"""

import argparse
import cProfile as profiler
import pstats

from oslo_utils import timeutils
import six
from six.moves import range as compat_range

from taskflow import engines
from taskflow.patterns import linear_flow as lf
from taskflow import task


def print_header(name):
    if name:
        header_footer = "-" * len(name)
        print(header_footer)
        print(name)
        print(header_footer)


class ProfileIt(object):
    stats_ordering = ('cumulative', 'calls',)

    def __init__(self, name, args):
        self.name = name
        self.profile = profiler.Profile()
        self.args = args

    def __enter__(self):
        self.profile.enable()

    def __exit__(self, exc_tp, exc_v, exc_tb):
        self.profile.disable()
        buf = six.StringIO()
        ps = pstats.Stats(self.profile, stream=buf)
        ps = ps.sort_stats(*self.stats_ordering)
        percent_limit = max(0.0, max(1.0, self.args.limit / 100.0))
        ps.print_stats(percent_limit)
        print_header(self.name)
        needs_newline = False
        for line in buf.getvalue().splitlines():
            line = line.lstrip()
            if line:
                print(line)
                needs_newline = True
        if needs_newline:
            print("")


class TimeIt(object):
    def __init__(self, name, args):
        self.watch = timeutils.StopWatch()
        self.name = name
        self.args = args

    def __enter__(self):
        self.watch.restart()

    def __exit__(self, exc_tp, exc_v, exc_tb):
        self.watch.stop()
        duration = self.watch.elapsed()
        print_header(self.name)
        print("- Took %0.3f seconds to run" % (duration))


class DummyTask(task.Task):
    def execute(self):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', "-p",
                        dest='profile', action='store_true',
                        default=False,
                        help='profile instead of gather timing'
                             ' (default: False)')
    parser.add_argument('--dummies', "-d",
                        dest='dummies', action='store', type=int,
                        default=100, metavar="<number>",
                        help='how many dummy/no-op tasks to inject'
                             ' (default: 100)')
    parser.add_argument('--limit', '-l',
                        dest='limit', action='store', type=float,
                        default=100.0, metavar="<number>",
                        help='percentage of profiling output to show'
                             ' (default: 100%%)')
    args = parser.parse_args()
    if args.profile:
        ctx_manager = ProfileIt
    else:
        ctx_manager = TimeIt
    dummy_am = max(0, args.dummies)
    with ctx_manager("Building linear flow with %s tasks" % dummy_am, args):
        f = lf.Flow("root")
        for i in compat_range(0, dummy_am):
            f.add(DummyTask(name="dummy_%s" % i))
    with ctx_manager("Loading", args):
        e = engines.load(f)
    with ctx_manager("Compiling", args):
        e.compile()
    with ctx_manager("Preparing", args):
        e.prepare()
    with ctx_manager("Validating", args):
        e.validate()
    with ctx_manager("Running", args):
        e.run()


if __name__ == "__main__":
    main()
