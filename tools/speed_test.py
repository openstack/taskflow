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

from six.moves import range as compat_range

from taskflow import engines
from taskflow.patterns import linear_flow as lf
from taskflow import task
from taskflow.types import timing


class TimeIt(object):
    def __init__(self, name):
        self.watch = timing.StopWatch()
        self.name = name

    def __enter__(self):
        self.watch.restart()

    def __exit__(self, exc_tp, exc_v, exc_tb):
        self.watch.stop()
        duration = self.watch.elapsed()
        print("Took %0.3f seconds to run '%s'" % (duration, self.name))


class DummyTask(task.Task):
    def execute(self):
        pass


def main():
    dummy_am = 100
    with TimeIt("building"):
        f = lf.Flow("root")
        for i in compat_range(0, dummy_am):
            f.add(DummyTask(name="dummy_%s" % i))
    with TimeIt("loading"):
        e = engines.load(f)
    with TimeIt("compiling"):
        e.compile()
    with TimeIt("preparing"):
        e.prepare()
    with TimeIt("validating"):
        e.validate()
    with TimeIt("running"):
        e.run()


if __name__ == "__main__":
    main()
