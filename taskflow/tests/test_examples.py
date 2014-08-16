# -*- coding: utf-8 -*-

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


"""Run examples as unit tests.

This module executes examples as unit tests, thus ensuring they at least
can be executed with current taskflow. For examples with deterministic
output, the output can be put to file with same name and '.out.txt'
extension; then it will be checked that output did not change.

When this module is used as main module, output for all examples are
generated. Please note that this will break tests as output for most
examples is indeterministic (due to hash randomization for example).
"""


import os
import re
import subprocess
import sys

import taskflow.test

ROOT_DIR = os.path.abspath(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(__file__))))

UUID_RE = re.compile('XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX'
                     .replace('X', '[0-9a-f]'))


def root_path(*args):
    return os.path.join(ROOT_DIR, *args)


def run_example(name):
    path = root_path('taskflow', 'examples', '%s.py' % name)
    obj = subprocess.Popen([sys.executable, path],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = obj.communicate()
    stdout = output[0].decode()
    stderr = output[1].decode()

    rc = obj.wait()
    if rc != 0:
        raise RuntimeError('Example %s failed, return code=%s\n'
                           '<<<Begin captured STDOUT>>>\n%s'
                           '<<<End captured STDOUT>>>\n'
                           '<<<Begin captured STDERR>>>\n%s'
                           '<<<End captured STDERR>>>'
                           % (name, rc, stdout, stderr))
    return stdout


def expected_output_path(name):
    return root_path('taskflow', 'examples', '%s.out.txt' % name)


def list_examples():
    examples_dir = root_path('taskflow', 'examples')
    for filename in os.listdir(examples_dir):
        path = os.path.join(examples_dir, filename)
        if not os.path.isfile(path):
            continue
        name, ext = os.path.splitext(filename)
        if ext != ".py":
            continue
        bad_endings = []
        for i in ("utils", "no_test"):
            if name.endswith(i):
                bad_endings.append(True)
        if not any(bad_endings):
            yield name


class ExamplesTestCase(taskflow.test.TestCase):
    @classmethod
    def update(cls):
        """For each example, adds on a test method.

        This newly created test method will then be activated by the testing
        framework when it scans for and runs tests. This makes for a elegant
        and simple way to ensure that all of the provided examples
        actually work.
        """
        def add_test_method(name, method_name):
            def test_example(self):
                self._check_example(name)
            test_example.__name__ = method_name
            setattr(cls, method_name, test_example)

        for name in list_examples():
            safe_name = str(re.sub("[^a-zA-Z0-9_]+", "_", name))
            if re.match(r"^[_]+$", safe_name):
                continue
            add_test_method(name, 'test_%s' % safe_name)

    def _check_example(self, name):
        """Runs the example, and checks the output against expected output."""
        output = run_example(name)
        eop = expected_output_path(name)
        if os.path.isfile(eop):
            with open(eop) as f:
                expected_output = f.read()
            # NOTE(imelnikov): on each run new uuid is generated, so we just
            # replace them with some constant string
            output = UUID_RE.sub('<SOME UUID>', output)
            expected_output = UUID_RE.sub('<SOME UUID>', expected_output)
            self.assertEqual(output, expected_output)

ExamplesTestCase.update()


def make_output_files():
    """Generate output files for all examples."""
    for name in list_examples():
        output = run_example(name)
        with open(expected_output_path(name), 'w') as f:
            f.write(output)


if __name__ == '__main__':
    make_output_files()
