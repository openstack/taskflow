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


import keyword
import os
import re
import subprocess
import sys

import six

from taskflow import test

ROOT_DIR = os.path.abspath(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(__file__))))

# This is used so that any uuid like data being output is removed (since it
# will change per test run and will invalidate the deterministic output that
# we expect to be able to check).
UUID_RE = re.compile('XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX'
                     .replace('X', '[0-9a-f]'))


def safe_filename(filename):
    # Translates a filename into a method name, returns falsey if not
    # possible to perform this translation...
    name = re.sub("[^a-zA-Z0-9_]+", "_", filename)
    if not name or re.match(r"^[_]+$", name) or keyword.iskeyword(name):
        return False
    return name


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


def iter_examples():
    examples_dir = root_path('taskflow', 'examples')
    for filename in os.listdir(examples_dir):
        path = os.path.join(examples_dir, filename)
        if not os.path.isfile(path):
            continue
        name, ext = os.path.splitext(filename)
        if ext != ".py":
            continue
        if not name.endswith('utils'):
            safe_name = safe_filename(name)
            if safe_name:
                yield name, safe_name


class ExampleAdderMeta(type):
    """Translates examples into test cases/methods."""

    def __new__(cls, name, parents, dct):

        def generate_test(example_name):
            def test_example(self):
                self._check_example(example_name)
            return test_example

        for example_name, safe_name in iter_examples():
            test_name = 'test_%s' % safe_name
            dct[test_name] = generate_test(example_name)

        return type.__new__(cls, name, parents, dct)


@six.add_metaclass(ExampleAdderMeta)
class ExamplesTestCase(test.TestCase):
    """Runs the examples, and checks the outputs against expected outputs."""

    def _check_example(self, name):
        output = run_example(name)
        eop = expected_output_path(name)
        if os.path.isfile(eop):
            with open(eop) as f:
                expected_output = f.read()
            # NOTE(imelnikov): on each run new uuid is generated, so we just
            # replace them with some constant string
            output = UUID_RE.sub('<SOME UUID>', output)
            expected_output = UUID_RE.sub('<SOME UUID>', expected_output)
            self.assertEqual(expected_output, output)


def make_output_files():
    """Generate output files for all examples."""
    for example_name, _safe_name in iter_examples():
        print("Running %s" % example_name)
        print("Please wait...")
        output = run_example(example_name)
        with open(expected_output_path(example_name), 'w') as f:
            f.write(output)


if __name__ == '__main__':
    make_output_files()
