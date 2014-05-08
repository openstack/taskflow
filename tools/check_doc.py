#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2014 Ivan Melnikov <iv at altlinux dot org>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


"""Check documentation for simple style requirements.

What is checked:
    - lines should not be longer than 79 characters
      - exception: line with no whitespace except maybe in the beginning
      - exception: line that starts with '..' -- longer directives are allowed,
        including footnotes
    - no tabulation for indentation
    - no trailing whitespace
"""

import fnmatch
import os
import re
import sys


FILE_PATTERNS = ['*.rst', '*.txt']
MAX_LINE_LENGTH = 79
TRAILING_WHITESPACE_REGEX = re.compile('\s$')
STARTING_WHITESPACE_REGEX = re.compile('^(\s+)')


def check_max_length(line):
    if len(line) > MAX_LINE_LENGTH:
        stripped = line.strip()
        if not any((
            line.startswith('..'),  # this is directive
            stripped.startswith('>>>'),  # this is doctest
            stripped.startswith('...'),  # and this
            stripped.startswith('taskflow.'),
            ' ' not in stripped  # line can't be split
        )):
            yield ('D001', 'Line too long')


def check_trailing_whitespace(line):
    if TRAILING_WHITESPACE_REGEX.search(line):
        yield ('D002', 'Trailing whitespace')


def check_indentation_no_tab(line):
    match = STARTING_WHITESPACE_REGEX.search(line)
    if match:
        spaces = match.group(1)
        if '\t' in spaces:
            yield ('D003', 'Tabulation used for indentation')


LINE_CHECKS = (check_max_length,
               check_trailing_whitespace,
               check_indentation_no_tab)


def check_lines(lines):
    for idx, line in enumerate(lines, 1):
        line = line.rstrip('\n')
        for check in LINE_CHECKS:
            for code, message in check(line):
                yield idx, code, message


def check_files(filenames):
    for fn in filenames:
        with open(fn) as f:
            for line_num, code, message in check_lines(f):
                yield fn, line_num, code, message


def find_files(pathes, patterns):
    for path in pathes:
        if os.path.isfile(path):
            yield path
        elif os.path.isdir(path):
            for root, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    if any(fnmatch.fnmatch(filename, pattern)
                           for pattern in patterns):
                        yield os.path.join(root, filename)
        else:
            print('Invalid path: %s' % path)


def main():
    ok = True
    if len(sys.argv) > 1:
        dirs = sys.argv[1:]
    else:
        dirs = ['.']
    for error in check_files(find_files(dirs, FILE_PATTERNS)):
        ok = False
        print('%s:%s: %s %s' % error)
    sys.exit(0 if ok else 1)

if __name__ == '__main__':
    main()
