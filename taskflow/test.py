# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

from testtools import compat
from testtools import matchers
from testtools import testcase


class GreaterThanEqual(object):
    """Matches if the item is geq than the matchers reference object."""

    def __init__(self, source):
        self.source = source

    def match(self, other):
        if other >= self.source:
            return None
        return matchers.Mismatch("%s was not >= %s" % (other, self.source))


class TestCase(testcase.TestCase):
    """Test case base class for all taskflow unit tests."""

    def assertRaisesRegexp(self, exc_class, pattern, callable_obj,
                           *args, **kwargs):
        # TODO(harlowja): submit a pull/review request to testtools to add
        # this method to there codebase instead of having it exist in ours
        # since it really doesn't belong here.

        class ReRaiseOtherTypes(object):
            def match(self, matchee):
                if not issubclass(matchee[0], exc_class):
                    compat.reraise(*matchee)

        class CaptureMatchee(object):
            def match(self, matchee):
                self.matchee = matchee[1]

        capture = CaptureMatchee()
        matcher = matchers.Raises(matchers.MatchesAll(ReRaiseOtherTypes(),
                                  matchers.MatchesException(exc_class,
                                                            pattern),
                                  capture))
        our_callable = testcase.Nullary(callable_obj, *args, **kwargs)
        self.assertThat(our_callable, matcher)
        return capture.matchee

    def assertGreater(self, first, second):
        matcher = matchers.GreaterThan(first)
        self.assertThat(second, matcher)

    def assertGreaterEqual(self, first, second):
        matcher = GreaterThanEqual(first)
        self.assertThat(second, matcher)

    def assertRegexpMatches(self, text, pattern):
        matcher = matchers.MatchesRegex(pattern)
        self.assertThat(text, matcher)

    def assertIsSubset(self, super_set, sub_set, msg=None):
        missing_set = set()
        for e in sub_set:
            if e not in super_set:
                missing_set.add(e)
        if len(missing_set):
            if msg is not None:
                self.fail(msg)
            else:
                self.fail("Subset %s has %s elements which are not in the "
                          "superset %s." % (sub_set, list(missing_set),
                                            list(super_set)))
