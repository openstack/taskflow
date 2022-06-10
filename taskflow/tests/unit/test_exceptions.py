# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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

import string

from taskflow import exceptions as exc
from taskflow import test


class TestExceptions(test.TestCase):
    def test_cause(self):
        capture = None
        try:
            raise exc.TaskFlowException("broken", cause=IOError("dead"))
        except Exception as e:
            capture = e
        self.assertIsNotNone(capture)
        self.assertIsInstance(capture, exc.TaskFlowException)
        self.assertIsNotNone(capture.cause)
        self.assertIsInstance(capture.cause, IOError)

    def test_cause_pformat(self):
        capture = None
        try:
            raise exc.TaskFlowException("broken", cause=IOError("dead"))
        except Exception as e:
            capture = e
        self.assertIsNotNone(capture)
        self.assertGreater(0, len(capture.pformat()))

    def test_raise_with(self):
        capture = None
        try:
            raise IOError('broken')
        except Exception:
            try:
                exc.raise_with_cause(exc.TaskFlowException, 'broken')
            except Exception as e:
                capture = e
        self.assertIsNotNone(capture)
        self.assertIsInstance(capture, exc.TaskFlowException)
        self.assertIsNotNone(capture.cause)
        self.assertIsInstance(capture.cause, IOError)

    def test_no_looping(self):
        causes = []
        for a in string.ascii_lowercase:
            try:
                cause = causes[-1]
            except IndexError:
                cause = None
            causes.append(exc.TaskFlowException('%s broken' % a, cause=cause))
        e = causes[0]
        last_e = causes[-1]
        e._cause = last_e
        self.assertIsNotNone(e.pformat())

    def test_pformat_str(self):
        ex = None
        try:
            try:
                try:
                    raise IOError("Didn't work")
                except IOError:
                    exc.raise_with_cause(exc.TaskFlowException,
                                         "It didn't go so well")
            except exc.TaskFlowException:
                exc.raise_with_cause(exc.TaskFlowException, "I Failed")
        except exc.TaskFlowException as e:
            ex = e

        self.assertIsNotNone(ex)
        self.assertIsInstance(ex, exc.TaskFlowException)
        self.assertIsInstance(ex.cause, exc.TaskFlowException)
        self.assertIsInstance(ex.cause.cause, IOError)

        p_msg = ex.pformat()
        p_str_msg = str(ex)
        for msg in ["I Failed", "It didn't go so well", "Didn't work"]:
            self.assertIn(msg, p_msg)
            self.assertIn(msg, p_str_msg)

    def test_pformat_root_class(self):
        ex = exc.TaskFlowException("Broken")
        self.assertIn("TaskFlowException",
                      ex.pformat(show_root_class=True))
        self.assertNotIn("TaskFlowException",
                         ex.pformat(show_root_class=False))
        self.assertIn("Broken",
                      ex.pformat(show_root_class=True))

    def test_invalid_pformat_indent(self):
        ex = exc.TaskFlowException("Broken")
        self.assertRaises(ValueError, ex.pformat, indent=-100)

    def test_raise_with_cause(self):
        capture = None
        try:
            raise IOError('broken')
        except Exception:
            try:
                exc.raise_with_cause(exc.TaskFlowException, 'broken')
            except Exception as e:
                capture = e
        self.assertIsNotNone(capture)
        self.assertIsInstance(capture, exc.TaskFlowException)
        self.assertIsNotNone(capture.cause)
        self.assertIsInstance(capture.cause, IOError)
        self.assertIsNotNone(capture.__cause__)
        self.assertIsInstance(capture.__cause__, IOError)
