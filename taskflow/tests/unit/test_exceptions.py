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

import six
import testtools

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

    @testtools.skipIf(not six.PY3, 'py3.x is not available')
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
