# -*- coding: utf-8 -*-

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

from taskflow import engines
from taskflow import formatters
from taskflow.listeners import logging as logging_listener
from taskflow.patterns import linear_flow
from taskflow import states
from taskflow import test
from taskflow.test import mock
from taskflow.test import utils as test_utils


class FormattersTest(test.TestCase):

    @staticmethod
    def _broken_atom_matcher(node):
        return node.item.name == 'Broken'

    def _make_test_flow(self):
        b = test_utils.TaskWithFailure("Broken")
        h_1 = test_utils.ProgressingTask("Happy-1")
        h_2 = test_utils.ProgressingTask("Happy-2")
        flo = linear_flow.Flow("test")
        flo.add(h_1, h_2, b)
        return flo

    def test_exc_info_format(self):
        flo = self._make_test_flow()
        e = engines.load(flo)
        self.assertRaises(RuntimeError, e.run)

        fails = e.storage.get_execute_failures()
        self.assertEqual(1, len(fails))
        self.assertIn('Broken', fails)
        fail = fails['Broken']

        f = formatters.FailureFormatter(e)
        (exc_info, details) = f.format(fail, self._broken_atom_matcher)
        self.assertEqual(3, len(exc_info))
        self.assertEqual("", details)

    @mock.patch('taskflow.formatters.FailureFormatter._format_node')
    def test_exc_info_with_details_format(self, mock_format_node):
        mock_format_node.return_value = 'A node'

        flo = self._make_test_flow()
        e = engines.load(flo)
        self.assertRaises(RuntimeError, e.run)
        fails = e.storage.get_execute_failures()
        self.assertEqual(1, len(fails))
        self.assertIn('Broken', fails)
        fail = fails['Broken']

        # Doing this allows the details to be shown...
        e.storage.set_atom_intention("Broken", states.EXECUTE)
        f = formatters.FailureFormatter(e)
        (exc_info, details) = f.format(fail, self._broken_atom_matcher)
        self.assertEqual(3, len(exc_info))
        self.assertTrue(mock_format_node.called)

    @mock.patch('taskflow.storage.Storage.get_execute_result')
    def test_exc_info_with_details_format_hidden(self, mock_get_execute):
        flo = self._make_test_flow()
        e = engines.load(flo)
        self.assertRaises(RuntimeError, e.run)
        fails = e.storage.get_execute_failures()
        self.assertEqual(1, len(fails))
        self.assertIn('Broken', fails)
        fail = fails['Broken']

        # Doing this allows the details to be shown...
        e.storage.set_atom_intention("Broken", states.EXECUTE)
        hide_inputs_outputs_of = ['Broken', "Happy-1", "Happy-2"]
        f = formatters.FailureFormatter(
            e, hide_inputs_outputs_of=hide_inputs_outputs_of)
        (exc_info, details) = f.format(fail, self._broken_atom_matcher)
        self.assertEqual(3, len(exc_info))
        self.assertFalse(mock_get_execute.called)

    @mock.patch('taskflow.formatters.FailureFormatter._format_node')
    def test_formatted_via_listener(self, mock_format_node):
        mock_format_node.return_value = 'A node'

        flo = self._make_test_flow()
        e = engines.load(flo)
        with logging_listener.DynamicLoggingListener(e):
            self.assertRaises(RuntimeError, e.run)
        self.assertTrue(mock_format_node.called)
