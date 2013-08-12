# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Rackspace Hosting All Rights Reserved.
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

"""Import required libraries"""
import unittest2

from taskflow.backends import api as b_api
from taskflow import exceptions as exception
from taskflow.generics import taskdetail
from taskflow.openstack.common import uuidutils
from taskflow.tests import utils


class TaskDetailTest(unittest2.TestCase):
    """This class is designed to test the functionality of the backend API's
       taskdetail methods
    """
    tsks = []
    td_names = []
    td_ids = []

    @classmethod
    def setUpClass(cls):
        # Create a task for taskdetails to be made from
        task_id = uuidutils.generate_uuid()
        task_name = 'task-%s' % (task_id)

        tsk = utils.DummyTask(task_name, task_id)
        tsk.requires.update('r')
        tsk.optional.update('o')
        tsk.provides.update('p')

        cls.tsks.append(tsk)

    @classmethod
    def tearDownClass(cls):
        # Clear the tasks
        utils.drain(cls.tsks)

    def setUp(self):
        # Create a taskdetail and record its uuid and name
        td_id = uuidutils.generate_uuid()
        td_name = 'td-%s' % (td_id)

        b_api.taskdetail_create(td_name, self.tsks[0], td_id)
        self.td_names.append(td_name)
        self.td_ids.append(td_id)

    def tearDown(self):
        # Destroy all taskdetails from the backend
        for id in self.td_ids:
            b_api.taskdetail_destroy(id)

        # Clear the list of taskdetail names and uuids
        utils.drain(self.td_names)
        utils.drain(self.td_ids)

    def test_taskdetail_create(self):
        # Create a taskdetail and record its uuid and name
        td_id = uuidutils.generate_uuid()
        td_name = 'td-%s' % (td_id)

        b_api.taskdetail_create(td_name, self.tsks[0], td_id)
        self.td_names.append(td_name)
        self.td_ids.append(td_id)

        # Check that the taskdetail is there
        actual = b_api.taskdetail_get(td_id)

        self.assertIsNotNone(actual)

    def test_taskdetail_destroy(self):
        # Destroy the last added taskdetail
        id = self.td_ids.pop()
        b_api.taskdetail_destroy(id)
        self.td_names.pop()

        # Check that the deleted taskdetail is no longer there
        self.assertRaises(exception.NotFound, b_api.taskdetail_get,
                          id)

    def test_taskdetail_save(self):
        # Create a generic taskdetail to save
        td_id = uuidutils.generate_uuid()
        td_name = 'td-%s' % (td_id)
        tsk = self.tsks[0]
        td = taskdetail.TaskDetail(td_name, tsk, td_id)

        # Save the generic taskdetail to the backend and record uuid/name
        b_api.taskdetail_save(td)
        self.td_names.append(td_name)
        self.td_ids.append(td_id)

        # Get the created taskdetail and check for default attributes
        actual = b_api.taskdetail_get(td_id)

        self.assertIsNotNone(actual)
        self.assertIsNone(actual.state)
        self.assertIsNone(actual.results)
        self.assertIsNone(actual.exception)
        self.assertIsNone(actual.stacktrace)
        self.assertIsNone(actual.meta)

        # Change the generic taskdetail's attributes
        td.state = 'SUCCESS'
        td.exception = 'ERROR'
        td.stacktrace = 'STACKTRACE'
        td.meta = 'META'

        # Save the changed taskdetail
        b_api.taskdetail_save(td)

        # Get the updated taskdetail and check for updated attributes
        actual = b_api.taskdetail_get(td_id)

        self.assertEquals(actual.state, 'SUCCESS')
        self.assertIsNone(actual.results)
        self.assertEquals(actual.exception, 'ERROR')
        self.assertEquals(actual.stacktrace, 'STACKTRACE')
        self.assertEquals(actual.meta, 'META')

    def test_taskdetail_delete(self):
        # Get the taskdetail to delete
        id = self.td_ids.pop()
        td = b_api.taskdetail_get(id)
        # Delete the desired taskdetail
        b_api.taskdetail_delete(td)
        self.td_names.pop()

        # Check that the deleted taskdetail is no longer there
        self.assertRaises(exception.NotFound, b_api.taskdetail_get,
                          id)

    def test_taskdetail_get(self):
        # Get the first taskdetail
        actual = b_api.taskdetail_get(self.td_ids[0])

        # Check that it is actually a taskdetail
        self.assertIsInstance(actual, taskdetail.TaskDetail)
        # Check that its name is what we expect
        self.assertEquals(actual.name, self.td_names[0])

    def test_taskdetail_update(self):
        # Get the first taskdetail and check for default attributes
        actual = b_api.taskdetail_get(self.td_ids[0])

        self.assertIsNone(actual.state)
        self.assertIsNone(actual.results)
        self.assertIsNone(actual.exception)
        self.assertIsNone(actual.stacktrace)
        self.assertIsNone(actual.meta)

        # Prepare attributes for updating
        values = dict(state='SUCCESS', exception='ERROR',
                      stacktrace='STACKTRACE', meta='META')

        # Update attributes
        b_api.taskdetail_update(self.td_ids[0], values)

        # Get the updated taskdetila and check for updated attributes
        actual = b_api.taskdetail_get(self.td_ids[0])

        self.assertEquals(actual.state, 'SUCCESS')
        self.assertIsNone(actual.results)
        self.assertEquals(actual.exception, 'ERROR')
        self.assertEquals(actual.stacktrace, 'STACKTRACE')
        self.assertEquals(actual.meta, 'META')

    def test_taskdetail_get_ids_names(self):
        # Get dict of uuids and names
        actual = b_api.taskdetail_get_ids_names()

        # Check that it matches our in-memory records
        self.assertEquals(actual.values(), self.td_names)
        self.assertEquals(actual.keys(), self.td_ids)
