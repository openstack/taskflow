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

from taskflow import exceptions as exception
from taskflow.openstack.common import uuidutils
from taskflow.patterns import graph_flow as flow
from taskflow.persistence.backends import api as b_api
from taskflow.persistence import flowdetail
from taskflow.tests import utils


class FlowDetailTest(unittest2.TestCase):
    """This class is to test the functionality of the backend API's flowdetail
       methods.
    """
    wfs = []
    fd_names = []
    fd_ids = []
    tsks = []
    td_names = []
    td_ids = []

    @classmethod
    def setUpClass(cls):
        # Create a workflow for flowdetails to use
        wf_id = uuidutils.generate_uuid()
        wf_name = 'wf-%s' % (wf_id)

        wf = flow.Flow(wf_name, None, wf_id)
        cls.wfs.append(wf)

        # Create a task for taskdetails to use
        task_id = uuidutils.generate_uuid()
        task_name = 'task-%s' % (task_id)

        tsk = utils.DummyTask(task_name, task_id)
        cls.tsks.append(tsk)

    @classmethod
    def tearDownClass(cls):
        # Clear out the lists of workflows and tasks
        utils.drain(cls.wfs)
        utils.drain(cls.tsks)

    def setUp(self):
        # Create a flowdetail and record its uuid and name
        fd_id = uuidutils.generate_uuid()
        fd_name = 'fd-%s' % (fd_id)

        b_api.flowdetail_create(fd_name, self.wfs[0], fd_id)
        self.fd_names.append(fd_name)
        self.fd_ids.append(fd_id)

        # Create a taskdetail and record its uuid and name
        td_id = uuidutils.generate_uuid()
        td_name = 'td-%s' % (td_id)

        b_api.taskdetail_create(td_name, self.tsks[0], td_id)
        self.td_names.append(td_name)
        self.td_ids.append(td_id)

    def tearDown(self):
        # Destroy all taskdetails and flowdetails in the backend
        for id in self.td_ids:
            b_api.taskdetail_destroy(id)
        for id in self.fd_ids:
            b_api.flowdetail_destroy(id)

        # Drain the lists of taskdetail and flowdetail uuids and names
        utils.drain(self.fd_names)
        utils.drain(self.fd_ids)
        utils.drain(self.td_names)
        utils.drain(self.td_ids)

    def test_flowdetail_create(self):
        # Create a flowdetail and record its uuid and name
        fd_id = uuidutils.generate_uuid()
        fd_name = 'fd-%s' % (fd_id)

        b_api.flowdetail_create(fd_name, self.wfs[0], fd_id)
        self.fd_names.append(fd_name)
        self.fd_ids.append(fd_id)

        # Check to see that the created flowdetail is there
        actual = b_api.flowdetail_get(fd_id)

        self.assertIsNotNone(actual)

    def test_flowdetail_destroy(self):
        # Destroy the last added flowdetail
        id = self.fd_ids.pop()
        b_api.flowdetail_destroy(id)
        self.fd_names.pop()

        # Check to make sure the removed flowdetail is no longer there
        self.assertRaises(exception.NotFound, b_api.flowdetail_get,
                          id)

    def test_flowdetail_save(self):
        # Create a generic flowdetail to save
        fd_id = uuidutils.generate_uuid()
        fd_name = 'fd-%s' % (fd_id)
        wf = self.wfs[0]
        fd = flowdetail.FlowDetail(fd_name, wf, fd_id)

        # Save the generic flowdetail to the backend and record its uuid/name
        b_api.flowdetail_save(fd)
        self.fd_names.append(fd_name)
        self.fd_ids.append(fd_id)

        # Check that the saved flowdetail is in the backend
        actual = b_api.flowdetail_get(fd_id)

        self.assertIsNotNone(actual)
        # Check that the saved flowdetail has no taskdetails
        self.assertEquals(len(actual), 0)

        # Add a generic taskdetail to the flowdetail
        td = b_api.taskdetail_get(self.td_ids[0])
        fd.add_task_detail(td)

        # Save the updated flowdetail
        b_api.flowdetail_save(fd)

        # Check that the saved flowdetail is still there
        actual = b_api.flowdetail_get(fd_id)

        self.assertIsNotNone(actual)
        # Check that the addition of a taskdetail was recorded
        self.assertEquals(len(actual), 1)

    def test_flowdetail_delete(self):
        # Get the flowdetail to delete
        id = self.fd_ids.pop()
        fd = b_api.flowdetail_get(id)
        # Delete the flowdetail
        b_api.flowdetail_delete(fd)
        self.fd_names.pop()

        # Make sure it is not there anymore
        self.assertRaises(exception.NotFound, b_api.flowdetail_get,
                          id)

    def test_flowdetail_get(self):
        # Get the first flowdetail
        actual = b_api.flowdetail_get(self.fd_ids[0])

        # Check that it is a flowdetail
        self.assertIsInstance(actual, flowdetail.FlowDetail)
        # Check that its name matches what is expected
        self.assertEquals(actual.name, self.fd_names[0])

    def test_flowdetail_add_task_detail(self):
        # Get the first flowdetail
        actual = b_api.flowdetail_get(self.fd_ids[0])

        # Make sure it has no taskdetails
        self.assertEquals(len(actual), 0)

        # Add a taskdetail to the flowdetail
        b_api.flowdetail_add_task_detail(self.fd_ids[0], self.td_ids[0])

        # Get the flowdetail again
        actual = b_api.flowdetail_get(self.fd_ids[0])

        # Check that the flowdetail has one taskdetail
        self.assertEquals(len(actual), 1)

    def test_flowdetail_remove_taskdetail(self):
        # Add a taskdetail to the first flowdetail
        b_api.flowdetail_add_task_detail(self.fd_ids[0], self.td_ids[0])

        # Get the first flowdetail
        actual = b_api.flowdetail_get(self.fd_ids[0])

        # Check that the first flowdetail has exactly one taskdetail
        self.assertEquals(len(actual), 1)

        # Remove the taskdetail from the first flowdetail
        b_api.flowdetail_remove_taskdetail(self.fd_ids[0], self.td_ids[0])

        # Get the first flowdetail
        actual = b_api.flowdetail_get(self.fd_ids[0])

        # Check that the first flowdetail no longer has any taskdetails
        self.assertEquals(len(actual), 0)

    def test_flowdetail_get_ids_names(self):
        # Get a list of all uuids and names for flowdetails
        actual = b_api.flowdetail_get_ids_names()

        # Match it to our in-memory records
        self.assertEquals(actual.values(), self.fd_names)
        self.assertEquals(actual.keys(), self.fd_ids)
