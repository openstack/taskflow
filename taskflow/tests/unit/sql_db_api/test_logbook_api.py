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
from taskflow.generics import logbook
from taskflow.openstack.common import uuidutils
from taskflow.patterns import graph_flow as flow
from taskflow.tests import utils


class LogBookTest(unittest2.TestCase):
    """This class is designed to test the functionality of the backend API's
       logbook methods
    """
    lb_names = []
    lb_ids = []
    wfs = []
    fd_names = []
    fd_ids = []

    @classmethod
    def setUpClass(cls):
        # Create a workflow to create flowdetails with
        wf_id = uuidutils.generate_uuid()
        wf_name = 'wf-%s' % (wf_id)

        wf = flow.Flow(wf_name, None, wf_id)
        cls.wfs.append(wf)

    @classmethod
    def tearDownClass(cls):
        # Empty the list of workflows
        utils.drain(cls.wfs)

    def setUp(self):
        # Create a logbook and record its uuid and name
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)

        b_api.logbook_create(lb_name, lb_id)
        self.lb_names.append(lb_name)
        self.lb_ids.append(lb_id)

        # Create a flowdetail and record its uuid and name
        fd_id = uuidutils.generate_uuid()
        fd_name = 'fd-%s' % (fd_id)

        b_api.flowdetail_create(fd_name, self.wfs[0], fd_id)
        self.fd_names.append(fd_name)
        self.fd_ids.append(fd_id)

    def tearDown(self):
        # Destroy all flowdetails and logbooks in the backend
        for id in self.fd_ids:
            b_api.flowdetail_destroy(id)
        for id in self.lb_ids:
            b_api.logbook_destroy(id)

        # Clear the lists of logbook and flowdetail uuids and names
        utils.drain(self.lb_names)
        utils.drain(self.lb_ids)
        utils.drain(self.fd_names)
        utils.drain(self.fd_ids)

    def test_logbook_create(self):
        # Create a logbook and record its uuid and name
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)

        b_api.logbook_create(lb_name, lb_id)
        self.lb_names.append(lb_name)
        self.lb_ids.append(lb_id)

        # Check that the created logbook exists in the backend
        actual = b_api.logbook_get(lb_id)

        self.assertIsNotNone(actual)

    def test_logbook_destroy(self):
        # Delete the last added logbook
        id = self.lb_ids.pop()
        b_api.logbook_destroy(id)
        self.lb_names.pop()

        # Check that the deleted logbook is no longer there
        self.assertRaises(exception.NotFound, b_api.logbook_get,
                          id)

    def test_logbook_save(self):
        # Create a generic logbook to save
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(lb_name, lb_id)

        # Save the logbook and record its uuid and name
        b_api.logbook_save(lb)
        self.lb_names.append(lb_name)
        self.lb_ids.append(lb_id)

        # Check that the saved logbook exists in the backend
        actual = b_api.logbook_get(lb_id)

        self.assertIsNotNone(actual)
        # Check that the saved logbook has no flowdetails
        self.assertEquals(len(actual), 0)

        # Add a flowdetail to the logbook
        fd = b_api.flowdetail_get(self.fd_ids[0])
        lb.add_flow_detail(fd)

        # Save the updated logbook
        b_api.logbook_save(lb)

        # Check that the updated logbook is still in the backend
        actual = b_api.logbook_get(lb_id)

        self.assertIsNotNone(actual)
        # Check that the added flowdetail was recorded
        self.assertEquals(len(actual), 1)

    def test_logbook_delete(self):
        # Get the logbook to delete
        id = self.lb_ids.pop()
        lb = b_api.logbook_get(id)
        # Delete the logbook from the backend
        b_api.logbook_delete(lb)
        self.lb_names.pop()

        # Check that the deleted logbook is no longer present
        self.assertRaises(exception.NotFound, b_api.logbook_get,
                          id)

    def test_logbook_get(self):
        # Get the logbook from the backend
        actual = b_api.logbook_get(self.lb_ids[0])

        # Check that it is actually a logbook
        self.assertIsInstance(actual, logbook.LogBook)
        # Check that the name is correct
        self.assertEquals(actual.name, self.lb_names[0])

    def test_logbook_add_flow_detail(self):
        # Get the logbook from the backend
        actual = b_api.logbook_get(self.lb_ids[0])

        # Check that it has no flowdetails
        self.assertEquals(len(actual), 0)

        # Add a flowdetail to the logbook
        b_api.logbook_add_flow_detail(self.lb_ids[0], self.fd_ids[0])

        # Get the logbook again
        actual = b_api.logbook_get(self.lb_ids[0])

        # Check that the logbook has exactly one flowdetail
        self.assertEquals(len(actual), 1)

    def test_logbook_remove_flowdetail(self):
        # Add a flowdetail to the first logbook
        b_api.logbook_add_flow_detail(self.lb_ids[0], self.fd_ids[0])

        # Get the first logbook
        actual = b_api.logbook_get(self.lb_ids[0])

        # Check that it has exactly one flowdetail
        self.assertEquals(len(actual), 1)

        # Remove the flowdetail from the logbook
        b_api.logbook_remove_flowdetail(self.lb_ids[0], self.fd_ids[0])

        # Get the logbook again
        actual = b_api.logbook_get(self.lb_ids[0])

        # Check that the logbook now has no flowdetails
        self.assertEquals(len(actual), 0)

    def test_logbook_get_ids_names(self):
        # Get the dict of uuids and names
        actual = b_api.logbook_get_ids_names()

        # Check that it matches our in-memory list
        self.assertEquals(actual.values(), self.lb_names)
        self.assertEquals(actual.keys(), self.lb_ids)
