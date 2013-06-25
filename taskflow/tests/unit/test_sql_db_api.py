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
import os
import unittest2

from os import path

from taskflow.openstack.common import exception

from taskflow.db import api as db_api
from taskflow.db.sqlalchemy import models
from taskflow import states

db_api.configure()
db_api.SQL_CONNECTION = 'sqlite:///test.db'


def setUpModule():
    if not path.isfile('test.db'):
        models.create_tables()


def tearDownModule():
    os.remove('test.db')

"""
JobTest
"""


class JobTest(unittest2.TestCase):
    wf_ids = []
    wf_names = []
    lb_ids = []
    lb_names = []
    job_ids = []
    job_names = []

    @classmethod
    def setUpClass(cls):
        wf_fmt = u'workflow_{0}'
        db_api.logbook_create('', u'logbook_1', 1)
        cls.lb_ids.append(1)
        cls.lb_names.append(u'logbook_1')
        db_api.job_create('', u'job_1', 1)
        cls.job_ids.append(1)
        cls.job_names.append(u'job_1')
        for i in range(1, 10):
            db_api.workflow_create('', wf_fmt.format(i))

            db_api.logbook_add_workflow('', 1, wf_fmt.format(i))
            db_api.job_add_workflow('', 1, wf_fmt.format(i))

            cls.wf_ids.append(i)
            cls.wf_names.append(wf_fmt.format(i))

    @classmethod
    def tearDownClass(cls):
        for name in cls.wf_names:
            db_api.workflow_destroy('', name)
        for id in cls.lb_ids:
            db_api.logbook_destroy('', id)
        for id in cls.job_ids:
            db_api.job_destroy('', id)
        cls.wf_ids = []
        cls.wf_names = []
        cls.lb_ids = []
        cls.lb_names = []
        cls.job_ids = []
        cls.job_names = []

    def test_job_get(self):
        expected = self.job_names[0]
        actual = db_api.job_get('', self.job_ids[0]).name

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.job_get, '', 9001)

    def test_job_update(self):
        db_api.job_update('', 1, dict(owner='OwnerTest', state=states.CLAIMED))
        job = db_api.job_get('', 1)

        expected = 'OwnerTest'
        actual = job.owner

        self.assertEquals(expected, actual)

        expected = states.CLAIMED
        actual = job.state

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.job_update, '', 9001,
                          dict(owner='OwnerTest', state=states.CLAIMED))

    def test_job_add_workflow(self):
        db_api.workflow_create('', u'workflow_10')
        self.wf_ids.append(10)
        self.wf_names.append(u'workflow_10')

        expected = self.wf_ids
        actual = []
        temp = db_api.job_add_workflow('', 1, u'workflow_10')

        for workflow in temp:
            actual.append(workflow.id)

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.job_add_workflow, '',
                          9001, u'workflow_10')
        self.assertRaises(exception.NotFound, db_api.job_add_workflow, '',
                          1, u'workflow_9001')

    def test_job_get_owner(self):
        actual = db_api.job_get_owner('', 1)

        self.assertIsNone(actual)

        self.assertRaises(exception.NotFound, db_api.job_get_owner, '', 9001)

    def test_job_get_state(self):
        expected = states.UNCLAIMED
        actual = db_api.job_get_state('', 1)

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.job_get_state, '', 9001)

    def test_job_get_logbook(self):
        expected = self.lb_names[0]
        actual = db_api.job_get_logbook('', 1).name

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.job_get_logbook, '', 9001)

    def test_job_create(self):
        id = 1
        while (self.job_ids.count(id) > 0):
            id = id + 1
        db_api.job_create('', u'job_{0}'.format(id), id)
        self.job_ids.append(id)
        self.job_names.append(u'job_{0}'.format(id))

        actual = db_api.job_get('', id)
        self.assertIsNotNone(actual)

    def test_job_destroy(self):
        id = self.job_ids.pop()
        db_api.job_destroy('', id)
        self.job_names.pop()

        self.assertRaises(exception.NotFound, db_api.job_get, '', id)

"""
LogBookTest
"""


class LogBookTest(unittest2.TestCase):
    wf_ids = []
    wf_names = []
    lb_ids = []
    lb_names = []

    @classmethod
    def setUpClass(cls):
        wf_fmt = u'workflow_{0}'
        db_api.logbook_create('', u'logbook_1', 1)
        cls.lb_ids.append(1)
        cls.lb_names.append(u'logbook_1')
        for i in range(1, 10):
            db_api.workflow_create('', wf_fmt.format(i))

            db_api.logbook_add_workflow('', 1, wf_fmt.format(i))

            cls.wf_ids.append(i)
            cls.wf_names.append(wf_fmt.format(i))

    @classmethod
    def tearDownClass(cls):
        for name in cls.wf_names:
            db_api.workflow_destroy('', name)
        for id in cls.lb_ids:
            db_api.logbook_destroy('', id)
        cls.wf_ids = []
        cls.wf_names = []
        cls.lb_ids = []
        cls.lb_names = []

    def test_logbook_get(self):
        expected = self.lb_names[0]
        actual = db_api.logbook_get('', self.lb_ids[0]).name

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.logbook_get, '', 9001)

    def test_logbook_get_by_name(self):
        expected = [self.lb_ids[0]]
        actual = []
        for logbook in db_api.logbook_get_by_name('', self.lb_names[0]):
            actual.append(logbook.id)

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.logbook_get_by_name, '',
                          u'logbook_9001')

    def test_logbook_create(self):
        id = 1
        while (self.lb_ids.count(id) > 0):
            id = id + 1
        db_api.logbook_create('', u'logbook_{0}'.format(id), id)
        self.lb_ids.append(id)
        self.lb_names.append(u'logbook_{0}'.format(id))

        actual = db_api.logbook_get('', id)

        self.assertIsNotNone(actual)

    def test_logbook_get_workflows(self):
        expected = self.wf_ids
        actual = []
        wfs = db_api.logbook_get_workflows('', self.lb_ids[0])

        for workflow in wfs:
            actual.append(workflow.id)

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.logbook_get_workflows,
                          '', 9001)

    def test_logbook_add_workflow(self):
        db_api.workflow_create('', u'workflow_10')
        self.wf_ids.append(10)
        self.wf_names.append(u'workflow_10')

        expected = self.wf_ids
        actual = []
        temp = db_api.logbook_add_workflow('', 1, u'workflow_10')

        for workflow in temp:
            actual.append(workflow.id)

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.logbook_add_workflow, '',
                          9001, u'workflow_10')
        self.assertRaises(exception.NotFound, db_api.logbook_add_workflow, '',
                          1, u'workflow_9001')

    def test_logbook_destroy(self):
        id = self.lb_ids.pop()
        db_api.logbook_destroy('', id)
        self.lb_names.pop()

        self.assertRaises(exception.NotFound, db_api.logbook_get, '', id)

"""
WorkflowTest
"""


class WorkflowTest(unittest2.TestCase):
    tsk_ids = []
    tsk_names = []
    wf_ids = []
    wf_names = []

    @classmethod
    def setUpClass(cls):
        wf_fmt = u'workflow_{0}'
        tsk_fmt = u'task_{0}'
        for i in range(1, 10):
            db_api.workflow_create('', wf_fmt.format(i))
            db_api.task_create('', tsk_fmt.format(i), i, i)

            db_api.workflow_add_task('', wf_fmt.format(i), i)

            cls.tsk_ids.append(i)
            cls.tsk_names.append(tsk_fmt.format(i))
            cls.wf_ids.append(i)
            cls.wf_names.append(wf_fmt.format(i))

    @classmethod
    def teardownClass(cls):
        for id in cls.tsk_ids:
            db_api.task_destroy('', id)
        for name in cls.wf_names:
            db_api.workflow_destroy('', name)
        cls.tsk_ids = []
        cls.tsk_names = []
        cls.wf_ids = []
        cls.wf_names = []

    def test_workflow_get(self):
        expected = self.wf_ids[0]
        actual = db_api.workflow_get('', self.wf_names[0]).id

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.workflow_get, '',
                          u'workflow_9001')

    def test_workflow_get_all(self):
        expected = self.wf_ids
        actual = []
        temp = db_api.workflow_get_all('')

        for workflow in temp:
            actual.append(workflow.id)

        self.assertEquals(expected, actual)

    def test_workflow_get_names(self):
        expected = []
        for name in self.wf_names:
            expected.append(name)
        expected = tuple(expected)
        expected = [expected]
        actual = db_api.workflow_get_names('')

        self.assertEquals(expected, actual)

    def test_workflow_get_tasks(self):
        expected = [self.tsk_names[0], self.tsk_names[9]]
        actual = []
        temp = db_api.workflow_get_tasks('', u'workflow_1')

        for task in temp:
            actual.append(task.name)

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.workflow_get_tasks, '',
                          u'workflow_9001')

    def test_workflow_add_task(self):
        db_api.task_create('', u'task_10', 1, 10)
        db_api.workflow_add_task('', u'workflow_1', 10)
        self.tsk_ids.append(10)
        self.tsk_names.append('task_10')
        expected = [self.tsk_names[0], self.tsk_names[9]]
        tsks = db_api.workflow_get_tasks('', u'workflow_1')
        actual = [tsks[0].name, tsks[1].name]

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.workflow_add_task, '',
                          u'workflow_9001', 10)
        self.assertRaises(exception.NotFound, db_api.workflow_add_task, '',
                          u'workflow_1', 9001)

    def test_workflow_create(self):
        id = 0
        while (self.wf_ids.count(id) > 0):
            id = id + 1
        db_api.workflow_create('', u'workflow_{0}'.format(id))
        self.wf_ids.append(id)
        self.wf_names.append(u'workflow_{0}'.format(id))

        self.assertIsNotNone(db_api.workflow_get('',
                             u'workflow_{0}'.format(id)))

    def test_workflow_destroy(self):
        name = self.wf_names.pop()
        db_api.workflow_destroy('', name)
        self.wf_ids.pop()

        self.assertRaises(exception.NotFound, db_api.workflow_get, '', name)

"""
TaskTest
"""


class TaskTest(unittest2.TestCase):
    tsk_ids = []
    tsk_names = []

    @classmethod
    def setUpClass(cls):
        tsk_fmt = u'task_{0}'
        for i in range(1, 10):
            db_api.task_create('', tsk_fmt.format(i), i, i)
            cls.tsk_ids.append(i)
            cls.tsk_names.append(tsk_fmt.format(i))

    @classmethod
    def teardownClass(cls):
        for id in cls.tsk_ids:
            db_api.task_destroy('', id)
        cls.tsk_ids = []
        cls.tsk_names = []

    def test_task_get(self):
        expected = self.tsk_names[0]
        actual = db_api.task_get('', self.tsk_ids[0])

        self.assertEquals(expected, actual.name)

        self.assertRaises(exception.NotFound, db_api.task_get, '', 9001)

    def test_task_create(self):
        id = 1
        while (self.tsk_ids.count(id) > 0):
            id = id + 1
        db_api.task_create('', u'task_{0}'.format(id), 1, id)
        self.tsk_ids.append(id)
        self.tsk_names.append(u'task_{0}'.format(id))

        self.assertIsNotNone(db_api.task_get('', id))

    def test_task_update(self):
        db_api.task_update('', 1, dict(exception='ExceptionTest',
                           stacktrace='StacktraceTest'))
        task = db_api.task_get('', 1)

        expected = 'ExceptionTest'
        actual = task.exception

        self.assertEquals(expected, actual)

        expected = 'StacktraceTest'
        actual = task.stacktrace

        self.assertEquals(expected, actual)

        self.assertRaises(exception.NotFound, db_api.task_update, '', 9001,
                          dict(exception='ExceptionTest',
                               stacktrace='StacktraceTest'))

    def test_task_destroy(self):
        id = self.tsk_ids.pop()
        db_api.task_destroy('', id)
        self.tsk_names.pop()

        self.assertRaises(exception.NotFound, db_api.task_get, '', id)
