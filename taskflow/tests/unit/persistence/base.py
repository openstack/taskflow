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

import contextlib

from taskflow import exceptions as exc
from taskflow.openstack.common import uuidutils
from taskflow.persistence import logbook
from taskflow.utils import misc


class PersistenceTestMixin(object):
    def _get_connection():
        raise NotImplementedError()

    def test_logbook_save_retrieve(self):
        lb_id = uuidutils.generate_uuid()
        lb_meta = {'1': 2}
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id)
        lb.meta = lb_meta

        # Should not already exist
        with contextlib.closing(self._get_connection()) as conn:
            self.assertRaises(exc.NotFound, conn.get_logbook, lb_id)
            conn.save_logbook(lb)

        # Make sure we can reload it (and all of its attributes are what
        # we expect them to be).
        with contextlib.closing(self._get_connection()) as conn:
            lb = conn.get_logbook(lb_id)
        self.assertEqual(lb_name, lb.name)
        self.assertEqual(0, len(lb))
        self.assertEqual(lb_meta, lb.meta)
        self.assertIsNone(lb.updated_at)
        self.assertIsNotNone(lb.created_at)

    def test_flow_detail_save(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id)
        fd = logbook.FlowDetail('test', uuid=uuidutils.generate_uuid())
        lb.add(fd)

        # Ensure we can't save it since its owning logbook hasn't been
        # saved (flow details can not exist on there own without a connection
        # to a logbook).
        with contextlib.closing(self._get_connection()) as conn:
            self.assertRaises(exc.NotFound, conn.get_logbook, lb_id)
            self.assertRaises(exc.NotFound, conn.update_flow_details, fd)

        # Ok now we should be able to save both.
        with contextlib.closing(self._get_connection()) as conn:
            conn.save_logbook(lb)
            conn.update_flow_details(fd)

    def test_task_detail_save(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id)
        fd = logbook.FlowDetail('test', uuid=uuidutils.generate_uuid())
        lb.add(fd)
        td = logbook.TaskDetail("detail-1", uuid=uuidutils.generate_uuid())
        fd.add(td)

        # Ensure we can't save it since its owning logbook hasn't been
        # saved (flow details/task details can not exist on there own without
        # there parent existing).
        with contextlib.closing(self._get_connection()) as conn:
            self.assertRaises(exc.NotFound, conn.update_flow_details, fd)
            self.assertRaises(exc.NotFound, conn.update_task_details, td)

        # Ok now we should be able to save them.
        with contextlib.closing(self._get_connection()) as conn:
            conn.save_logbook(lb)
            conn.update_flow_details(fd)
            conn.update_task_details(td)

    def test_task_detail_with_failure(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id)
        fd = logbook.FlowDetail('test', uuid=uuidutils.generate_uuid())
        lb.add(fd)
        td = logbook.TaskDetail("detail-1", uuid=uuidutils.generate_uuid())

        try:
            raise RuntimeError('Woot!')
        except Exception:
            td.failure = misc.Failure()

        fd.add(td)

        with contextlib.closing(self._get_connection()) as conn:
            conn.save_logbook(lb)
            conn.update_flow_details(fd)
            conn.update_task_details(td)

        # Read failure back
        with contextlib.closing(self._get_connection()) as conn:
            lb2 = conn.get_logbook(lb_id)
        fd2 = lb2.find(fd.uuid)
        td2 = fd2.find(td.uuid)
        failure = td2.failure
        self.assertEqual(failure.exception_str, 'Woot!')
        self.assertIs(failure.check(RuntimeError), RuntimeError)
        self.assertEqual(failure.traceback_str, td.failure.traceback_str)

    def test_logbook_merge_flow_detail(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id)
        fd = logbook.FlowDetail('test', uuid=uuidutils.generate_uuid())
        lb.add(fd)
        with contextlib.closing(self._get_connection()) as conn:
            conn.save_logbook(lb)
        lb2 = logbook.LogBook(name=lb_name, uuid=lb_id)
        fd2 = logbook.FlowDetail('test2', uuid=uuidutils.generate_uuid())
        lb2.add(fd2)
        with contextlib.closing(self._get_connection()) as conn:
            conn.save_logbook(lb2)
        with contextlib.closing(self._get_connection()) as conn:
            lb3 = conn.get_logbook(lb_id)
            self.assertEqual(2, len(lb3))

    def test_logbook_add_flow_detail(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id)
        fd = logbook.FlowDetail('test', uuid=uuidutils.generate_uuid())
        lb.add(fd)
        with contextlib.closing(self._get_connection()) as conn:
            conn.save_logbook(lb)
        with contextlib.closing(self._get_connection()) as conn:
            lb2 = conn.get_logbook(lb_id)
            self.assertEqual(1, len(lb2))
            self.assertEqual(1, len(lb))
            self.assertEqual(fd.name, lb2.find(fd.uuid).name)

    def test_logbook_add_task_detail(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id)
        fd = logbook.FlowDetail('test', uuid=uuidutils.generate_uuid())
        td = logbook.TaskDetail("detail-1", uuid=uuidutils.generate_uuid())
        td.version = '4.2'
        fd.add(td)
        lb.add(fd)
        with contextlib.closing(self._get_connection()) as conn:
            conn.save_logbook(lb)
        with contextlib.closing(self._get_connection()) as conn:
            lb2 = conn.get_logbook(lb_id)
            self.assertEqual(1, len(lb2))
            tasks = 0
            for fd in lb:
                tasks += len(fd)
            self.assertEqual(1, tasks)
        with contextlib.closing(self._get_connection()) as conn:
            lb2 = conn.get_logbook(lb_id)
            fd2 = lb2.find(fd.uuid)
            td2 = fd2.find(td.uuid)
            self.assertIsNot(td2, None)
            self.assertEqual(td2.name, 'detail-1')
            self.assertEqual(td2.version, '4.2')

    def test_logbook_delete(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id)
        with contextlib.closing(self._get_connection()) as conn:
            self.assertRaises(exc.NotFound, conn.destroy_logbook, lb_id)
        with contextlib.closing(self._get_connection()) as conn:
            conn.save_logbook(lb)
        with contextlib.closing(self._get_connection()) as conn:
            lb2 = conn.get_logbook(lb_id)
            self.assertIsNotNone(lb2)
        with contextlib.closing(self._get_connection()) as conn:
            conn.destroy_logbook(lb_id)
            self.assertRaises(exc.NotFound, conn.destroy_logbook, lb_id)
