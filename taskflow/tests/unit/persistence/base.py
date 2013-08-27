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

from taskflow import exceptions as exc
from taskflow.openstack.common import uuidutils
from taskflow.persistence import flowdetail
from taskflow.persistence import logbook
from taskflow.persistence import taskdetail


class PersistenceTestMixin(object):
    def _get_backend():
        raise NotImplementedError()

    def test_logbook_simple_save(self):
        lb_id = uuidutils.generate_uuid()
        lb_meta = {'1': 2}
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id,
                             backend=self._get_backend())
        lb.meta = lb_meta

        # Should not already exist
        self.assertRaises(exc.NotFound, logbook.load, lb_id,
                          backend=self._get_backend())

        lb.save()
        del lb
        lb = None

        lb = logbook.load(lb_id, backend=self._get_backend())
        self.assertEquals(lb_name, lb.name)
        self.assertEquals(0, len(lb))
        self.assertEquals(lb_meta, lb.meta)
        self.assertIsNone(lb.updated_at)
        self.assertIsNotNone(lb.created_at)

    def test_flow_detail_save(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id,
                             backend=self._get_backend())

        fd = flowdetail.FlowDetail('test', uuid=uuidutils.generate_uuid())
        lb.add(fd)

        # Ensure we can't save it since its owning logbook hasn't been
        # saved.
        self.assertRaises(exc.NotFound, fd.save)

        # Ok now we should be able to save it
        lb.save()
        fd.save()

    def test_task_detail_save(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id,
                             backend=self._get_backend())

        fd = flowdetail.FlowDetail('test', uuid=uuidutils.generate_uuid())
        lb.add(fd)
        td = taskdetail.TaskDetail("detail-1", uuid=uuidutils.generate_uuid())
        fd.add(td)

        # Ensure we can't save it since its owning logbook hasn't been
        # saved.
        self.assertRaises(exc.NotFound, fd.save)
        self.assertRaises(exc.NotFound, td.save)

        # Ok now we should be able to save it
        lb.save()
        fd.save()
        td.save()

    def test_logbook_merge_flow_detail(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id,
                             backend=self._get_backend())

        fd = flowdetail.FlowDetail('test', uuid=uuidutils.generate_uuid())
        lb.add(fd)
        lb.save()

        lb2 = logbook.LogBook(name=lb_name, uuid=lb_id,
                              backend=self._get_backend())
        fd = flowdetail.FlowDetail('test2', uuid=uuidutils.generate_uuid())
        lb2.add(fd)
        lb2.save()

        lb3 = logbook.load(lb_id, backend=self._get_backend())
        self.assertEquals(2, len(lb3))

    def test_logbook_add_flow_detail(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id,
                             backend=self._get_backend())

        fd = flowdetail.FlowDetail('test', uuid=uuidutils.generate_uuid())
        lb.add(fd)
        lb.save()

        lb2 = logbook.load(lb_id, backend=self._get_backend())
        self.assertEquals(1, len(lb2))
        self.assertEquals(1, len(lb))

        self.assertEquals(fd.name, lb2.find(fd.uuid).name)

    def test_logbook_add_task_detail(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id,
                             backend=self._get_backend())

        fd = flowdetail.FlowDetail('test', uuid=uuidutils.generate_uuid())
        td = taskdetail.TaskDetail("detail-1", uuid=uuidutils.generate_uuid())
        fd.add(td)
        lb.add(fd)
        lb.save()

        lb2 = logbook.load(lb_id, backend=self._get_backend())
        self.assertEquals(1, len(lb2))
        tasks = 0
        for fd in lb:
            tasks += len(fd)
        self.assertEquals(1, tasks)

    def test_logbook_delete(self):
        lb_id = uuidutils.generate_uuid()
        lb_name = 'lb-%s' % (lb_id)
        lb = logbook.LogBook(name=lb_name, uuid=lb_id,
                             backend=self._get_backend())

        # Ensure we can't delete it since it hasn't been saved
        self.assertRaises(exc.NotFound, lb.delete)

        lb.save()

        lb2 = logbook.load(lb_id, backend=self._get_backend())
        self.assertIsNotNone(lb2)

        lb.delete()

        self.assertRaises(exc.NotFound, lb.delete)
