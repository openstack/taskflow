# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2014 AT&T Labs All Rights Reserved.
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
import logging

from kazoo import client as kazoo_client
from kazoo import exceptions as k_exc
from kazoo.protocol import paths
from zake import fake_client

from taskflow import exceptions as exc
from taskflow.openstack.common import jsonutils
from taskflow.persistence.backends import base
from taskflow.persistence import logbook
from taskflow.utils import misc
from taskflow.utils import persistence_utils as p_utils

LOG = logging.getLogger(__name__)


class ZkBackend(base.Backend):
    """ZooKeeper as backend storage implementation

    Example conf (use Kazoo):

    conf = {
        "hosts": "192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181",
        "path": "/taskflow",
    }

    Example conf (use Zake):

    conf = {
        "path": "/taskflow",
    }
    """
    def __init__(self, conf):
        super(ZkBackend, self).__init__(conf)
        path = conf.get("path", "/taskflow")
        self._path = path
        hosts = conf.get("hosts", None)
        # if no specified hosts, use zake to fake
        if hosts is None:
            self._zk = fake_client.FakeClient()
        # otherwise use Kazoo
        else:
            self._zk = kazoo_client.KazooClient(hosts=hosts)

    @property
    def zk(self):
        return self._zk

    @property
    def path(self):
        return self._path

    def get_connection(self):
        return ZkConnection(self)

    def close(self):
        self.zk.stop()
        self.zk.close()


class ZkConnection(base.Connection):
    def __init__(self, backend):
        self._backend = backend

        self._log_path = paths.join(self._backend.path, "log_books")
        self._flow_path = paths.join(self._backend.path, "flow_details")
        self._task_path = paths.join(self._backend.path, "task_details")

        with self._exc_wrapper():
            self._backend.zk.start()
            self._backend.zk.ensure_path(self._backend.path)
            self._backend.zk.ensure_path(self.log_path)
            self._backend.zk.ensure_path(self.flow_path)
            self._backend.zk.ensure_path(self.task_path)

    @property
    def backend(self):
        return self._backend

    @property
    def log_path(self):
        return self._log_path

    @property
    def flow_path(self):
        return self._flow_path

    @property
    def task_path(self):
        return self._task_path

    def close(self):
        pass

    def upgrade(self):
        pass

    @contextlib.contextmanager
    def _exc_wrapper(self):
        """Exception wrapper which wraps kazoo exceptions and groups them
        to taskflow exceptions.
        """
        try:
            yield
        except self._backend.zk.handler.timeout_exception as e:
            raise exc.ConnectionFailure("Backend error: %s" % e)
        except k_exc.SessionExpiredError as e:
            raise exc.ConnectionFailure("Backend error: %s" % e)
        except k_exc.NoNodeError as e:
            raise exc.NotFound("Backend error: %s" % e)
        except k_exc.NodeExistsError as e:
            raise exc.AlreadyExists("Backend error: %s" % e)
        except (k_exc.KazooException, k_exc.ZookeeperError) as e:
            raise exc.TaskFlowException("Backend error: %s" % e)

    def update_task_details(self, td):
        """Update a task_detail transactionally.
        """
        with self._exc_wrapper():
            with self.backend.zk.transaction() as txn:
                return self._update_task_details(td, txn)

    def _update_task_details(self, td, txn, create_missing=False):
        zk_path = paths.join(self.task_path, td.uuid)

        # determine whether the desired data exists or not
        try:
            zk_data, _zstat = self.backend.zk.get(zk_path)
        except k_exc.NoNodeError:
        # Not-existent: create or raise exception
            if create_missing:
                txn.create(zk_path)
                e_td = logbook.TaskDetail(name=td.name, uuid=td.uuid)
            else:
                raise exc.NotFound("No task details found with id: %s"
                                   % td.uuid)
        # Existent: read it out
        else:
            e_td = p_utils.unformat_task_detail(td.uuid,
                                                misc.decode_json(zk_data))

        # Update and write it back
        e_td = p_utils.task_details_merge(e_td, td)
        zk_data = misc.binary_encode(jsonutils.dumps(
            p_utils.format_task_detail(e_td)))
        txn.set_data(zk_path, zk_data)
        return e_td

    def get_task_details(self, td_uuid):
        """Read a task_detail. *Read-only*, so no need of zk transaction.
        """
        with self._exc_wrapper():
            return self._get_task_details(td_uuid)

    def _get_task_details(self, td_uuid):
        zk_path = paths.join(self.task_path, td_uuid)

        try:
            zk_data, _zstat = self.backend.zk.get(zk_path)
        except k_exc.NoNodeError:
            raise exc.NotFound("No task details found with id: %s" % td_uuid)

        td = p_utils.unformat_task_detail(td_uuid, misc.decode_json(zk_data))
        return td

    def update_flow_details(self, fd):
        """Update a flow_detail transactionally.
        """
        with self._exc_wrapper():
            with self.backend.zk.transaction() as txn:
                return self._update_flow_details(fd, txn)

    def _update_flow_details(self, fd, txn, create_missing=False):
        zk_path = paths.join(self.flow_path, fd.uuid)

        # determine whether the desired data exists or not
        try:
            zk_data, _zstat = self.backend.zk.get(zk_path)
        except k_exc.NoNodeError:
        # Not-existent: create or raise exception
            if create_missing:
                txn.create(zk_path)
                e_fd = logbook.FlowDetail(name=fd.name, uuid=fd.uuid)
            else:
                raise exc.NotFound("No flow details found with id: %s"
                                   % fd.uuid)
        # Existent: read it out
        else:
            e_fd = p_utils.unformat_flow_detail(fd.uuid,
                                                misc.decode_json(zk_data))

        # Update and write it back
        e_fd = p_utils.flow_details_merge(e_fd, fd)
        zk_data = misc.binary_encode(jsonutils.dumps(
            p_utils.format_flow_detail(e_fd)))
        txn.set_data(zk_path, zk_data)
        for td in fd:
            zk_path = paths.join(self.flow_path, fd.uuid, td.uuid)
            if not self.backend.zk.exists(zk_path):
                txn.create(zk_path)
            e_td = self._update_task_details(td, txn, create_missing=True)
            e_fd.add(e_td)
        return e_fd

    def get_flow_details(self, fd_uuid):
        """Read a flow_detail. *Read-only*, so no need of zk transaction.
        """
        with self._exc_wrapper():
            return self._get_flow_details(fd_uuid)

    def _get_flow_details(self, fd_uuid):
        zk_path = paths.join(self.flow_path, fd_uuid)

        try:
            zk_data, _zstat = self.backend.zk.get(zk_path)
        except k_exc.NoNodeError:
            raise exc.NotFound("No flow details found with id: %s" % fd_uuid)

        fd = p_utils.unformat_flow_detail(fd_uuid, misc.decode_json(zk_data))
        for td_uuid in self.backend.zk.get_children(zk_path):
            td = self._get_task_details(td_uuid)
            fd.add(td)
        return fd

    def save_logbook(self, lb):
        """Save (update) a log_book transactionally.
        """
        with self._exc_wrapper():
            with self.backend.zk.transaction() as txn:
                zk_path = paths.join(self.log_path, lb.uuid)

                # determine whether the desired data exists or not
                try:
                    zk_data, _zstat = self.backend.zk.get(zk_path)
                except k_exc.NoNodeError:
                # Create if a new log_book
                    e_lb = lb
                    zk_data = misc.binary_encode(jsonutils.dumps(
                        p_utils.format_logbook(lb, created_at=None)))
                    txn.create(zk_path, zk_data)
                    for fd in lb:
                        zk_path = paths.join(self.log_path, lb.uuid, fd.uuid)
                        txn.create(zk_path)
                        zk_path = paths.join(self.flow_path, fd.uuid)
                        zk_data = misc.binary_encode(jsonutils.dumps(
                            p_utils.format_flow_detail(fd)))
                        txn.create(zk_path, zk_data)
                        for td in fd:
                            zk_path = paths.join(
                                self.flow_path, fd.uuid, td.uuid)
                            txn.create(zk_path)
                            zk_path = paths.join(self.task_path, td.uuid)
                            zk_data = misc.binary_encode(jsonutils.dumps(
                                p_utils.format_task_detail(td)))
                            txn.create(zk_path, zk_data)

                # Otherwise update the existing log_book
                else:
                    e_lb = p_utils.unformat_logbook(lb.uuid,
                                                    misc.decode_json(zk_data))
                    e_lb = p_utils.logbook_merge(e_lb, lb)
                    zk_data = misc.binary_encode((jsonutils.dumps(
                        p_utils.format_logbook(e_lb,
                                               created_at=lb.created_at))))
                    txn.set_data(zk_path, zk_data)
                    for fd in lb:
                        zk_path = paths.join(self.log_path, lb.uuid, fd.uuid)
                        if not self.backend.zk.exists(zk_path):
                            txn.create(zk_path)
                        e_fd = self._update_flow_details(fd, txn,
                                                         create_missing=True)
                        e_lb.add(e_fd)
                # finally return (updated) log_book
                return e_lb

    def get_logbook(self, lb_uuid):
        """Read a log_book. *Read-only*, so no need of zk transaction.
        """
        with self._exc_wrapper():
            zk_path = paths.join(self.log_path, lb_uuid)

            try:
                zk_data, _zstat = self.backend.zk.get(zk_path)
            except k_exc.NoNodeError:
                raise exc.NotFound("No logbook found with id: %s" % lb_uuid)

            lb = p_utils.unformat_logbook(lb_uuid, misc.decode_json(zk_data))
            for fd_uuid in self.backend.zk.get_children(zk_path):
                fd = self._get_flow_details(fd_uuid)
                lb.add(fd)
            return lb

    def get_logbooks(self):
        """Read all logbooks. *Read-only*, so no need of zk transaction
        """
        with self._exc_wrapper():
            for lb_uuid in self.backend.zk.get_children(self.log_path):
                yield self.get_logbook(lb_uuid)

    def destroy_logbook(self, lb_uuid):
        """Detroy (delete) a log_book transactionally.
        """

        def _destroy_task_details(td_uuid, txn):
            zk_path = paths.join(self.task_path, td_uuid)
            if not self.backend.zk.exists(zk_path):
                raise exc.NotFound("No task details found with id: %s"
                                   % td_uuid)

            txn.delete(zk_path)

        def _destroy_flow_details(fd_uuid, txn):
            zk_path = paths.join(self.flow_path, fd_uuid)
            if not self.backend.zk.exists(zk_path):
                raise exc.NotFound("No flow details found with id: %s"
                                   % fd_uuid)

            for td_uuid in self.backend.zk.get_children(zk_path):
                _destroy_task_details(td_uuid, txn)
                txn.delete(paths.join(zk_path, td_uuid))
            txn.delete(zk_path)

        def _destroy_logbook(lb_uuid, txn):
            zk_path = paths.join(self.log_path, lb_uuid)
            if not self.backend.zk.exists(zk_path):
                raise exc.NotFound("No logbook found with id: %s" % lb_uuid)

            for fd_uuid in self.backend.zk.get_children(zk_path):
                _destroy_flow_details(fd_uuid, txn)
                txn.delete(paths.join(zk_path, fd_uuid))
            txn.delete(zk_path)

        with self._exc_wrapper():
            with self.backend.zk.transaction() as txn:
                _destroy_logbook(lb_uuid, txn)

    def clear_all(self):
        """Delete all data transactioanlly.
        """
        with self._exc_wrapper():
            with self.backend.zk.transaction() as txn:
                # delete all data under log_book path
                for lb_uuid in self.backend.zk.get_children(self.log_path):
                    zk_path = paths.join(self.log_path, lb_uuid)
                    for fd_uuid in self.backend.zk.get_children(zk_path):
                        txn.delete(paths.join(zk_path, fd_uuid))
                    txn.delete(zk_path)
                txn.delete(self.log_path)

                # delete all data under flow_detail path
                for fd_uuid in self.backend.zk.get_children(self.flow_path):
                    zk_path = paths.join(self.flow_path, fd_uuid)
                    for td_uuid in self.backend.zk.get_children(zk_path):
                        txn.delete(paths.join(zk_path, td_uuid))
                    txn.delete(zk_path)
                txn.delete(self.flow_path)

                # delete all data under task_detail path
                for td_uuid in self.backend.zk.get_children(self.task_path):
                    zk_path = paths.join(self.task_path, td_uuid)
                    txn.delete(zk_path)
                txn.delete(self.task_path)

                # delete top-level path
                txn.delete(self.backend.path)
