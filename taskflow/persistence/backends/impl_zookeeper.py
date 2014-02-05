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

from kazoo import exceptions as k_exc
from kazoo.protocol import paths

from taskflow import exceptions as exc
from taskflow.openstack.common import jsonutils
from taskflow.persistence.backends import base
from taskflow.persistence import logbook
from taskflow.utils import kazoo_utils as k_utils
from taskflow.utils import misc
from taskflow.utils import persistence_utils as p_utils

LOG = logging.getLogger(__name__)

# Transaction support was added in 3.4.0
MIN_ZK_VERSION = (3, 4, 0)


class ZkBackend(base.Backend):
    """ZooKeeper as backend storage implementation

    Example conf (use Kazoo):

    conf = {
        "hosts": "192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181",
        "path": "/taskflow",
    }
    """
    def __init__(self, conf, client=None):
        super(ZkBackend, self).__init__(conf)
        path = str(conf.get("path", "/taskflow"))
        if not path:
            raise ValueError("Empty zookeeper path is disallowed")
        if not paths.isabs(path):
            raise ValueError("Zookeeper path must be absolute")
        self._path = path
        if client is not None:
            self._client = client
            self._owned = False
        else:
            self._client = k_utils.make_client(conf)
            self._owned = True
        self._validated = False

    @property
    def path(self):
        return self._path

    def get_connection(self):
        conn = ZkConnection(self, self._client)
        if not self._validated:
            conn.validate()
            self._validated = True
        return conn

    def close(self):
        self._validated = False
        if not self._owned:
            return
        try:
            self._client.stop()
        except (k_exc.KazooException, k_exc.ZookeeperError) as e:
            raise exc.StorageError("Unable to stop client: %s" % e)
        try:
            self._client.close()
        except TypeError:
            # NOTE(harlowja): https://github.com/python-zk/kazoo/issues/167
            pass
        except (k_exc.KazooException, k_exc.ZookeeperError) as e:
            raise exc.StorageError("Unable to close client: %s" % e)


class ZkConnection(base.Connection):
    def __init__(self, backend, client):
        self._backend = backend
        self._client = client
        self._book_path = paths.join(self._backend.path, "books")
        self._flow_path = paths.join(self._backend.path, "flow_details")
        self._task_path = paths.join(self._backend.path, "task_details")
        with self._exc_wrapper():
            # NOOP if already started.
            self._client.start()

    def validate(self):
        with self._exc_wrapper():
            zk_ver = self._client.server_version()
            if tuple(zk_ver) < MIN_ZK_VERSION:
                given_zk_ver = ".".join([str(a) for a in zk_ver])
                desired_zk_ver = ".".join([str(a) for a in MIN_ZK_VERSION])
                raise exc.StorageError("Incompatible zookeeper version"
                                       " %s detected, zookeeper >= %s required"
                                       % (given_zk_ver, desired_zk_ver))

    @property
    def backend(self):
        return self._backend

    @property
    def book_path(self):
        return self._book_path

    @property
    def flow_path(self):
        return self._flow_path

    @property
    def task_path(self):
        return self._task_path

    def close(self):
        pass

    def upgrade(self):
        """Creates the initial paths (if they already don't exist)."""
        with self._exc_wrapper():
            for path in (self.book_path, self.flow_path, self.task_path):
                self._client.ensure_path(path)

    @contextlib.contextmanager
    def _exc_wrapper(self):
        """Exception wrapper which wraps kazoo exceptions and groups them
        to taskflow exceptions.
        """
        try:
            yield
        except self._client.handler.timeout_exception as e:
            raise exc.ConnectionFailure("Storage backend timeout: %s" % e)
        except k_exc.SessionExpiredError as e:
            raise exc.ConnectionFailure("Storage backend session"
                                        " has expired: %s" % e)
        except k_exc.NoNodeError as e:
            raise exc.NotFound("Storage backend node not found: %s" % e)
        except k_exc.NodeExistsError as e:
            raise exc.AlreadyExists("Storage backend duplicate node: %s" % e)
        except (k_exc.KazooException, k_exc.ZookeeperError) as e:
            raise exc.StorageError("Storage backend internal error: %s" % e)

    def update_task_details(self, td):
        """Update a task_detail transactionally."""
        with self._exc_wrapper():
            with self._client.transaction() as txn:
                return self._update_task_details(td, txn)

    def _update_task_details(self, td, txn, create_missing=False):
        # Determine whether the desired data exists or not.
        td_path = paths.join(self.task_path, td.uuid)
        try:
            td_data, _zstat = self._client.get(td_path)
        except k_exc.NoNodeError:
            # Not-existent: create or raise exception.
            if create_missing:
                txn.create(td_path)
                e_td = logbook.TaskDetail(name=td.name, uuid=td.uuid)
            else:
                raise exc.NotFound("No task details found with id: %s"
                                   % td.uuid)
        else:
            # Existent: read it out.
            e_td = p_utils.unformat_task_detail(td.uuid,
                                                misc.decode_json(td_data))

        # Update and write it back
        e_td = p_utils.task_details_merge(e_td, td)
        td_data = p_utils.format_task_detail(e_td)
        txn.set_data(td_path, misc.binary_encode(jsonutils.dumps(td_data)))
        return e_td

    def get_task_details(self, td_uuid):
        """Read a taskdetail.

        *Read-only*, so no need of zk transaction.
        """
        with self._exc_wrapper():
            return self._get_task_details(td_uuid)

    def _get_task_details(self, td_uuid):
        td_path = paths.join(self.task_path, td_uuid)
        try:
            td_data, _zstat = self._client.get(td_path)
        except k_exc.NoNodeError:
            raise exc.NotFound("No task details found with id: %s" % td_uuid)
        else:
            return p_utils.unformat_task_detail(td_uuid,
                                                misc.decode_json(td_data))

    def update_flow_details(self, fd):
        """Update a flowdetail transactionally."""
        with self._exc_wrapper():
            with self._client.transaction() as txn:
                return self._update_flow_details(fd, txn)

    def _update_flow_details(self, fd, txn, create_missing=False):
        # Determine whether the desired data exists or not
        fd_path = paths.join(self.flow_path, fd.uuid)
        try:
            fd_data, _zstat = self._client.get(fd_path)
        except k_exc.NoNodeError:
            # Not-existent: create or raise exception
            if create_missing:
                txn.create(fd_path)
                e_fd = logbook.FlowDetail(name=fd.name, uuid=fd.uuid)
            else:
                raise exc.NotFound("No flow details found with id: %s"
                                   % fd.uuid)
        else:
            # Existent: read it out
            e_fd = p_utils.unformat_flow_detail(fd.uuid,
                                                misc.decode_json(fd_data))

        # Update and write it back
        e_fd = p_utils.flow_details_merge(e_fd, fd)
        fd_data = p_utils.format_flow_detail(e_fd)
        txn.set_data(fd_path, misc.binary_encode(jsonutils.dumps(fd_data)))
        for td in fd:
            td_path = paths.join(fd_path, td.uuid)
            # NOTE(harlowja): create an entry in the flow detail path
            # for the provided task detail so that a reference exists
            # from the flow detail to its task details.
            if not self._client.exists(td_path):
                txn.create(td_path)
            e_fd.add(self._update_task_details(td, txn, create_missing=True))
        return e_fd

    def get_flow_details(self, fd_uuid):
        """Read a flowdetail.

        *Read-only*, so no need of zk transaction.
        """
        with self._exc_wrapper():
            return self._get_flow_details(fd_uuid)

    def _get_flow_details(self, fd_uuid):
        fd_path = paths.join(self.flow_path, fd_uuid)
        try:
            fd_data, _zstat = self._client.get(fd_path)
        except k_exc.NoNodeError:
            raise exc.NotFound("No flow details found with id: %s" % fd_uuid)

        fd = p_utils.unformat_flow_detail(fd_uuid, misc.decode_json(fd_data))
        for td_uuid in self._client.get_children(fd_path):
            fd.add(self._get_task_details(td_uuid))
        return fd

    def save_logbook(self, lb):
        """Save (update) a log_book transactionally."""

        def _create_logbook(lb_path, txn):
            lb_data = p_utils.format_logbook(lb, created_at=None)
            txn.create(lb_path, misc.binary_encode(jsonutils.dumps(lb_data)))
            for fd in lb:
                # NOTE(harlowja): create an entry in the logbook path
                # for the provided flow detail so that a reference exists
                # from the logbook to its flow details.
                txn.create(paths.join(lb_path, fd.uuid))
                fd_path = paths.join(self.flow_path, fd.uuid)
                fd_data = jsonutils.dumps(p_utils.format_flow_detail(fd))
                txn.create(fd_path, misc.binary_encode(fd_data))
                for td in fd:
                    # NOTE(harlowja): create an entry in the flow detail path
                    # for the provided task detail so that a reference exists
                    # from the flow detail to its task details.
                    txn.create(paths.join(fd_path, td.uuid))
                    td_path = paths.join(self.task_path, td.uuid)
                    td_data = jsonutils.dumps(p_utils.format_task_detail(td))
                    txn.create(td_path, misc.binary_encode(td_data))
            return lb

        def _update_logbook(lb_path, lb_data, txn):
            e_lb = p_utils.unformat_logbook(lb.uuid, misc.decode_json(lb_data))
            e_lb = p_utils.logbook_merge(e_lb, lb)
            lb_data = p_utils.format_logbook(e_lb, created_at=lb.created_at)
            txn.set_data(lb_path, misc.binary_encode(jsonutils.dumps(lb_data)))
            for fd in lb:
                fd_path = paths.join(lb_path, fd.uuid)
                if not self._client.exists(fd_path):
                    # NOTE(harlowja): create an entry in the logbook path
                    # for the provided flow detail so that a reference exists
                    # from the logbook to its flow details.
                    txn.create(fd_path)
                e_fd = self._update_flow_details(fd, txn, create_missing=True)
                e_lb.add(e_fd)
            return e_lb

        with self._exc_wrapper():
            with self._client.transaction() as txn:
                # Determine whether the desired data exists or not.
                lb_path = paths.join(self.book_path, lb.uuid)
                try:
                    lb_data, _zstat = self._client.get(lb_path)
                except k_exc.NoNodeError:
                    # Create a new logbook since it doesn't exist.
                    e_lb = _create_logbook(lb_path, txn)
                else:
                    # Otherwise update the existing logbook instead.
                    e_lb = _update_logbook(lb_path, lb_data, txn)
                # Finally return (updated) logbook.
                return e_lb

    def _get_logbook(self, lb_uuid):
        lb_path = paths.join(self.book_path, lb_uuid)
        try:
            lb_data, _zstat = self._client.get(lb_path)
        except k_exc.NoNodeError:
            raise exc.NotFound("No logbook found with id: %s" % lb_uuid)
        else:
            lb = p_utils.unformat_logbook(lb_uuid,
                                          misc.decode_json(lb_data))
            for fd_uuid in self._client.get_children(lb_path):
                lb.add(self._get_flow_details(fd_uuid))
            return lb

    def get_logbook(self, lb_uuid):
        """Read a logbook.

        *Read-only*, so no need of zk transaction.
        """
        with self._exc_wrapper():
            return self._get_logbook(lb_uuid)

    def get_logbooks(self):
        """Read all logbooks.

        *Read-only*, so no need of zk transaction.
        """
        with self._exc_wrapper():
            for lb_uuid in self._client.get_children(self.book_path):
                yield self._get_logbook(lb_uuid)

    def destroy_logbook(self, lb_uuid):
        """Detroy (delete) a log_book transactionally."""

        def _destroy_task_details(td_uuid, txn):
            td_path = paths.join(self.task_path, td_uuid)
            if not self._client.exists(td_path):
                raise exc.NotFound("No task details found with id: %s"
                                   % td_uuid)
            txn.delete(td_path)

        def _destroy_flow_details(fd_uuid, txn):
            fd_path = paths.join(self.flow_path, fd_uuid)
            if not self._client.exists(fd_path):
                raise exc.NotFound("No flow details found with id: %s"
                                   % fd_uuid)
            for td_uuid in self._client.get_children(fd_path):
                _destroy_task_details(td_uuid, txn)
                txn.delete(paths.join(fd_path, td_uuid))
            txn.delete(fd_path)

        def _destroy_logbook(lb_uuid, txn):
            lb_path = paths.join(self.book_path, lb_uuid)
            if not self._client.exists(lb_path):
                raise exc.NotFound("No logbook found with id: %s" % lb_uuid)
            for fd_uuid in self._client.get_children(lb_path):
                _destroy_flow_details(fd_uuid, txn)
                txn.delete(paths.join(lb_path, fd_uuid))
            txn.delete(lb_path)

        with self._exc_wrapper():
            with self._client.transaction() as txn:
                _destroy_logbook(lb_uuid, txn)

    def clear_all(self, delete_dirs=True):
        """Delete all data transactioanlly."""
        with self._exc_wrapper():
            with self._client.transaction() as txn:

                # Delete all data under logbook path.
                for lb_uuid in self._client.get_children(self.book_path):
                    lb_path = paths.join(self.book_path, lb_uuid)
                    for fd_uuid in self._client.get_children(lb_path):
                        txn.delete(paths.join(lb_path, fd_uuid))
                    txn.delete(lb_path)

                # Delete all data under flowdetail path.
                for fd_uuid in self._client.get_children(self.flow_path):
                    fd_path = paths.join(self.flow_path, fd_uuid)
                    for td_uuid in self._client.get_children(fd_path):
                        txn.delete(paths.join(fd_path, td_uuid))
                    txn.delete(fd_path)

                # Delete all data under taskdetail path.
                for td_uuid in self._client.get_children(self.task_path):
                    td_path = paths.join(self.task_path, td_uuid)
                    txn.delete(td_path)

                # Delete containing directories.
                if delete_dirs:
                    txn.delete(self.book_path)
                    txn.delete(self.task_path)
                    txn.delete(self.flow_path)
