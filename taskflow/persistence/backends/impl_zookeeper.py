# -*- coding: utf-8 -*-

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

from kazoo import exceptions as k_exc
from kazoo.protocol import paths
from oslo_serialization import jsonutils

from taskflow import exceptions as exc
from taskflow import logging
from taskflow.persistence import base
from taskflow.persistence import logbook
from taskflow.utils import kazoo_utils as k_utils
from taskflow.utils import misc

LOG = logging.getLogger(__name__)

# Transaction support was added in 3.4.0
MIN_ZK_VERSION = (3, 4, 0)


class ZkBackend(base.Backend):
    """A zookeeper backend.

    This backend writes logbooks, flow details, and atom details to a provided
    base path in zookeeper. It will create and store those objects in three
    key directories (one for logbooks, one for flow details and one for atom
    details). It creates those associated directories and then creates files
    inside those directories that represent the contents of those objects for
    later reading and writing.

    Example configuration::

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
        conn = ZkConnection(self, self._client, self._conf)
        if not self._validated:
            conn.validate()
            self._validated = True
        return conn

    def close(self):
        self._validated = False
        if not self._owned:
            return
        try:
            k_utils.finalize_client(self._client)
        except (k_exc.KazooException, k_exc.ZookeeperError) as e:
            raise exc.StorageFailure("Unable to finalize client", e)


class ZkConnection(base.Connection):
    def __init__(self, backend, client, conf):
        self._backend = backend
        self._client = client
        self._conf = conf
        self._book_path = paths.join(self._backend.path, "books")
        self._flow_path = paths.join(self._backend.path, "flow_details")
        self._atom_path = paths.join(self._backend.path, "atom_details")
        with self._exc_wrapper():
            # NOOP if already started.
            self._client.start()

    def validate(self):
        with self._exc_wrapper():
            try:
                if self._conf.get('check_compatible', True):
                    k_utils.check_compatible(self._client, MIN_ZK_VERSION)
            except exc.IncompatibleVersion as e:
                raise exc.StorageFailure("Backend storage is not a"
                                         " compatible version", e)

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
    def atom_path(self):
        return self._atom_path

    def close(self):
        pass

    def upgrade(self):
        """Creates the initial paths (if they already don't exist)."""
        with self._exc_wrapper():
            for path in (self.book_path, self.flow_path, self.atom_path):
                self._client.ensure_path(path)

    @contextlib.contextmanager
    def _exc_wrapper(self):
        """Exception context-manager which wraps kazoo exceptions.

        This is used to capture and wrap any kazoo specific exceptions and
        then group them into corresponding taskflow exceptions (not doing
        that would expose the underlying kazoo exception model).
        """
        try:
            yield
        except self._client.handler.timeout_exception as e:
            raise exc.StorageFailure("Storage backend timeout", e)
        except k_exc.SessionExpiredError as e:
            raise exc.StorageFailure("Storage backend session"
                                     " has expired", e)
        except k_exc.NoNodeError as e:
            raise exc.NotFound("Storage backend node not found: %s" % e)
        except k_exc.NodeExistsError as e:
            raise exc.Duplicate("Storage backend duplicate node: %s" % e)
        except (k_exc.KazooException, k_exc.ZookeeperError) as e:
            raise exc.StorageFailure("Storage backend internal error", e)

    def update_atom_details(self, ad):
        """Update a atom detail transactionally."""
        with self._exc_wrapper():
            txn = self._client.transaction()
            ad = self._update_atom_details(ad, txn)
            k_utils.checked_commit(txn)
            return ad

    def _update_atom_details(self, ad, txn, create_missing=False):
        # Determine whether the desired data exists or not.
        ad_path = paths.join(self.atom_path, ad.uuid)
        e_ad = None
        try:
            ad_data, _zstat = self._client.get(ad_path)
        except k_exc.NoNodeError:
            # Not-existent: create or raise exception.
            if not create_missing:
                raise exc.NotFound("No atom details found with"
                                   " id: %s" % ad.uuid)
            else:
                txn.create(ad_path)
        else:
            # Existent: read it out.
            try:
                ad_data = misc.decode_json(ad_data)
                ad_cls = logbook.atom_detail_class(ad_data['type'])
                e_ad = ad_cls.from_dict(ad_data['atom'])
            except KeyError:
                pass

        # Update and write it back
        if e_ad:
            e_ad = e_ad.merge(ad)
        else:
            e_ad = ad
        ad_data = base._format_atom(e_ad)
        txn.set_data(ad_path,
                     misc.binary_encode(jsonutils.dumps(ad_data)))
        return e_ad

    def get_atom_details(self, ad_uuid):
        """Read a atom detail.

        *Read-only*, so no need of zk transaction.
        """
        with self._exc_wrapper():
            return self._get_atom_details(ad_uuid)

    def _get_atom_details(self, ad_uuid):
        ad_path = paths.join(self.atom_path, ad_uuid)
        try:
            ad_data, _zstat = self._client.get(ad_path)
        except k_exc.NoNodeError:
            raise exc.NotFound("No atom details found with id: %s" % ad_uuid)
        else:
            ad_data = misc.decode_json(ad_data)
            ad_cls = logbook.atom_detail_class(ad_data['type'])
            return ad_cls.from_dict(ad_data['atom'])

    def update_flow_details(self, fd):
        """Update a flow detail transactionally."""
        with self._exc_wrapper():
            txn = self._client.transaction()
            fd = self._update_flow_details(fd, txn)
            k_utils.checked_commit(txn)
            return fd

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
            e_fd = logbook.FlowDetail.from_dict(misc.decode_json(fd_data))

        # Update and write it back
        e_fd = e_fd.merge(fd)
        fd_data = e_fd.to_dict()
        txn.set_data(fd_path, misc.binary_encode(jsonutils.dumps(fd_data)))
        for ad in fd:
            ad_path = paths.join(fd_path, ad.uuid)
            # NOTE(harlowja): create an entry in the flow detail path
            # for the provided atom detail so that a reference exists
            # from the flow detail to its atom details.
            if not self._client.exists(ad_path):
                txn.create(ad_path)
            e_fd.add(self._update_atom_details(ad, txn, create_missing=True))
        return e_fd

    def get_flow_details(self, fd_uuid):
        """Read a flow detail.

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

        fd = logbook.FlowDetail.from_dict(misc.decode_json(fd_data))
        for ad_uuid in self._client.get_children(fd_path):
            fd.add(self._get_atom_details(ad_uuid))
        return fd

    def save_logbook(self, lb):
        """Save (update) a log_book transactionally."""

        def _create_logbook(lb_path, txn):
            lb_data = lb.to_dict(marshal_time=True)
            txn.create(lb_path, misc.binary_encode(jsonutils.dumps(lb_data)))
            for fd in lb:
                # NOTE(harlowja): create an entry in the logbook path
                # for the provided flow detail so that a reference exists
                # from the logbook to its flow details.
                txn.create(paths.join(lb_path, fd.uuid))
                fd_path = paths.join(self.flow_path, fd.uuid)
                fd_data = jsonutils.dumps(fd.to_dict())
                txn.create(fd_path, misc.binary_encode(fd_data))
                for ad in fd:
                    # NOTE(harlowja): create an entry in the flow detail path
                    # for the provided atom detail so that a reference exists
                    # from the flow detail to its atom details.
                    txn.create(paths.join(fd_path, ad.uuid))
                    ad_path = paths.join(self.atom_path, ad.uuid)
                    ad_data = base._format_atom(ad)
                    txn.create(ad_path,
                               misc.binary_encode(jsonutils.dumps(ad_data)))
            return lb

        def _update_logbook(lb_path, lb_data, txn):
            e_lb = logbook.LogBook.from_dict(misc.decode_json(lb_data),
                                             unmarshal_time=True)
            e_lb = e_lb.merge(lb)
            lb_data = e_lb.to_dict(marshal_time=True)
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
            txn = self._client.transaction()
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
            k_utils.checked_commit(txn)
            return e_lb

    def _get_logbook(self, lb_uuid):
        lb_path = paths.join(self.book_path, lb_uuid)
        try:
            lb_data, _zstat = self._client.get(lb_path)
        except k_exc.NoNodeError:
            raise exc.NotFound("No logbook found with id: %s" % lb_uuid)
        else:
            lb = logbook.LogBook.from_dict(misc.decode_json(lb_data),
                                           unmarshal_time=True)
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
        """Destroy (delete) a log_book transactionally."""

        def _destroy_atom_details(ad_uuid, txn):
            ad_path = paths.join(self.atom_path, ad_uuid)
            if not self._client.exists(ad_path):
                raise exc.NotFound("No atom details found with id: %s"
                                   % ad_uuid)
            txn.delete(ad_path)

        def _destroy_flow_details(fd_uuid, txn):
            fd_path = paths.join(self.flow_path, fd_uuid)
            if not self._client.exists(fd_path):
                raise exc.NotFound("No flow details found with id: %s"
                                   % fd_uuid)
            for ad_uuid in self._client.get_children(fd_path):
                _destroy_atom_details(ad_uuid, txn)
                txn.delete(paths.join(fd_path, ad_uuid))
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
            txn = self._client.transaction()
            _destroy_logbook(lb_uuid, txn)
            k_utils.checked_commit(txn)

    def clear_all(self, delete_dirs=True):
        """Delete all data transactionally."""
        with self._exc_wrapper():
            txn = self._client.transaction()

            # Delete all data under logbook path.
            for lb_uuid in self._client.get_children(self.book_path):
                lb_path = paths.join(self.book_path, lb_uuid)
                for fd_uuid in self._client.get_children(lb_path):
                    txn.delete(paths.join(lb_path, fd_uuid))
                txn.delete(lb_path)

            # Delete all data under flow detail path.
            for fd_uuid in self._client.get_children(self.flow_path):
                fd_path = paths.join(self.flow_path, fd_uuid)
                for ad_uuid in self._client.get_children(fd_path):
                    txn.delete(paths.join(fd_path, ad_uuid))
                txn.delete(fd_path)

            # Delete all data under atom detail path.
            for ad_uuid in self._client.get_children(self.atom_path):
                ad_path = paths.join(self.atom_path, ad_uuid)
                txn.delete(ad_path)

            # Delete containing directories.
            if delete_dirs:
                txn.delete(self.book_path)
                txn.delete(self.atom_path)
                txn.delete(self.flow_path)

            k_utils.checked_commit(txn)
