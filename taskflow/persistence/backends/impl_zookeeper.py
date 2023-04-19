# -*- coding: utf-8 -*-

#    Copyright (C) 2014 AT&T Labs All Rights Reserved.
#    Copyright (C) 2015 Rackspace Hosting All Rights Reserved.
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
from oslo_utils import strutils

from taskflow import exceptions as exc
from taskflow.persistence import path_based
from taskflow.utils import kazoo_utils as k_utils
from taskflow.utils import misc


MIN_ZK_VERSION = (3, 4, 0)


class ZkBackend(path_based.PathBasedBackend):
    """A zookeeper-backed backend.

    Example configuration::

        conf = {
            "hosts": "192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181",
            "path": "/taskflow",
        }

    Do note that the creation of a kazoo client is achieved
    by :py:func:`~taskflow.utils.kazoo_utils.make_client` and the transfer
    of this backend configuration to that function to make a
    client may happen at ``__init__`` time. This implies that certain
    parameters from this backend configuration may be provided to
    :py:func:`~taskflow.utils.kazoo_utils.make_client` such
    that if a client was not provided by the caller one will be created
    according to :py:func:`~taskflow.utils.kazoo_utils.make_client`'s
    specification
    """

    #: Default path used when none is provided.
    DEFAULT_PATH = '/taskflow'

    def __init__(self, conf, client=None):
        super(ZkBackend, self).__init__(conf)
        if not paths.isabs(self._path):
            raise ValueError("Zookeeper path must be absolute")
        if client is not None:
            self._client = client
            self._owned = False
        else:
            self._client = k_utils.make_client(self._conf)
            self._owned = True
        self._validated = False

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
        except (k_exc.KazooException, k_exc.ZookeeperError):
            exc.raise_with_cause(exc.StorageFailure,
                                 "Unable to finalize client")


class ZkConnection(path_based.PathBasedConnection):
    def __init__(self, backend, client, conf):
        super(ZkConnection, self).__init__(backend)
        self._conf = conf
        self._client = client
        with self._exc_wrapper():
            # NOOP if already started.
            self._client.start()

    @contextlib.contextmanager
    def _exc_wrapper(self):
        """Exception context-manager which wraps kazoo exceptions.

        This is used to capture and wrap any kazoo specific exceptions and
        then group them into corresponding taskflow exceptions (not doing
        that would expose the underlying kazoo exception model).
        """
        try:
            yield
        except self._client.handler.timeout_exception:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Storage backend timeout")
        except k_exc.SessionExpiredError:
            exc.raise_with_cause(exc.StorageFailure,
                                 "Storage backend session has expired")
        except k_exc.NoNodeError:
            exc.raise_with_cause(exc.NotFound,
                                 "Storage backend node not found")
        except k_exc.NodeExistsError:
            exc.raise_with_cause(exc.Duplicate,
                                 "Storage backend duplicate node")
        except (k_exc.KazooException, k_exc.ZookeeperError):
            exc.raise_with_cause(exc.StorageFailure,
                                 "Storage backend internal error")

    def _join_path(self, *parts):
        return paths.join(*parts)

    def _get_item(self, path):
        with self._exc_wrapper():
            data, _ = self._client.get(path)
        return misc.decode_json(data)

    def _set_item(self, path, value, transaction):
        data = misc.binary_encode(jsonutils.dumps(value))
        if not self._client.exists(path):
            transaction.create(path, data)
        else:
            transaction.set_data(path, data)

    def _del_tree(self, path, transaction):
        for child in self._get_children(path):
            self._del_tree(self._join_path(path, child), transaction)
        transaction.delete(path)

    def _get_children(self, path):
        with self._exc_wrapper():
            return self._client.get_children(path)

    def _ensure_path(self, path):
        with self._exc_wrapper():
            self._client.ensure_path(path)

    def _create_link(self, src_path, dest_path, transaction):
        if not self._client.exists(dest_path):
            transaction.create(dest_path)

    @contextlib.contextmanager
    def _transaction(self):
        transaction = self._client.transaction()
        with self._exc_wrapper():
            yield transaction
            k_utils.checked_commit(transaction)

    def validate(self):
        with self._exc_wrapper():
            try:
                if strutils.bool_from_string(
                        self._conf.get('check_compatible'), default=True):
                    k_utils.check_compatible(self._client, MIN_ZK_VERSION)
            except exc.IncompatibleVersion:
                exc.raise_with_cause(exc.StorageFailure, "Backend storage is"
                                     " not a compatible version")
