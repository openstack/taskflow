# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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

from kazoo import client as kazoo_client
from kazoo.recipe import lock as kazoo_lock

from nova.workflow.locks import api

from oslo.config import cfg


CONF = cfg.CONF
CONF.import_opt('address', 'nova.servicegroup.zk', group='zookeeper')
CONF.import_opt('recv_timeout', 'nova.servicegroup.zk', group='zookeeper')


class Lock(api.Lock):
    def __init__(self, resource_uri, blocking, client):
        super(Lock, self).__init__(resource_uri, blocking)
        self._client = client
        self._lock = kazoo_lock.Lock(client, resource_uri)

    def acquire(self):
        return self._lock.acquire(self._blocking)

    def is_locked(self):
        return self._lock.is_acquired

    def release(self):
        return self._lock.release()

    def cancel(self):
        return self._lock.cancel()


class LockProvider(api.LockProvider):
    def __init__(self, *args, **kwargs):
        self._client = kazoo_client.KazooClient(hosts=CONF.address,
                                                timeout=CONF.recv_timeout)
        self._client.start()

    def provide(self, resource_uri, blocking=True):
        return Lock(self._client, resource_uri, blocking)

    def close(self):
        if self._client:
            self._client.stop()
