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

from nova.openstack.common import jsonutils
from nova.workflow.logbook import api

from oslo.config import cfg


CONF = cfg.CONF
CONF.import_opt('address', 'nova.servicegroup.zk', group='zookeeper')
CONF.import_opt('recv_timeout', 'nova.servicegroup.zk', group='zookeeper')


class LogBook(api.LogBook):
    prefix = "entry-"

    def __init__(self, client, resource_uri):
        super(LogBook, self).__init__(self, "%s/logbook" % (resource_uri))
        self._client = client
        self._paths_made = False
        self._paths = [self.uri]

    def _ensure_paths(self):
        if self._paths_made:
            return
        for p in self._paths:
            self._client.ensure_path(p)
        self._paths_made = True

    def add_record(self, name, metadata=None):
        self._ensure_paths()
        (path, value) = self._make_storage(name, metadata)
        self._client.create(path, jsonutils.dumps(value), sequence=True)

    def _make_storage(self, name, metadata):
        path = "{root}/{prefix}".format(root=self.uri, prefix=self.prefix)
        value = {
            'name': name,
            'metadata': metadata,
        }
        return (path, value)

    def __iter__(self):
        for c in self._client.get_children(self.uri + "/"):
            if not c.startswith(self.prefix):
                continue
            (value, _zk) = self._client.get("%s/%s" % (self.uri, c))
            value = jsonutils.loads(value)
            yield (value['name'], value['metadata'])

    def __contains__(self, name):
        for (n, metadata) in self:
            if name == n:
                return True
        return False

    def mark(self, name, metadata, merge_func=None):
        if merge_func is None:
            merge_func = lambda old,new : new
        for c in self._client.get_children(self.uri + "/"):
           if not c.startswith(self.prefix):
               continue
           (value, _zk) = self._client.get("%s/%s" % (self.uri, c))
           value = jsonutils.loads(value)
           if value['name'] == name:
               value['metadata'] = merge_func(value['metadata'], metadata)
               self._client.set("%s/%s" % (self.uri, c),
                                jsonutils.dumps(value))
               return
        raise api.RecordNotFound()

    def fetch_record(self, name):
        for n, metadata in self:
            if name == n:
                return metadata
        raise api.RecordNotFound()


class LogBookProvider(api.LogBookProvider):
    def __init__(self, *args, **kwargs):
        self._client = kazoo_client.KazooClient(hosts=CONF.address,
                                                timeout=CONF.recv_timeout)
        self._client.start()

    def close(self):
        if self._client:
            self._client.stop()

    def provide(self, resource_uri):
        return LogBook(self._client, resource_uri)
