# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

from kazoo import client
import six

from taskflow import exceptions as exc


def _parse_hosts(hosts):
    if isinstance(hosts, six.string_types):
        return hosts.strip()
    if isinstance(hosts, (dict)):
        host_ports = []
        for (k, v) in six.iteritems(hosts):
            host_ports.append("%s:%s" % (k, v))
        hosts = host_ports
    if isinstance(hosts, (list, set, tuple)):
        return ",".join([str(h) for h in hosts])
    return hosts


def finalize_client(client):
    """Stops and closes a client, even if it wasn't started."""
    client.stop()
    try:
        client.close()
    except TypeError:
        # NOTE(harlowja): https://github.com/python-zk/kazoo/issues/167
        #
        # This can be removed after that one is fixed/merged.
        pass


def check_compatible(client, min_version=None, max_version=None):
    """Checks if a kazoo client is backed by a zookeeper server version
    that satisfies a given min (inclusive) and max (inclusive) version range.
    """
    server_version = None
    if min_version:
        server_version = tuple((int(a) for a in client.server_version()))
        min_version = tuple((int(a) for a in min_version))
        if server_version < min_version:
            pretty_server_version = ".".join([str(a) for a in server_version])
            min_version = ".".join([str(a) for a in min_version])
            raise exc.IncompatibleVersion("Incompatible zookeeper version"
                                          " %s detected, zookeeper >= %s"
                                          " required" % (pretty_server_version,
                                                         min_version))
    if max_version:
        if server_version is None:
            server_version = tuple((int(a) for a in client.server_version()))
        max_version = tuple((int(a) for a in max_version))
        if server_version > max_version:
            pretty_server_version = ".".join([str(a) for a in server_version])
            max_version = ".".join([str(a) for a in max_version])
            raise exc.IncompatibleVersion("Incompatible zookeeper version"
                                          " %s detected, zookeeper <= %s"
                                          " required" % (pretty_server_version,
                                                         max_version))


def make_client(conf):
    """Creates a kazoo client given a configuration dictionary."""
    # See: http://kazoo.readthedocs.org/en/latest/api/client.html
    client_kwargs = {
        'read_only': bool(conf.get('read_only')),
        'randomize_hosts': bool(conf.get('randomize_hosts')),
    }
    # See: http://kazoo.readthedocs.org/en/latest/api/retry.html
    if 'command_retry' in conf:
        client_kwargs['command_retry'] = conf['command_retry']
    if 'connection_retry' in conf:
        client_kwargs['connection_retry'] = conf['connection_retry']
    hosts = _parse_hosts(conf.get("hosts", "localhost:2181"))
    if not hosts or not isinstance(hosts, six.string_types):
        raise TypeError("Invalid hosts format, expected "
                        "non-empty string/list, not %s" % type(hosts))
    client_kwargs['hosts'] = hosts
    if 'timeout' in conf:
        client_kwargs['timeout'] = float(conf['timeout'])
    # Kazoo supports various handlers, gevent, threading, eventlet...
    # allow the user of this client object to optionally specify one to be
    # used.
    if 'handler' in conf:
        client_kwargs['handler'] = conf['handler']
    return client.KazooClient(**client_kwargs)
