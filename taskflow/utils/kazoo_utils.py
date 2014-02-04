# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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


def make_client(conf):
    """Creates a kazoo client given a configuration dictionary."""
    client_kwargs = {
        'read_only': bool(conf.get('read_only')),
        'randomize_hosts': bool(conf.get('randomize_hosts')),
    }
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
