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
from kazoo import exceptions as k_exc
from oslo_utils import reflection
import six
from six.moves import zip as compat_zip

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


def prettify_failures(failures, limit=-1):
    """Prettifies a checked commits failures (ignores sensitive data...)."""
    prettier = []
    for (op, r) in failures:
        pretty_op = reflection.get_class_name(op, fully_qualified=False)
        # Pick off a few attributes that are meaningful (but one that don't
        # show actual data, which might not be desired to show...).
        selected_attrs = [
            "path=%r" % op.path,
        ]
        try:
            if op.version != -1:
                selected_attrs.append("version=%s" % op.version)
        except AttributeError:
            pass
        pretty_op += "(%s)" % (", ".join(selected_attrs))
        pretty_cause = reflection.get_class_name(r, fully_qualified=False)
        prettier.append("%s@%s" % (pretty_cause, pretty_op))
    if limit <= 0 or len(prettier) <= limit:
        return ", ".join(prettier)
    else:
        leftover = prettier[limit:]
        prettier = prettier[0:limit]
        return ", ".join(prettier) + " and %s more..." % len(leftover)


class KazooTransactionException(k_exc.KazooException):
    """Exception raised when a checked commit fails."""

    def __init__(self, message, failures):
        super(KazooTransactionException, self).__init__(message)
        self._failures = tuple(failures)

    @property
    def failures(self):
        return self._failures


def checked_commit(txn):
    """Commits a kazoo transcation and validates the result.

    NOTE(harlowja): Until https://github.com/python-zk/kazoo/pull/224 is fixed
    or a similar pull request is merged we have to workaround the transaction
    failing silently.
    """
    if not txn.operations:
        return []
    results = txn.commit()
    failures = []
    for op, result in compat_zip(txn.operations, results):
        if isinstance(result, k_exc.KazooException):
            failures.append((op, result))
    if len(results) < len(txn.operations):
        raise KazooTransactionException(
            "Transaction returned %s results, this is less than"
            " the number of expected transaction operations %s"
            % (len(results), len(txn.operations)), failures)
    if len(results) > len(txn.operations):
        raise KazooTransactionException(
            "Transaction returned %s results, this is greater than"
            " the number of expected transaction operations %s"
            % (len(results), len(txn.operations)), failures)
    if failures:
        raise KazooTransactionException(
            "Transaction with %s operations failed: %s"
            % (len(txn.operations),
               prettify_failures(failures, limit=1)), failures)
    return results


def finalize_client(client):
    """Stops and closes a client, even if it wasn't started."""
    client.stop()
    client.close()


def check_compatible(client, min_version=None, max_version=None):
    """Checks if a kazoo client is backed by a zookeeper server version.

    This check will verify that the zookeeper server version that the client
    is connected to satisfies a given minimum version (inclusive) and
    maximum (inclusive) version range. If the server is not in the provided
    version range then a exception is raised indiciating this.
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
    """Creates a `kazoo`_ `client`_ given a configuration dictionary.

    :param conf: configuration dictionary that will be used to configure
                 the created client
    :type conf: dict

    The keys that will be extracted are:

    - ``read_only``: boolean that specifies whether to allow connections to
      read only servers, defaults to ``False``
    - ``randomize_hosts``: boolean that specifies whether to randomize
      host lists provided, defaults to ``False``
    - ``command_retry``: a kazoo `retry`_ object (or dict of options which
      will be used for creating one) that will be used for retrying commands
      that are executed
    - ``connection_retry``: a kazoo `retry`_ object (or dict of options which
      will be used for creating one)  that will be used for retrying
      connection failures that occur
    - ``hosts``: a string, list, set (or dict with host keys) that will
      specify the hosts the kazoo client should be connected to, if none
      is provided then ``localhost:2181`` will be used by default
    - ``timeout``: a float value that specifies the default timeout that the
      kazoo client will use
    - ``handler``: a kazoo handler object that can be used to provide the
      client with alternate async strategies (the default is `thread`_
      based, but `gevent`_, or `eventlet`_ ones can be provided as needed)

    .. _client: http://kazoo.readthedocs.org/en/latest/api/client.html
    .. _kazoo: http://kazoo.readthedocs.org/
    .. _retry: http://kazoo.readthedocs.org/en/latest/api/retry.html
    .. _gevent: http://kazoo.readthedocs.org/en/latest/api/\
                handlers/gevent.html
    .. _eventlet: http://kazoo.readthedocs.org/en/latest/api/\
                  handlers/eventlet.html
    .. _thread: http://kazoo.readthedocs.org/en/latest/api/\
                handlers/threading.html
    """
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
                        "non-empty string/list, not '%s' (%s)"
                        % (hosts, type(hosts)))
    client_kwargs['hosts'] = hosts
    if 'timeout' in conf:
        client_kwargs['timeout'] = float(conf['timeout'])
    # Kazoo supports various handlers, gevent, threading, eventlet...
    # allow the user of this client object to optionally specify one to be
    # used.
    if 'handler' in conf:
        client_kwargs['handler'] = conf['handler']
    return client.KazooClient(**client_kwargs)
