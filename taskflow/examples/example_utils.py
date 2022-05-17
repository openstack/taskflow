# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
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
import os
import shutil
import sys
import tempfile

from urllib import parse as urllib_parse

from taskflow import exceptions
from taskflow.persistence import backends

LOG = logging.getLogger(__name__)

try:
    import sqlalchemy as _sa  # noqa
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False


def print_wrapped(text):
    print("-" * (len(text)))
    print(text)
    print("-" * (len(text)))


def rm_path(persist_path):
    if not os.path.exists(persist_path):
        return
    if os.path.isdir(persist_path):
        rm_func = shutil.rmtree
    elif os.path.isfile(persist_path):
        rm_func = os.unlink
    else:
        raise ValueError("Unknown how to `rm` path: %s" % (persist_path))
    try:
        rm_func(persist_path)
    except (IOError, OSError):
        pass


def _make_conf(backend_uri):
    parsed_url = urllib_parse.urlparse(backend_uri)
    backend_type = parsed_url.scheme.lower()
    if not backend_type:
        raise ValueError("Unknown backend type for uri: %s" % (backend_type))
    if backend_type in ('file', 'dir'):
        conf = {
            'path': parsed_url.path,
            'connection': backend_uri,
        }
    elif backend_type in ('zookeeper',):
        conf = {
            'path': parsed_url.path,
            'hosts': parsed_url.netloc,
            'connection': backend_uri,
        }
    else:
        conf = {
            'connection': backend_uri,
        }
    return conf


@contextlib.contextmanager
def get_backend(backend_uri=None):
    tmp_dir = None
    if not backend_uri:
        if len(sys.argv) > 1:
            backend_uri = str(sys.argv[1])
        if not backend_uri:
            tmp_dir = tempfile.mkdtemp()
            backend_uri = "file:///%s" % tmp_dir
    try:
        backend = backends.fetch(_make_conf(backend_uri))
    except exceptions.NotFound as e:
        # Fallback to one that will work if the provided backend is not found.
        if not tmp_dir:
            tmp_dir = tempfile.mkdtemp()
            backend_uri = "file:///%s" % tmp_dir
            LOG.exception("Falling back to file backend using temporary"
                          " directory located at: %s", tmp_dir)
            backend = backends.fetch(_make_conf(backend_uri))
        else:
            raise e
    try:
        # Ensure schema upgraded before we continue working.
        with contextlib.closing(backend.get_connection()) as conn:
            conn.upgrade()
        yield backend
    finally:
        # Make sure to cleanup the temporary path if one was created for us.
        if tmp_dir:
            rm_path(tmp_dir)
