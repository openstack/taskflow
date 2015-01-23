# -*- coding: utf-8 -*-

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
#    Copyright (C) 2013 Rackspace Hosting All Rights Reserved.
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

import errno
import os
import shutil

from oslo_serialization import jsonutils
import six

from taskflow import exceptions as exc
from taskflow import logging
from taskflow.persistence import base
from taskflow.persistence import logbook
from taskflow.utils import lock_utils
from taskflow.utils import misc

LOG = logging.getLogger(__name__)


class DirBackend(base.Backend):
    """A directory and file based backend.

    This backend writes logbooks, flow details, and atom details to a provided
    base path on the local filesystem. It will create and store those objects
    in three key directories (one for logbooks, one for flow details and one
    for atom details). It creates those associated directories and then
    creates files inside those directories that represent the contents of those
    objects for later reading and writing.

    This backend does *not* provide true transactional semantics. It does
    guarantee that there will be no interprocess race conditions when
    writing and reading by using a consistent hierarchy of file based locks.

    Example configuration::

        conf = {
            "path": "/tmp/taskflow",
        }
    """
    def __init__(self, conf):
        super(DirBackend, self).__init__(conf)
        self._path = os.path.abspath(conf['path'])
        self._lock_path = os.path.join(self._path, 'locks')
        self._file_cache = {}

    @property
    def lock_path(self):
        return self._lock_path

    @property
    def base_path(self):
        return self._path

    def get_connection(self):
        return Connection(self)

    def close(self):
        pass


class Connection(base.Connection):
    def __init__(self, backend):
        self._backend = backend
        self._file_cache = self._backend._file_cache
        self._flow_path = os.path.join(self._backend.base_path, 'flows')
        self._atom_path = os.path.join(self._backend.base_path, 'atoms')
        self._book_path = os.path.join(self._backend.base_path, 'books')

    def validate(self):
        # Verify key paths exist.
        paths = [
            self._backend.base_path,
            self._backend.lock_path,
            self._flow_path,
            self._atom_path,
            self._book_path,
        ]
        for p in paths:
            if not os.path.isdir(p):
                raise RuntimeError("Missing required directory: %s" % (p))

    def _read_from(self, filename):
        # This is very similar to the oslo-incubator fileutils module, but
        # tweaked to not depend on a global cache, as well as tweaked to not
        # pull-in the oslo logging module (which is a huge pile of code).
        mtime = os.path.getmtime(filename)
        cache_info = self._file_cache.setdefault(filename, {})
        if not cache_info or mtime > cache_info.get('mtime', 0):
            with open(filename, 'rb') as fp:
                cache_info['data'] = fp.read().decode('utf-8')
                cache_info['mtime'] = mtime
        return cache_info['data']

    def _write_to(self, filename, contents):
        if isinstance(contents, six.text_type):
            contents = contents.encode('utf-8')
        with open(filename, 'wb') as fp:
            fp.write(contents)
        self._file_cache.pop(filename, None)

    def _run_with_process_lock(self, lock_name, functor, *args, **kwargs):
        lock_path = os.path.join(self.backend.lock_path, lock_name)
        with lock_utils.InterProcessLock(lock_path):
            try:
                return functor(*args, **kwargs)
            except exc.TaskFlowException:
                raise
            except Exception as e:
                LOG.exception("Failed running locking file based session")
                # NOTE(harlowja): trap all other errors as storage errors.
                raise exc.StorageFailure("Storage backend internal error", e)

    def _get_logbooks(self):
        lb_uuids = []
        try:
            lb_uuids = [d for d in os.listdir(self._book_path)
                        if os.path.isdir(os.path.join(self._book_path, d))]
        except EnvironmentError as e:
            if e.errno != errno.ENOENT:
                raise
        for lb_uuid in lb_uuids:
            try:
                yield self._get_logbook(lb_uuid)
            except exc.NotFound:
                pass

    def get_logbooks(self):
        try:
            books = list(self._get_logbooks())
        except EnvironmentError as e:
            raise exc.StorageFailure("Unable to fetch logbooks", e)
        else:
            for b in books:
                yield b

    @property
    def backend(self):
        return self._backend

    def close(self):
        pass

    def _save_atom_details(self, atom_detail, ignore_missing):
        # See if we have an existing atom detail to merge with.
        e_ad = None
        try:
            e_ad = self._get_atom_details(atom_detail.uuid, lock=False)
        except EnvironmentError:
            if not ignore_missing:
                raise exc.NotFound("No atom details found with id: %s"
                                   % atom_detail.uuid)
        if e_ad is not None:
            atom_detail = e_ad.merge(atom_detail)
        ad_path = os.path.join(self._atom_path, atom_detail.uuid)
        ad_data = base._format_atom(atom_detail)
        self._write_to(ad_path, jsonutils.dumps(ad_data))
        return atom_detail

    def update_atom_details(self, atom_detail):
        return self._run_with_process_lock("atom",
                                           self._save_atom_details,
                                           atom_detail,
                                           ignore_missing=False)

    def _get_atom_details(self, uuid, lock=True):

        def _get():
            ad_path = os.path.join(self._atom_path, uuid)
            ad_data = misc.decode_json(self._read_from(ad_path))
            ad_cls = logbook.atom_detail_class(ad_data['type'])
            return ad_cls.from_dict(ad_data['atom'])

        if lock:
            return self._run_with_process_lock('atom', _get)
        else:
            return _get()

    def _get_flow_details(self, uuid, lock=True):

        def _get():
            fd_path = os.path.join(self._flow_path, uuid)
            meta_path = os.path.join(fd_path, 'metadata')
            meta = misc.decode_json(self._read_from(meta_path))
            fd = logbook.FlowDetail.from_dict(meta)
            ad_to_load = []
            ad_path = os.path.join(fd_path, 'atoms')
            try:
                ad_to_load = [f for f in os.listdir(ad_path)
                              if os.path.islink(os.path.join(ad_path, f))]
            except EnvironmentError as e:
                if e.errno != errno.ENOENT:
                    raise
            for ad_uuid in ad_to_load:
                fd.add(self._get_atom_details(ad_uuid))
            return fd

        if lock:
            return self._run_with_process_lock('flow', _get)
        else:
            return _get()

    def _save_atoms_and_link(self, atom_details, local_atom_path):
        for atom_detail in atom_details:
            self._save_atom_details(atom_detail, ignore_missing=True)
            src_ad_path = os.path.join(self._atom_path, atom_detail.uuid)
            target_ad_path = os.path.join(local_atom_path, atom_detail.uuid)
            try:
                os.symlink(src_ad_path, target_ad_path)
            except EnvironmentError as e:
                if e.errno != errno.EEXIST:
                    raise

    def _save_flow_details(self, flow_detail, ignore_missing):
        # See if we have an existing flow detail to merge with.
        e_fd = None
        try:
            e_fd = self._get_flow_details(flow_detail.uuid, lock=False)
        except EnvironmentError:
            if not ignore_missing:
                raise exc.NotFound("No flow details found with id: %s"
                                   % flow_detail.uuid)
        if e_fd is not None:
            e_fd = e_fd.merge(flow_detail)
            for ad in flow_detail:
                if e_fd.find(ad.uuid) is None:
                    e_fd.add(ad)
            flow_detail = e_fd
        flow_path = os.path.join(self._flow_path, flow_detail.uuid)
        misc.ensure_tree(flow_path)
        self._write_to(os.path.join(flow_path, 'metadata'),
                       jsonutils.dumps(flow_detail.to_dict()))
        if len(flow_detail):
            atom_path = os.path.join(flow_path, 'atoms')
            misc.ensure_tree(atom_path)
            self._run_with_process_lock('atom',
                                        self._save_atoms_and_link,
                                        list(flow_detail), atom_path)
        return flow_detail

    def update_flow_details(self, flow_detail):
        return self._run_with_process_lock("flow",
                                           self._save_flow_details,
                                           flow_detail,
                                           ignore_missing=False)

    def _save_flows_and_link(self, flow_details, local_flow_path):
        for flow_detail in flow_details:
            self._save_flow_details(flow_detail, ignore_missing=True)
            src_fd_path = os.path.join(self._flow_path, flow_detail.uuid)
            target_fd_path = os.path.join(local_flow_path, flow_detail.uuid)
            try:
                os.symlink(src_fd_path, target_fd_path)
            except EnvironmentError as e:
                if e.errno != errno.EEXIST:
                    raise

    def _save_logbook(self, book):
        # See if we have an existing logbook to merge with.
        e_lb = None
        try:
            e_lb = self._get_logbook(book.uuid)
        except exc.NotFound:
            pass
        if e_lb is not None:
            e_lb = e_lb.merge(book)
            for fd in book:
                if e_lb.find(fd.uuid) is None:
                    e_lb.add(fd)
            book = e_lb
        book_path = os.path.join(self._book_path, book.uuid)
        misc.ensure_tree(book_path)
        self._write_to(os.path.join(book_path, 'metadata'),
                       jsonutils.dumps(book.to_dict(marshal_time=True)))
        if len(book):
            flow_path = os.path.join(book_path, 'flows')
            misc.ensure_tree(flow_path)
            self._run_with_process_lock('flow',
                                        self._save_flows_and_link,
                                        list(book), flow_path)
        return book

    def save_logbook(self, book):
        return self._run_with_process_lock("book",
                                           self._save_logbook, book)

    def upgrade(self):

        def _step_create():
            for path in (self._book_path, self._flow_path, self._atom_path):
                try:
                    misc.ensure_tree(path)
                except EnvironmentError as e:
                    raise exc.StorageFailure("Unable to create logbooks"
                                             " required child path %s" % path,
                                             e)

        for path in (self._backend.base_path, self._backend.lock_path):
            try:
                misc.ensure_tree(path)
            except EnvironmentError as e:
                raise exc.StorageFailure("Unable to create logbooks required"
                                         " path %s" % path, e)

        self._run_with_process_lock("init", _step_create)

    def clear_all(self):

        def _step_clear():
            for d in (self._book_path, self._flow_path, self._atom_path):
                if os.path.isdir(d):
                    shutil.rmtree(d)

        def _step_atom():
            self._run_with_process_lock("atom", _step_clear)

        def _step_flow():
            self._run_with_process_lock("flow", _step_atom)

        def _step_book():
            self._run_with_process_lock("book", _step_flow)

        # Acquire all locks by going through this little hierarchy.
        self._run_with_process_lock("init", _step_book)

    def destroy_logbook(self, book_uuid):

        def _destroy_atoms(atom_details):
            for atom_detail in atom_details:
                atom_path = os.path.join(self._atom_path, atom_detail.uuid)
                try:
                    shutil.rmtree(atom_path)
                except EnvironmentError as e:
                    if e.errno != errno.ENOENT:
                        raise exc.StorageFailure("Unable to remove atom"
                                                 " directory %s" % atom_path,
                                                 e)

        def _destroy_flows(flow_details):
            for flow_detail in flow_details:
                flow_path = os.path.join(self._flow_path, flow_detail.uuid)
                self._run_with_process_lock("atom", _destroy_atoms,
                                            list(flow_detail))
                try:
                    shutil.rmtree(flow_path)
                except EnvironmentError as e:
                    if e.errno != errno.ENOENT:
                        raise exc.StorageFailure("Unable to remove flow"
                                                 " directory %s" % flow_path,
                                                 e)

        def _destroy_book():
            book = self._get_logbook(book_uuid)
            book_path = os.path.join(self._book_path, book.uuid)
            self._run_with_process_lock("flow", _destroy_flows, list(book))
            try:
                shutil.rmtree(book_path)
            except EnvironmentError as e:
                if e.errno != errno.ENOENT:
                    raise exc.StorageFailure("Unable to remove book"
                                             " directory %s" % book_path, e)

        # Acquire all locks by going through this little hierarchy.
        self._run_with_process_lock("book", _destroy_book)

    def _get_logbook(self, book_uuid):
        book_path = os.path.join(self._book_path, book_uuid)
        meta_path = os.path.join(book_path, 'metadata')
        try:
            meta = misc.decode_json(self._read_from(meta_path))
        except EnvironmentError as e:
            if e.errno == errno.ENOENT:
                raise exc.NotFound("No logbook found with id: %s" % book_uuid)
            else:
                raise
        lb = logbook.LogBook.from_dict(meta, unmarshal_time=True)
        fd_path = os.path.join(book_path, 'flows')
        fd_uuids = []
        try:
            fd_uuids = [f for f in os.listdir(fd_path)
                        if os.path.islink(os.path.join(fd_path, f))]
        except EnvironmentError as e:
            if e.errno != errno.ENOENT:
                raise
        for fd_uuid in fd_uuids:
            lb.add(self._get_flow_details(fd_uuid))
        return lb

    def get_logbook(self, book_uuid):
        return self._run_with_process_lock("book",
                                           self._get_logbook, book_uuid)
