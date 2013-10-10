# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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
import logging
import os
import shutil
import six
import threading
import weakref

from taskflow import exceptions as exc
from taskflow.openstack.common import jsonutils
from taskflow.openstack.common import timeutils
from taskflow.persistence.backends import base
from taskflow.persistence import logbook
from taskflow.utils import lock_utils
from taskflow.utils import misc
from taskflow.utils import persistence_utils as p_utils

LOG = logging.getLogger(__name__)

# The lock storage is not thread safe to set items in, so this lock is used to
# protect that access.
_LOCK_STORAGE_MUTATE = threading.RLock()

# Currently in use paths -> in-process locks are maintained here.
#
# NOTE(harlowja): Values in this dictionary will be automatically released once
# the objects referencing those objects have been garbage collected.
_LOCK_STORAGE = weakref.WeakValueDictionary()


class DirBackend(base.Backend):
    """A backend that writes logbooks, flow details, and task details to a
    provided directory. This backend does *not* provide transactional semantics
    although it does guarantee that there will be no race conditions when
    writing/reading by using file level locking and in-process locking.

    NOTE(harlowja): this is more of an example/testing backend and likely
    should *not* be used in production, since this backend lacks transactional
    semantics.
    """
    def __init__(self, conf):
        super(DirBackend, self).__init__(conf)
        self._path = os.path.abspath(conf['path'])
        self._lock_path = os.path.join(self._path, 'locks')
        self._file_cache = {}
        # Ensure that multiple threads are not accessing the same storage at
        # the same time, the file lock mechanism doesn't protect against this
        # so we must do in-process locking as well.
        with _LOCK_STORAGE_MUTATE:
            self._lock = _LOCK_STORAGE.setdefault(self._path,
                                                  threading.RLock())

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
        self._task_path = os.path.join(self._backend.base_path, 'tasks')
        self._book_path = os.path.join(self._backend.base_path, 'books')
        # Share the backends lock so that all threads using the given backend
        # are restricted in writing, since the per-process lock we are using
        # to restrict the multi-process access does not work inside a process.
        self._lock = backend._lock

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
                raise exc.StorageError("Failed running locking file based "
                                       "session: %s" % e, e)

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
        # NOTE(harlowja): don't hold the lock while iterating
        with self._lock:
            books = list(self._get_logbooks())
        for b in books:
            yield b

    @property
    def backend(self):
        return self._backend

    def close(self):
        pass

    def _save_task_details(self, task_detail, ignore_missing):
        # See if we have an existing task detail to merge with.
        e_td = None
        try:
            e_td = self._get_task_details(task_detail.uuid, lock=False)
        except EnvironmentError:
            if not ignore_missing:
                raise exc.NotFound("No task details found with id: %s"
                                   % task_detail.uuid)
        if e_td is not None:
            task_detail = p_utils.task_details_merge(e_td, task_detail)
        td_path = os.path.join(self._task_path, task_detail.uuid)
        td_data = _format_task_detail(task_detail)
        self._write_to(td_path, jsonutils.dumps(td_data))
        return task_detail

    @lock_utils.locked
    def update_task_details(self, task_detail):
        return self._run_with_process_lock("task",
                                           self._save_task_details,
                                           task_detail,
                                           ignore_missing=False)

    def _get_task_details(self, uuid, lock=True):

        def _get():
            td_path = os.path.join(self._task_path, uuid)
            td_data = jsonutils.loads(self._read_from(td_path))
            return _unformat_task_detail(uuid, td_data)

        if lock:
            return self._run_with_process_lock('task', _get)
        else:
            return _get()

    def _get_flow_details(self, uuid, lock=True):

        def _get():
            fd_path = os.path.join(self._flow_path, uuid)
            meta_path = os.path.join(fd_path, 'metadata')
            meta = jsonutils.loads(self._read_from(meta_path))
            fd = _unformat_flow_detail(uuid, meta)
            td_to_load = []
            td_path = os.path.join(fd_path, 'tasks')
            try:
                td_to_load = [f for f in os.listdir(td_path)
                              if os.path.islink(os.path.join(td_path, f))]
            except EnvironmentError as e:
                if e.errno != errno.ENOENT:
                    raise
            for t_uuid in td_to_load:
                fd.add(self._get_task_details(t_uuid))
            return fd

        if lock:
            return self._run_with_process_lock('flow', _get)
        else:
            return _get()

    def _save_tasks_and_link(self, task_details, local_task_path):
        for task_detail in task_details:
            self._save_task_details(task_detail, ignore_missing=True)
            src_td_path = os.path.join(self._task_path, task_detail.uuid)
            target_td_path = os.path.join(local_task_path, task_detail.uuid)
            try:
                os.symlink(src_td_path, target_td_path)
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
            e_fd = p_utils.flow_details_merge(e_fd, flow_detail)
            for td in flow_detail:
                if e_fd.find(td.uuid) is None:
                    e_fd.add(td)
            flow_detail = e_fd
        flow_path = os.path.join(self._flow_path, flow_detail.uuid)
        misc.ensure_tree(flow_path)
        self._write_to(os.path.join(flow_path, 'metadata'),
                       jsonutils.dumps(_format_flow_detail(flow_detail)))
        if len(flow_detail):
            task_path = os.path.join(flow_path, 'tasks')
            misc.ensure_tree(task_path)
            self._run_with_process_lock('task',
                                        self._save_tasks_and_link,
                                        list(flow_detail), task_path)
        return flow_detail

    @lock_utils.locked
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
            e_lb = p_utils.logbook_merge(e_lb, book)
            for fd in book:
                if e_lb.find(fd.uuid) is None:
                    e_lb.add(fd)
            book = e_lb
        book_path = os.path.join(self._book_path, book.uuid)
        misc.ensure_tree(book_path)
        created_at = None
        if e_lb is not None:
            created_at = e_lb.created_at
        self._write_to(os.path.join(book_path, 'metadata'),
                       jsonutils.dumps(_format_logbook(book,
                                                       created_at=created_at)))
        if len(book):
            flow_path = os.path.join(book_path, 'flows')
            misc.ensure_tree(flow_path)
            self._run_with_process_lock('flow',
                                        self._save_flows_and_link,
                                        list(book), flow_path)
        return book

    @lock_utils.locked
    def save_logbook(self, book):
        return self._run_with_process_lock("book",
                                           self._save_logbook, book)

    @lock_utils.locked
    def upgrade(self):

        def _step_create():
            for d in (self._book_path, self._flow_path, self._task_path):
                misc.ensure_tree(d)

        misc.ensure_tree(self._backend.base_path)
        misc.ensure_tree(self._backend.lock_path)
        self._run_with_process_lock("init", _step_create)

    @lock_utils.locked
    def clear_all(self):

        def _step_clear():
            for d in (self._book_path, self._flow_path, self._task_path):
                if os.path.isdir(d):
                    shutil.rmtree(d)

        def _step_task():
            self._run_with_process_lock("task", _step_clear)

        def _step_flow():
            self._run_with_process_lock("flow", _step_task)

        def _step_book():
            self._run_with_process_lock("book", _step_flow)

        # Acquire all locks by going through this little hiearchy.
        self._run_with_process_lock("init", _step_book)

    @lock_utils.locked
    def destroy_logbook(self, book_uuid):

        def _destroy_tasks(task_details):
            for task_detail in task_details:
                try:
                    shutil.rmtree(os.path.join(self._task_path,
                                               task_detail.uuid))
                except EnvironmentError as e:
                    if e.errno != errno.ENOENT:
                        raise

        def _destroy_flows(flow_details):
            for flow_detail in flow_details:
                self._run_with_process_lock("task", _destroy_tasks,
                                            list(flow_detail))
                try:
                    shutil.rmtree(os.path.join(self._flow_path,
                                               flow_detail.uuid))
                except EnvironmentError as e:
                    if e.errno != errno.ENOENT:
                        raise

        def _destroy_book():
            book = self._get_logbook(book_uuid)
            self._run_with_process_lock("flow", _destroy_flows, list(book))
            try:
                shutil.rmtree(os.path.join(self._book_path, book.uuid))
            except EnvironmentError as e:
                if e.errno != errno.ENOENT:
                    raise

        # Acquire all locks by going through this little hiearchy.
        self._run_with_process_lock("book", _destroy_book)

    def _get_logbook(self, book_uuid):
        book_path = os.path.join(self._book_path, book_uuid)
        meta_path = os.path.join(book_path, 'metadata')
        try:
            meta = jsonutils.loads(self._read_from(meta_path))
        except EnvironmentError as e:
            if e.errno == errno.ENOENT:
                raise exc.NotFound("No logbook found with id: %s" % book_uuid)
            else:
                raise
        lb = _unformat_logbook(book_uuid, meta)
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

    @lock_utils.locked
    def get_logbook(self, book_uuid):
        return self._run_with_process_lock("book",
                                           self._get_logbook, book_uuid)


###
# Internal <-> external model + other helper functions.
###

def _str_2_datetime(text):
    """Converts an iso8601 string/text into a datetime object (or none)"""
    if text is None:
        return None
    if not isinstance(text, six.string_types):
        raise ValueError("Can only convert strings into a datetime object and"
                         " not %r" % (text))
    if not len(text):
        return None
    return timeutils.parse_isotime(text)


def _format_task_detail(task_detail):
    return {
        'failure': p_utils.failure_to_dict(task_detail.failure),
        'meta': task_detail.meta,
        'name': task_detail.name,
        'results': task_detail.results,
        'state': task_detail.state,
        'version': task_detail.version,
    }


def _unformat_task_detail(uuid, td_data):
    td = logbook.TaskDetail(name=td_data['name'], uuid=uuid)
    td.state = td_data.get('state')
    td.results = td_data.get('results')
    td.failure = p_utils.failure_from_dict(td_data.get('failure'))
    td.meta = td_data.get('meta')
    td.version = td_data.get('version')
    return td


def _format_flow_detail(flow_detail):
    return {
        'name': flow_detail.name,
        'meta': flow_detail.meta,
        'state': flow_detail.state,
    }


def _unformat_flow_detail(uuid, fd_data):
    fd = logbook.FlowDetail(name=fd_data['name'], uuid=uuid)
    fd.state = fd_data.get('state')
    fd.meta = fd_data.get('meta')
    return fd


def _format_logbook(book, created_at=None):
    lb_data = {
        'name': book.name,
        'meta': book.meta,
    }
    if created_at:
        lb_data['created_at'] = timeutils.isotime(at=created_at)
        lb_data['updated_at'] = timeutils.isotime()
    else:
        lb_data['created_at'] = timeutils.isotime()
        lb_data['updated_at'] = None
    return lb_data


def _unformat_logbook(uuid, lb_data):
    lb = logbook.LogBook(name=lb_data['name'],
                         uuid=uuid,
                         updated_at=_str_2_datetime(lb_data['updated_at']),
                         created_at=_str_2_datetime(lb_data['created_at']))
    lb.meta = lb_data.get('meta')
    return lb
