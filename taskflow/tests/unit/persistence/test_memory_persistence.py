# -*- coding: utf-8 -*-

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

import contextlib

from taskflow import exceptions as exc
from taskflow.persistence import backends
from taskflow.persistence.backends import impl_memory
from taskflow import test
from taskflow.tests.unit.persistence import base


class MemoryPersistenceTest(test.TestCase, base.PersistenceTestMixin):
    def setUp(self):
        super(MemoryPersistenceTest, self).setUp()
        self._backend = impl_memory.MemoryBackend({})

    def _get_connection(self):
        return self._backend.get_connection()

    def tearDown(self):
        conn = self._get_connection()
        conn.clear_all()
        self._backend = None
        super(MemoryPersistenceTest, self).tearDown()

    def test_memory_backend_entry_point(self):
        conf = {'connection': 'memory:'}
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_memory.MemoryBackend)

    def test_memory_backend_fetch_by_name(self):
        conf = {'connection': 'memory'}  # note no colon
        with contextlib.closing(backends.fetch(conf)) as be:
            self.assertIsInstance(be, impl_memory.MemoryBackend)


class MemoryFilesystemTest(test.TestCase):

    @staticmethod
    def _get_item_path(fs, path):
        # TODO(harlowja): is there a better way to do this??
        return fs[path]

    @staticmethod
    def _del_item_path(fs, path):
        # TODO(harlowja): is there a better way to do this??
        del fs[path]

    def test_set_get_ls(self):
        fs = impl_memory.FakeFilesystem()
        fs['/d'] = 'd'
        fs['/c'] = 'c'
        fs['/d/b'] = 'db'
        self.assertEqual(2, len(fs.ls('/')))
        self.assertEqual(1, len(fs.ls('/d')))
        self.assertEqual('d', fs['/d'])
        self.assertEqual('c', fs['/c'])
        self.assertEqual('db', fs['/d/b'])

    def test_ls_recursive(self):
        fs = impl_memory.FakeFilesystem()
        fs.ensure_path("/d")
        fs.ensure_path("/c/d")
        fs.ensure_path("/b/c/d")
        fs.ensure_path("/a/b/c/d")
        contents = fs.ls_r("/", absolute=False)
        self.assertEqual([
            'a',
            'b',
            'c',
            'd',
            'a/b',
            'b/c',
            'c/d',
            'a/b/c',
            'b/c/d',
            'a/b/c/d',
        ], contents)

    def test_ls_recursive_absolute(self):
        fs = impl_memory.FakeFilesystem()
        fs.ensure_path("/d")
        fs.ensure_path("/c/d")
        fs.ensure_path("/b/c/d")
        fs.ensure_path("/a/b/c/d")
        contents = fs.ls_r("/", absolute=True)
        self.assertEqual([
            '/a',
            '/b',
            '/c',
            '/d',
            '/a/b',
            '/b/c',
            '/c/d',
            '/a/b/c',
            '/b/c/d',
            '/a/b/c/d',
        ], contents)

    def test_ls_recursive_targeted(self):
        fs = impl_memory.FakeFilesystem()
        fs.ensure_path("/d")
        fs.ensure_path("/c/d")
        fs.ensure_path("/b/c/d")
        fs.ensure_path("/a/b/c/d")
        contents = fs.ls_r("/a/b", absolute=False)
        self.assertEqual(['c', 'c/d'], contents)

    def test_ls_targeted(self):
        fs = impl_memory.FakeFilesystem()
        fs.ensure_path("/d")
        fs.ensure_path("/c/d")
        fs.ensure_path("/b/c/d")
        fs.ensure_path("/a/b/c/d")
        contents = fs.ls("/a/b", absolute=False)
        self.assertEqual(['c'], contents)

    def test_ls_targeted_absolute(self):
        fs = impl_memory.FakeFilesystem()
        fs.ensure_path("/d")
        fs.ensure_path("/c/d")
        fs.ensure_path("/b/c/d")
        fs.ensure_path("/a/b/c/d")
        contents = fs.ls("/a/b", absolute=True)
        self.assertEqual(['/a/b/c'], contents)

    def test_ls_recursive_targeted_absolute(self):
        fs = impl_memory.FakeFilesystem()
        fs.ensure_path("/d")
        fs.ensure_path("/c/d")
        fs.ensure_path("/b/c/d")
        fs.ensure_path("/a/b/c/d")
        contents = fs.ls_r("/a/b", absolute=True)
        self.assertEqual(['/a/b/c', '/a/b/c/d'], contents)

    def test_ensure_path(self):
        fs = impl_memory.FakeFilesystem()
        pieces = ['a', 'b', 'c']
        path = "/" + "/".join(pieces)
        fs.ensure_path(path)
        path = fs.root_path
        for i, p in enumerate(pieces):
            if i == 0:
                path += p
            else:
                path += "/" + p
            self.assertIsNone(fs[path])

    def test_clear(self):
        fs = impl_memory.FakeFilesystem()
        paths = ['/b', '/c', '/a/b/c']
        for p in paths:
            fs.ensure_path(p)
        for p in paths:
            self.assertIsNone(self._get_item_path(fs, p))
        fs.clear()
        for p in paths:
            self.assertRaises(exc.NotFound, self._get_item_path, fs, p)

    def test_not_found(self):
        fs = impl_memory.FakeFilesystem()
        self.assertRaises(exc.NotFound, self._get_item_path, fs, '/c')

    def test_bad_norms(self):
        fs = impl_memory.FakeFilesystem()
        self.assertRaises(ValueError, fs.normpath, '')
        self.assertRaises(ValueError, fs.normpath, 'abc/c')
        self.assertRaises(ValueError, fs.normpath, '../c')

    def test_del_root_not_allowed(self):
        fs = impl_memory.FakeFilesystem()
        self.assertRaises(ValueError, fs.delete, "/", recursive=False)

    def test_del_no_children_allowed(self):
        fs = impl_memory.FakeFilesystem()
        fs['/a'] = 'a'
        self.assertEqual(1, len(fs.ls_r("/")))
        fs.delete("/a")
        self.assertEqual(0, len(fs.ls("/")))

    def test_del_many_children_not_allowed(self):
        fs = impl_memory.FakeFilesystem()
        fs['/a'] = 'a'
        fs['/a/b'] = 'b'
        self.assertRaises(ValueError, fs.delete, "/", recursive=False)

    def test_del_with_children_not_allowed(self):
        fs = impl_memory.FakeFilesystem()
        fs['/a'] = 'a'
        fs['/a/b'] = 'b'
        self.assertRaises(ValueError, fs.delete, "/a", recursive=False)

    def test_del_many_children_allowed(self):
        fs = impl_memory.FakeFilesystem()
        fs['/a'] = 'a'
        fs['/a/b'] = 'b'
        self.assertEqual(2, len(fs.ls_r("/")))
        fs.delete("/a", recursive=True)
        self.assertEqual(0, len(fs.ls("/")))

    def test_del_many_children_allowed_not_recursive(self):
        fs = impl_memory.FakeFilesystem()
        fs['/a'] = 'a'
        fs['/a/b'] = 'b'
        self.assertEqual(2, len(fs.ls_r("/")))
        fs.delete("/a/b", recursive=False)
        self.assertEqual(1, len(fs.ls("/")))
        fs.delete("/a", recursive=False)
        self.assertEqual(0, len(fs.ls("/")))

    def test_link_loop_raises(self):
        fs = impl_memory.FakeFilesystem()
        fs['/b'] = 'c'
        fs.symlink('/b', '/b')
        self.assertRaises(ValueError, self._get_item_path, fs, '/b')

    def test_ensure_linked_delete(self):
        fs = impl_memory.FakeFilesystem()
        fs['/b'] = 'd'
        fs.symlink('/b', '/c')
        self.assertEqual('d', fs['/b'])
        self.assertEqual('d', fs['/c'])
        del fs['/b']
        self.assertRaises(exc.NotFound, self._get_item_path, fs, '/c')
        self.assertRaises(exc.NotFound, self._get_item_path, fs, '/b')
