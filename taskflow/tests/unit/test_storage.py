# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

from taskflow import exceptions
from taskflow import storage
from taskflow import test


class StorageTest(test.TestCase):
    def test_save_and_get(self):
        s = storage.Storage()
        s.save('42', 5)
        self.assertEquals(s.get('42'), 5)

    def test_get_non_existing_var(self):
        s = storage.Storage()
        with self.assertRaises(exceptions.NotFound):
            s.get('42')

    def test_reset(self):
        s = storage.Storage()
        s.save('42', 5)
        s.reset('42')
        with self.assertRaises(exceptions.NotFound):
            s.get('42')

    def test_reset_unknown_task(self):
        s = storage.Storage()
        self.assertEquals(s.reset('42'), None)
