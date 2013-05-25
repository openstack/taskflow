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

import functools
import unittest

from taskflow import utils


class UtilTest(unittest.TestCase):
    def test_rollback_accum(self):
        context = {}

        def caller(token, e):
            context[token] = True

        accum = utils.RollbackAccumulator()

        def blowup():
            for i in range(0, 10):
                accum.add(functools.partial(caller, i))
            self.assertEquals(0, len(context))
            raise Exception

        # Test manual triggering
        self.assertEquals(0, len(accum))
        self.assertRaises(Exception, blowup)
        self.assertEquals(10, len(accum))
        self.assertEquals(0, len(context))
        accum.rollback(Exception())
        self.assertEquals(10, len(context))

        # Test context manager triggering
        context = {}
        accum.reset()
        self.assertEquals(0, len(accum))
        try:
            with accum:
                blowup()
        except Exception:
            pass
        self.assertEquals(10, len(accum))
        self.assertEquals(10, len(context))
