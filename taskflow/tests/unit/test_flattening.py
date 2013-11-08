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

import string

import networkx as nx

from taskflow import exceptions as exc
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf

from taskflow import test
from taskflow.tests import utils as t_utils
from taskflow.utils import flow_utils as f_utils
from taskflow.utils import graph_utils as g_utils


def _make_many(amount):
    assert amount <= len(string.ascii_lowercase), 'Not enough letters'
    tasks = []
    for i in range(0, amount):
        tasks.append(t_utils.DummyTask(name=string.ascii_lowercase[i]))
    return tasks


class FlattenTest(test.TestCase):
    def test_linear_flatten(self):
        a, b, c, d = _make_many(4)
        flo = lf.Flow("test")
        flo.add(a, b, c)
        sflo = lf.Flow("sub-test")
        sflo.add(d)
        flo.add(sflo)

        g = f_utils.flatten(flo)
        self.assertEqual(4, len(g))

        order = nx.topological_sort(g)
        self.assertEqual([a, b, c, d], order)
        self.assertTrue(g.has_edge(c, d))
        self.assertEqual([d], list(g_utils.get_no_successors(g)))
        self.assertEqual([a], list(g_utils.get_no_predecessors(g)))

    def test_invalid_flatten(self):
        a, b, c = _make_many(3)
        flo = lf.Flow("test")
        flo.add(a, b, c)
        flo.add(flo)
        self.assertRaises(ValueError, f_utils.flatten, flo)

    def test_unordered_flatten(self):
        a, b, c, d = _make_many(4)
        flo = uf.Flow("test")
        flo.add(a, b, c, d)
        g = f_utils.flatten(flo)
        self.assertEqual(4, len(g))
        self.assertEqual(0, g.number_of_edges())
        self.assertEqual(set([a, b, c, d]),
                         set(g_utils.get_no_successors(g)))
        self.assertEqual(set([a, b, c, d]),
                         set(g_utils.get_no_predecessors(g)))

    def test_linear_nested_flatten(self):
        a, b, c, d = _make_many(4)
        flo = lf.Flow("test")
        flo.add(a, b)
        flo2 = uf.Flow("test2")
        flo2.add(c, d)
        flo.add(flo2)
        g = f_utils.flatten(flo)
        self.assertEqual(4, len(g))

        lb = g.subgraph([a, b])
        self.assertTrue(lb.has_edge(a, b))
        self.assertFalse(lb.has_edge(b, a))

        ub = g.subgraph([c, d])
        self.assertEqual(0, ub.number_of_edges())

        # This ensures that c and d do not start executing until after b.
        self.assertTrue(g.has_edge(b, c))
        self.assertTrue(g.has_edge(b, d))

    def test_unordered_nested_flatten(self):
        a, b, c, d = _make_many(4)
        flo = uf.Flow("test")
        flo.add(a, b)
        flo2 = lf.Flow("test2")
        flo2.add(c, d)
        flo.add(flo2)

        g = f_utils.flatten(flo)
        self.assertEqual(4, len(g))
        for n in [a, b]:
            self.assertFalse(g.has_edge(n, c))
            self.assertFalse(g.has_edge(n, d))
        self.assertTrue(g.has_edge(c, d))
        self.assertFalse(g.has_edge(d, c))

        ub = g.subgraph([a, b])
        self.assertEqual(0, ub.number_of_edges())
        lb = g.subgraph([c, d])
        self.assertEqual(1, lb.number_of_edges())

    def test_graph_flatten(self):
        a, b, c, d = _make_many(4)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)

        g = f_utils.flatten(flo)
        self.assertEqual(4, len(g))
        self.assertEqual(0, g.number_of_edges())

    def test_graph_flatten_nested(self):
        a, b, c, d, e, f, g = _make_many(7)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)

        flo2 = lf.Flow('test2')
        flo2.add(e, f, g)
        flo.add(flo2)

        g = f_utils.flatten(flo)
        self.assertEqual(7, len(g))
        self.assertEqual(2, g.number_of_edges())

    def test_graph_flatten_nested_graph(self):
        a, b, c, d, e, f, g = _make_many(7)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)

        flo2 = gf.Flow('test2')
        flo2.add(e, f, g)
        flo.add(flo2)

        g = f_utils.flatten(flo)
        self.assertEqual(7, len(g))
        self.assertEqual(0, g.number_of_edges())

    def test_graph_flatten_links(self):
        a, b, c, d = _make_many(4)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)
        flo.link(a, b)
        flo.link(b, c)
        flo.link(c, d)

        g = f_utils.flatten(flo)
        self.assertEqual(4, len(g))
        self.assertEqual(3, g.number_of_edges())
        self.assertEqual(set([a]),
                         set(g_utils.get_no_predecessors(g)))
        self.assertEqual(set([d]),
                         set(g_utils.get_no_successors(g)))

    def test_flatten_checks_for_dups(self):
        flo = gf.Flow("test").add(
            t_utils.DummyTask(name="a"),
            t_utils.DummyTask(name="a")
        )
        with self.assertRaisesRegexp(exc.InvariantViolationException,
                                     '^Tasks with duplicate names'):
            f_utils.flatten(flo)

    def test_flatten_checks_for_dups_globally(self):
        flo = gf.Flow("test").add(
            gf.Flow("int1").add(t_utils.DummyTask(name="a")),
            gf.Flow("int2").add(t_utils.DummyTask(name="a")))
        with self.assertRaisesRegexp(exc.InvariantViolationException,
                                     '^Tasks with duplicate names'):
            f_utils.flatten(flo)
