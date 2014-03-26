# -*- coding: utf-8 -*-

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
from taskflow import retry

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
    def test_flatten_task(self):
        task = t_utils.DummyTask(name='a')
        g = f_utils.flatten(task)

        self.assertEqual(list(g.nodes()), [task])
        self.assertEqual(list(g.edges()), [])

    def test_flatten_retry(self):
        r = retry.AlwaysRevert('r1')
        msg_regex = "^Retry controller .* is used not as a flow parameter"
        self.assertRaisesRegexp(TypeError, msg_regex, f_utils.flatten, r)

    def test_flatten_wrong_object(self):
        msg_regex = '^Unknown type requested to flatten'
        self.assertRaisesRegexp(TypeError, msg_regex, f_utils.flatten, 42)

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
        self.assertEqual(g.get_edge_data(c, d), {'invariant': True})

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
        self.assertFalse(lb.has_edge(b, a))
        self.assertTrue(lb.has_edge(a, b))
        self.assertEqual(g.get_edge_data(a, b), {'invariant': True})

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
        self.assertFalse(g.has_edge(d, c))
        self.assertTrue(g.has_edge(c, d))
        self.assertEqual(g.get_edge_data(c, d), {'invariant': True})

        ub = g.subgraph([a, b])
        self.assertEqual(0, ub.number_of_edges())
        lb = g.subgraph([c, d])
        self.assertEqual(1, lb.number_of_edges())

    def test_unordered_nested_in_linear_flatten(self):
        a, b, c, d = _make_many(4)
        flo = lf.Flow('lt').add(
            a,
            uf.Flow('ut').add(b, c),
            d)

        g = f_utils.flatten(flo)
        self.assertEqual(4, len(g))
        self.assertItemsEqual(g.edges(), [
            (a, b),
            (a, c),
            (b, d),
            (c, d)
        ])

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

        graph = f_utils.flatten(flo)
        self.assertEqual(7, len(graph))
        self.assertItemsEqual(graph.edges(data=True), [
            (e, f, {'invariant': True}),
            (f, g, {'invariant': True})
        ])

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
        self.assertItemsEqual(g.edges(data=True), [
            (a, b, {'manual': True}),
            (b, c, {'manual': True}),
            (c, d, {'manual': True}),
        ])
        self.assertItemsEqual([a], g_utils.get_no_predecessors(g))
        self.assertItemsEqual([d], g_utils.get_no_successors(g))

    def test_graph_flatten_dependencies(self):
        a = t_utils.ProvidesRequiresTask('a', provides=['x'], requires=[])
        b = t_utils.ProvidesRequiresTask('b', provides=[], requires=['x'])
        flo = gf.Flow("test").add(a, b)

        g = f_utils.flatten(flo)
        self.assertEqual(2, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (a, b, {'reasons': set(['x'])})
        ])
        self.assertItemsEqual([a], g_utils.get_no_predecessors(g))
        self.assertItemsEqual([b], g_utils.get_no_successors(g))

    def test_graph_flatten_nested_requires(self):
        a = t_utils.ProvidesRequiresTask('a', provides=['x'], requires=[])
        b = t_utils.ProvidesRequiresTask('b', provides=[], requires=[])
        c = t_utils.ProvidesRequiresTask('c', provides=[], requires=['x'])
        flo = gf.Flow("test").add(
            a,
            lf.Flow("test2").add(b, c)
        )

        g = f_utils.flatten(flo)
        self.assertEqual(3, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (a, c, {'reasons': set(['x'])}),
            (b, c, {'invariant': True})
        ])
        self.assertItemsEqual([a, b], g_utils.get_no_predecessors(g))
        self.assertItemsEqual([c], g_utils.get_no_successors(g))

    def test_graph_flatten_nested_provides(self):
        a = t_utils.ProvidesRequiresTask('a', provides=[], requires=['x'])
        b = t_utils.ProvidesRequiresTask('b', provides=['x'], requires=[])
        c = t_utils.ProvidesRequiresTask('c', provides=[], requires=[])
        flo = gf.Flow("test").add(
            a,
            lf.Flow("test2").add(b, c)
        )

        g = f_utils.flatten(flo)
        self.assertEqual(3, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (b, c, {'invariant': True}),
            (b, a, {'reasons': set(['x'])})
        ])
        self.assertItemsEqual([b], g_utils.get_no_predecessors(g))
        self.assertItemsEqual([a, c], g_utils.get_no_successors(g))

    def test_flatten_checks_for_dups(self):
        flo = gf.Flow("test").add(
            t_utils.DummyTask(name="a"),
            t_utils.DummyTask(name="a")
        )
        self.assertRaisesRegexp(exc.Duplicate,
                                '^Tasks with duplicate names',
                                f_utils.flatten, flo)

    def test_flatten_checks_for_dups_globally(self):
        flo = gf.Flow("test").add(
            gf.Flow("int1").add(t_utils.DummyTask(name="a")),
            gf.Flow("int2").add(t_utils.DummyTask(name="a")))
        self.assertRaisesRegexp(exc.Duplicate,
                                '^Tasks with duplicate names',
                                f_utils.flatten, flo)

    def test_flatten_retry_in_linear_flow(self):
        flo = lf.Flow("test", retry.AlwaysRevert("c"))
        g = f_utils.flatten(flo)
        self.assertEqual(1, len(g))
        self.assertEqual(0, g.number_of_edges())

    def test_flatten_retry_in_unordered_flow(self):
        flo = uf.Flow("test", retry.AlwaysRevert("c"))
        g = f_utils.flatten(flo)
        self.assertEqual(1, len(g))
        self.assertEqual(0, g.number_of_edges())

    def test_flatten_retry_in_graph_flow(self):
        flo = gf.Flow("test", retry.AlwaysRevert("c"))
        g = f_utils.flatten(flo)
        self.assertEqual(1, len(g))
        self.assertEqual(0, g.number_of_edges())

    def test_flatten_retry_in_nested_flows(self):
        c1 = retry.AlwaysRevert("c1")
        c2 = retry.AlwaysRevert("c2")
        flo = lf.Flow("test", c1).add(lf.Flow("test2", c2))
        g = f_utils.flatten(flo)

        self.assertEqual(2, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (c1, c2, {'retry': True})
        ])
        self.assertIs(c1, g.node[c2]['retry'])
        self.assertItemsEqual([c1], g_utils.get_no_predecessors(g))
        self.assertItemsEqual([c2], g_utils.get_no_successors(g))

    def test_flatten_retry_in_linear_flow_with_tasks(self):
        c = retry.AlwaysRevert("c")
        a, b = _make_many(2)
        flo = lf.Flow("test", c).add(a, b)
        g = f_utils.flatten(flo)
        self.assertEqual(3, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (a, b, {'invariant': True}),
            (c, a, {'retry': True})
        ])

        self.assertItemsEqual([c], g_utils.get_no_predecessors(g))
        self.assertItemsEqual([b], g_utils.get_no_successors(g))
        self.assertIs(c, g.node[a]['retry'])
        self.assertIs(c, g.node[b]['retry'])

    def test_flatten_retry_in_unordered_flow_with_tasks(self):
        c = retry.AlwaysRevert("c")
        a, b = _make_many(2)
        flo = uf.Flow("test", c).add(a, b)
        g = f_utils.flatten(flo)
        self.assertEqual(3, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (c, a, {'retry': True}),
            (c, b, {'retry': True})
        ])

        self.assertItemsEqual([c], g_utils.get_no_predecessors(g))
        self.assertItemsEqual([a, b], g_utils.get_no_successors(g))
        self.assertIs(c, g.node[a]['retry'])
        self.assertIs(c, g.node[b]['retry'])

    def test_flatten_retry_in_graph_flow_with_tasks(self):
        r = retry.AlwaysRevert("cp")
        a, b, c = _make_many(3)
        flo = gf.Flow("test", r).add(a, b, c).link(b, c)
        g = f_utils.flatten(flo)
        self.assertEqual(4, len(g))

        self.assertItemsEqual(g.edges(data=True), [
            (r, a, {'retry': True}),
            (r, b, {'retry': True}),
            (b, c, {'manual': True})
        ])

        self.assertItemsEqual([r], g_utils.get_no_predecessors(g))
        self.assertItemsEqual([a, c], g_utils.get_no_successors(g))
        self.assertIs(r, g.node[a]['retry'])
        self.assertIs(r, g.node[b]['retry'])
        self.assertIs(r, g.node[c]['retry'])

    def test_flatten_retries_hierarchy(self):
        c1 = retry.AlwaysRevert("cp1")
        c2 = retry.AlwaysRevert("cp2")
        a, b, c, d = _make_many(4)
        flo = lf.Flow("test", c1).add(
            a,
            lf.Flow("test", c2).add(b, c),
            d)
        g = f_utils.flatten(flo)
        self.assertEqual(6, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (c1, a, {'retry': True}),
            (a, c2, {'invariant': True}),
            (c2, b, {'retry': True}),
            (b, c, {'invariant': True}),
            (c, d, {'invariant': True}),
        ])
        self.assertIs(c1, g.node[a]['retry'])
        self.assertIs(c1, g.node[d]['retry'])
        self.assertIs(c2, g.node[b]['retry'])
        self.assertIs(c2, g.node[c]['retry'])
        self.assertIs(c1, g.node[c2]['retry'])
        self.assertIs(None, g.node[c1].get('retry'))

    def test_flatten_retry_subflows_hierarchy(self):
        c1 = retry.AlwaysRevert("cp1")
        a, b, c, d = _make_many(4)
        flo = lf.Flow("test", c1).add(
            a,
            lf.Flow("test").add(b, c),
            d)
        g = f_utils.flatten(flo)
        self.assertEqual(5, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (c1, a, {'retry': True}),
            (a, b, {'invariant': True}),
            (b, c, {'invariant': True}),
            (c, d, {'invariant': True}),
        ])
        self.assertIs(c1, g.node[a]['retry'])
        self.assertIs(c1, g.node[d]['retry'])
        self.assertIs(c1, g.node[b]['retry'])
        self.assertIs(c1, g.node[c]['retry'])
        self.assertIs(None, g.node[c1].get('retry'))
