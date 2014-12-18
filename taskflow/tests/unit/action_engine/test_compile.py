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

from taskflow.engines.action_engine import compiler
from taskflow import exceptions as exc
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow.patterns import unordered_flow as uf
from taskflow import retry
from taskflow import test
from taskflow.tests import utils as test_utils


class PatternCompileTest(test.TestCase):
    def test_task(self):
        task = test_utils.DummyTask(name='a')
        compilation = compiler.PatternCompiler(task).compile()
        g = compilation.execution_graph
        self.assertEqual(list(g.nodes()), [task])
        self.assertEqual(list(g.edges()), [])

    def test_retry(self):
        r = retry.AlwaysRevert('r1')
        msg_regex = "^Retry controller .* must only be used .*"
        self.assertRaisesRegexp(TypeError, msg_regex,
                                compiler.PatternCompiler(r).compile)

    def test_wrong_object(self):
        msg_regex = '^Unknown item .* requested to flatten'
        self.assertRaisesRegexp(TypeError, msg_regex,
                                compiler.PatternCompiler(42).compile)

    def test_empty(self):
        flo = lf.Flow("test")
        self.assertRaises(exc.Empty, compiler.PatternCompiler(flo).compile)

    def test_linear(self):
        a, b, c, d = test_utils.make_many(4)
        flo = lf.Flow("test")
        flo.add(a, b, c)
        sflo = lf.Flow("sub-test")
        sflo.add(d)
        flo.add(sflo)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(4, len(g))

        order = g.topological_sort()
        self.assertEqual([a, b, c, d], order)
        self.assertTrue(g.has_edge(c, d))
        self.assertEqual(g.get_edge_data(c, d), {'invariant': True})

        self.assertEqual([d], list(g.no_successors_iter()))
        self.assertEqual([a], list(g.no_predecessors_iter()))

    def test_invalid(self):
        a, b, c = test_utils.make_many(3)
        flo = lf.Flow("test")
        flo.add(a, b, c)
        flo.add(flo)
        self.assertRaises(ValueError,
                          compiler.PatternCompiler(flo).compile)

    def test_unordered(self):
        a, b, c, d = test_utils.make_many(4)
        flo = uf.Flow("test")
        flo.add(a, b, c, d)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(4, len(g))
        self.assertEqual(0, g.number_of_edges())
        self.assertEqual(set([a, b, c, d]),
                         set(g.no_successors_iter()))
        self.assertEqual(set([a, b, c, d]),
                         set(g.no_predecessors_iter()))

    def test_linear_nested(self):
        a, b, c, d = test_utils.make_many(4)
        flo = lf.Flow("test")
        flo.add(a, b)
        flo2 = uf.Flow("test2")
        flo2.add(c, d)
        flo.add(flo2)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
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

    def test_unordered_nested(self):
        a, b, c, d = test_utils.make_many(4)
        flo = uf.Flow("test")
        flo.add(a, b)
        flo2 = lf.Flow("test2")
        flo2.add(c, d)
        flo.add(flo2)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
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

    def test_unordered_nested_in_linear(self):
        a, b, c, d = test_utils.make_many(4)
        flo = lf.Flow('lt').add(
            a,
            uf.Flow('ut').add(b, c),
            d)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(4, len(g))
        self.assertItemsEqual(g.edges(), [
            (a, b),
            (a, c),
            (b, d),
            (c, d)
        ])

    def test_graph(self):
        a, b, c, d = test_utils.make_many(4)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(4, len(g))
        self.assertEqual(0, g.number_of_edges())

    def test_graph_nested(self):
        a, b, c, d, e, f, g = test_utils.make_many(7)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)

        flo2 = lf.Flow('test2')
        flo2.add(e, f, g)
        flo.add(flo2)

        compilation = compiler.PatternCompiler(flo).compile()
        graph = compilation.execution_graph
        self.assertEqual(7, len(graph))
        self.assertItemsEqual(graph.edges(data=True), [
            (e, f, {'invariant': True}),
            (f, g, {'invariant': True})
        ])

    def test_graph_nested_graph(self):
        a, b, c, d, e, f, g = test_utils.make_many(7)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)

        flo2 = gf.Flow('test2')
        flo2.add(e, f, g)
        flo.add(flo2)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(7, len(g))
        self.assertEqual(0, g.number_of_edges())

    def test_graph_links(self):
        a, b, c, d = test_utils.make_many(4)
        flo = gf.Flow("test")
        flo.add(a, b, c, d)
        flo.link(a, b)
        flo.link(b, c)
        flo.link(c, d)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(4, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (a, b, {'manual': True}),
            (b, c, {'manual': True}),
            (c, d, {'manual': True}),
        ])
        self.assertItemsEqual([a], g.no_predecessors_iter())
        self.assertItemsEqual([d], g.no_successors_iter())

    def test_graph_dependencies(self):
        a = test_utils.ProvidesRequiresTask('a', provides=['x'], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=['x'])
        flo = gf.Flow("test").add(a, b)

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(2, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (a, b, {'reasons': set(['x'])})
        ])
        self.assertItemsEqual([a], g.no_predecessors_iter())
        self.assertItemsEqual([b], g.no_successors_iter())

    def test_graph_nested_requires(self):
        a = test_utils.ProvidesRequiresTask('a', provides=['x'], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=[])
        c = test_utils.ProvidesRequiresTask('c', provides=[], requires=['x'])
        flo = gf.Flow("test").add(
            a,
            lf.Flow("test2").add(b, c)
        )

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(3, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (a, c, {'reasons': set(['x'])}),
            (b, c, {'invariant': True})
        ])
        self.assertItemsEqual([a, b], g.no_predecessors_iter())
        self.assertItemsEqual([c], g.no_successors_iter())

    def test_graph_nested_provides(self):
        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=['x'])
        b = test_utils.ProvidesRequiresTask('b', provides=['x'], requires=[])
        c = test_utils.ProvidesRequiresTask('c', provides=[], requires=[])
        flo = gf.Flow("test").add(
            a,
            lf.Flow("test2").add(b, c)
        )

        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(3, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (b, c, {'invariant': True}),
            (b, a, {'reasons': set(['x'])})
        ])
        self.assertItemsEqual([b], g.no_predecessors_iter())
        self.assertItemsEqual([a, c], g.no_successors_iter())

    def test_empty_flow_in_linear_flow(self):
        flow = lf.Flow('lf')
        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=[])
        empty_flow = gf.Flow("empty")
        flow.add(a, empty_flow, b)

        compilation = compiler.PatternCompiler(flow).compile()
        g = compilation.execution_graph
        self.assertItemsEqual(g.edges(data=True), [
            (a, b, {'invariant': True}),
        ])

    def test_many_empty_in_graph_flow(self):
        flow = gf.Flow('root')

        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=[])
        flow.add(a)

        b = lf.Flow('b')
        b_0 = test_utils.ProvidesRequiresTask('b.0', provides=[], requires=[])
        b_3 = test_utils.ProvidesRequiresTask('b.3', provides=[], requires=[])
        b.add(
            b_0,
            lf.Flow('b.1'), lf.Flow('b.2'),
            b_3,
        )
        flow.add(b)

        c = lf.Flow('c')
        c.add(lf.Flow('c.0'), lf.Flow('c.1'), lf.Flow('c.2'))
        flow.add(c)

        d = test_utils.ProvidesRequiresTask('d', provides=[], requires=[])
        flow.add(d)

        flow.link(b, d)
        flow.link(a, d)
        flow.link(c, d)

        compilation = compiler.PatternCompiler(flow).compile()
        g = compilation.execution_graph
        self.assertTrue(g.has_edge(b_0, b_3))
        self.assertTrue(g.has_edge(b_3, d))
        self.assertEqual(4, len(g))

    def test_empty_flow_in_nested_flow(self):
        flow = lf.Flow('lf')
        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=[])

        flow2 = lf.Flow("lf-2")
        c = test_utils.ProvidesRequiresTask('c', provides=[], requires=[])
        d = test_utils.ProvidesRequiresTask('d', provides=[], requires=[])
        empty_flow = gf.Flow("empty")
        flow2.add(c, empty_flow, d)
        flow.add(a, flow2, b)

        compilation = compiler.PatternCompiler(flow).compile()
        g = compilation.execution_graph

        self.assertTrue(g.has_edge(a, c))
        self.assertTrue(g.has_edge(c, d))
        self.assertTrue(g.has_edge(d, b))

    def test_empty_flow_in_graph_flow(self):
        flow = lf.Flow('lf')
        a = test_utils.ProvidesRequiresTask('a', provides=['a'], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=['a'])
        empty_flow = lf.Flow("empty")
        flow.add(a, empty_flow, b)

        compilation = compiler.PatternCompiler(flow).compile()
        g = compilation.execution_graph
        self.assertTrue(g.has_edge(a, b))

    def test_empty_flow_in_graph_flow_empty_linkage(self):
        flow = gf.Flow('lf')
        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=[])
        empty_flow = lf.Flow("empty")
        flow.add(a, empty_flow, b)
        flow.link(empty_flow, b)

        compilation = compiler.PatternCompiler(flow).compile()
        g = compilation.execution_graph
        self.assertEqual(0, len(g.edges()))

    def test_empty_flow_in_graph_flow_linkage(self):
        flow = gf.Flow('lf')
        a = test_utils.ProvidesRequiresTask('a', provides=[], requires=[])
        b = test_utils.ProvidesRequiresTask('b', provides=[], requires=[])
        empty_flow = lf.Flow("empty")
        flow.add(a, empty_flow, b)
        flow.link(a, b)

        compilation = compiler.PatternCompiler(flow).compile()
        g = compilation.execution_graph
        self.assertEqual(1, len(g.edges()))
        self.assertTrue(g.has_edge(a, b))

    def test_checks_for_dups(self):
        flo = gf.Flow("test").add(
            test_utils.DummyTask(name="a"),
            test_utils.DummyTask(name="a")
        )
        self.assertRaisesRegexp(exc.Duplicate,
                                '^Atoms with duplicate names',
                                compiler.PatternCompiler(flo).compile)

    def test_checks_for_dups_globally(self):
        flo = gf.Flow("test").add(
            gf.Flow("int1").add(test_utils.DummyTask(name="a")),
            gf.Flow("int2").add(test_utils.DummyTask(name="a")))
        self.assertRaisesRegexp(exc.Duplicate,
                                '^Atoms with duplicate names',
                                compiler.PatternCompiler(flo).compile)

    def test_retry_in_linear_flow(self):
        flo = lf.Flow("test", retry.AlwaysRevert("c"))
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(1, len(g))
        self.assertEqual(0, g.number_of_edges())

    def test_retry_in_unordered_flow(self):
        flo = uf.Flow("test", retry.AlwaysRevert("c"))
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(1, len(g))
        self.assertEqual(0, g.number_of_edges())

    def test_retry_in_graph_flow(self):
        flo = gf.Flow("test", retry.AlwaysRevert("c"))
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(1, len(g))
        self.assertEqual(0, g.number_of_edges())

    def test_retry_in_nested_flows(self):
        c1 = retry.AlwaysRevert("c1")
        c2 = retry.AlwaysRevert("c2")
        flo = lf.Flow("test", c1).add(lf.Flow("test2", c2))
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph

        self.assertEqual(2, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (c1, c2, {'retry': True})
        ])
        self.assertIs(c1, g.node[c2]['retry'])
        self.assertItemsEqual([c1], g.no_predecessors_iter())
        self.assertItemsEqual([c2], g.no_successors_iter())

    def test_retry_in_linear_flow_with_tasks(self):
        c = retry.AlwaysRevert("c")
        a, b = test_utils.make_many(2)
        flo = lf.Flow("test", c).add(a, b)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph

        self.assertEqual(3, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (a, b, {'invariant': True}),
            (c, a, {'retry': True})
        ])

        self.assertItemsEqual([c], g.no_predecessors_iter())
        self.assertItemsEqual([b], g.no_successors_iter())
        self.assertIs(c, g.node[a]['retry'])
        self.assertIs(c, g.node[b]['retry'])

    def test_retry_in_unordered_flow_with_tasks(self):
        c = retry.AlwaysRevert("c")
        a, b = test_utils.make_many(2)
        flo = uf.Flow("test", c).add(a, b)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph

        self.assertEqual(3, len(g))
        self.assertItemsEqual(g.edges(data=True), [
            (c, a, {'retry': True}),
            (c, b, {'retry': True})
        ])

        self.assertItemsEqual([c], g.no_predecessors_iter())
        self.assertItemsEqual([a, b], g.no_successors_iter())
        self.assertIs(c, g.node[a]['retry'])
        self.assertIs(c, g.node[b]['retry'])

    def test_retry_in_graph_flow_with_tasks(self):
        r = retry.AlwaysRevert("cp")
        a, b, c = test_utils.make_many(3)
        flo = gf.Flow("test", r).add(a, b, c).link(b, c)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph
        self.assertEqual(4, len(g))

        self.assertItemsEqual(g.edges(data=True), [
            (r, a, {'retry': True}),
            (r, b, {'retry': True}),
            (b, c, {'manual': True})
        ])

        self.assertItemsEqual([r], g.no_predecessors_iter())
        self.assertItemsEqual([a, c], g.no_successors_iter())
        self.assertIs(r, g.node[a]['retry'])
        self.assertIs(r, g.node[b]['retry'])
        self.assertIs(r, g.node[c]['retry'])

    def test_retries_hierarchy(self):
        c1 = retry.AlwaysRevert("cp1")
        c2 = retry.AlwaysRevert("cp2")
        a, b, c, d = test_utils.make_many(4)
        flo = lf.Flow("test", c1).add(
            a,
            lf.Flow("test", c2).add(b, c),
            d)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph

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

    def test_retry_subflows_hierarchy(self):
        c1 = retry.AlwaysRevert("cp1")
        a, b, c, d = test_utils.make_many(4)
        flo = lf.Flow("test", c1).add(
            a,
            lf.Flow("test").add(b, c),
            d)
        compilation = compiler.PatternCompiler(flo).compile()
        g = compilation.execution_graph

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
